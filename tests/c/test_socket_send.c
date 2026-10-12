#include <arpa/inet.h>
#include <assert.h>
#include <netinet/in.h>
#include <pthread.h>
#include <stdlib.h>
#include <string.h>
#include <sys/socket.h>
#include <unistd.h>

#include "core/net.h"
#include "core/socket.h"
#include "tests.h"

/* Short enough that every blocking send() in these tests runs into it. */
#define SEND_TIMEOUT_MS 200

typedef struct
{
    int fd;
    uint8_t *buffer;
    size_t size;
    size_t received;
} slow_reader_t;

/* reads 1 KB every 30 ms: much slower than the sender, but never stops */
static void *read_slowly(void *arg)
{
    slow_reader_t *reader = arg;
    while (reader->received < reader->size)
    {
        ssize_t n = read(reader->fd, reader->buffer + reader->received, 1024);
        if (n <= 0)
        {
            break;
        }
        reader->received += n;
        usleep(30 * 1000);
    }
    return NULL;
}

/* A TCP connection over loopback with small buffers: fds[0] sends, fds[1] receives.
 * TCP, not a socketpair, because only TCP lets one send() end early with part of
 * the data when the send timeout expires, the case these tests are about. */
static Socket *sending_socket(int fds[2])
{
    int small = 4096;
    struct sockaddr_in addr = {.sin_family = AF_INET};
    socklen_t len = sizeof(addr);
    assert(inet_pton(AF_INET, "127.0.0.1", &addr.sin_addr) == 1);

    int listener = socket(AF_INET, SOCK_STREAM, 0);
    assert(listener >= 0);
    assert(bind(listener, (struct sockaddr *)&addr, sizeof(addr)) == 0);
    assert(listen(listener, 1) == 0);
    assert(getsockname(listener, (struct sockaddr *)&addr, &len) == 0);

    fds[1] = socket(AF_INET, SOCK_STREAM, 0);
    assert(fds[1] >= 0);
    assert(setsockopt(fds[1], SOL_SOCKET, SO_RCVBUF, &small, sizeof(small)) == 0);
    assert(connect(fds[1], (struct sockaddr *)&addr, sizeof(addr)) == 0);
    fds[0] = accept(listener, NULL, NULL);
    assert(fds[0] >= 0);
    close(listener);
    assert(setsockopt(fds[0], SOL_SOCKET, SO_SNDBUF, &small, sizeof(small)) == 0);

    Socket *socket = calloc(1, sizeof(Socket));
    assert(socket != NULL);
    socket->descriptor = fds[0];
    socketSetTimeout(socket, SEND_TIMEOUT_MS);
    return socket;
}

static uint8_t *pattern(size_t size)
{
    uint8_t *data = malloc(size);
    assert(data != NULL);
    for (size_t i = 0; i < size; i++)
    {
        data[i] = (uint8_t)(i % 251);
    }
    return data;
}

/*
 * A peer that keeps taking data, only slowly, gets all of it: a send() that
 * hits the send timeout after part of the data went out is progress, not the end.
 */
void test_socket_send_slow_peer_gets_everything(void)
{
    int fds[2];
    Socket *socket = sending_socket(fds);
    size_t size = 64 * 1024;
    uint8_t *data = pattern(size);
    slow_reader_t reader = {fds[1], calloc(1, size), size, 0};
    assert(reader.buffer != NULL);
    pthread_t thread;
    assert(pthread_create(&thread, NULL, read_slowly, &reader) == 0);

    size_t written = 0;
    error_t error = socketSend(socket, data, size, &written, 0);
    shutdown(fds[0], SHUT_WR); /* the reader stops at the end of what was sent */

    assert(pthread_join(thread, NULL) == 0);
    assert(error == NO_ERROR);
    assert(written == size);
    assert(reader.received == size);
    assert(memcmp(reader.buffer, data, size) == 0);

    close(fds[1]);
    socketClose(socket);
    free(reader.buffer);
    free(data);
}

/* A peer that takes nothing anymore ends the send with ERROR_TIMEOUT, telling how much went out. */
void test_socket_send_stalled_peer_times_out(void)
{
    int fds[2];
    Socket *socket = sending_socket(fds);
    size_t size = 1024 * 1024;
    uint8_t *data = pattern(size);

    size_t written = 0;
    error_t error = socketSend(socket, data, size, &written, 0);

    assert(error == ERROR_TIMEOUT);
    assert(written > 0);
    assert(written < size);

    close(fds[1]);
    socketClose(socket);
    free(data);
}
