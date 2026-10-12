#ifndef _HANDLER_SSE_H
#define _HANDLER_SSE_H

#include "debug.h"

#include "core/net.h"
#include "core/ethernet.h"
#include "core/ip.h"
#include "core/tcp.h"
#include "http/http_server.h"
#include "http/http_server_misc.h"

#include "handler.h"

#define SSE_MAX_CHANNELS 8
#define SSE_TIMEOUT_S 60
#define SSE_KEEPALIVE_S 15
/* Send timeout for a subscriber's connection. Events are written to every
 * subscriber with the SSE locks held, so a subscriber that stops reading holds
 * up all others (and the box connection sending the event) until a write to it
 * gets nothing out for this long. Must stay well below SSE_TIMEOUT_S, or that
 * one blocked write makes every other subscriber look dead. */
#define SSE_SEND_TIMEOUT_MS 2000

typedef struct
{
    bool active;
    error_t error;
    HttpConnection *connection;
    time_t lastConnection;
    uint8_t channel;
} SseSubscriptionContext;

error_t handleApiSse(HttpConnection *connection, const char_t *uri, const char_t *queryString, client_ctx_t *ctx);
void sse_init();
error_t sse_sendEvent(const char *eventname, const char *content, bool escapeData);
error_t sse_startEventRaw(const char *eventname);
error_t sse_rawData(const char *content);
error_t sse_endEventRaw(void);
error_t sse_keepAlive(void);

#endif