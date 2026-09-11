# P1: Model Access Gateway

## 1. Project Goal

Build a concurrent HTTP gateway that controls access to a model service.

For each client request, the gateway must:

1. Parse one HTTP/1.1 request.
2. Ask an RFC 868 Time service for the current rate-limit window.
3. Ask a WHOIS service for the caller's account tier.
4. Atomically increment the caller's usage count in Redis.
5. Use the tier and increment result to allow or deny the request.
6. Forward an allowed query to the supplied fLLM client.
7. Return one correctly framed HTTP response.

The gateway is both a server and a client. It is a server to the program sending
the chat request. While processing that request, it becomes a client of Time,
WHOIS, Redis, and the fLLM.

The project uses local services, small limits, and short time windows so that
the behavior is easy to observe. The architecture models a real API gateway
protecting an expensive shared service.

### Services in the project

| Service | Real-world purpose | Purpose in P1 |
|---|---|---|
| Gateway | An API gateway accepts client traffic, enforces policies, and routes approved requests to backend services. | Your program receives chat requests, applies the usage limit, and forwards approved queries to the fLLM. |
| Time | A network time service gives systems a shared source of time. RFC 868 is an early, simple example; modern systems usually use NTP. | The Time service supplies the timestamp used to place every request into a fixed 60-second rate-limit window. |
| WHOIS | WHOIS services publish registration records for Internet resources such as domains and IP address blocks. | The simplified WHOIS registry identifies a caller as `DEFAULT` or `REGISTERED`, which determines the caller's limit. |
| Redis | Redis is a fast in-memory data store often used for caching, counters, coordination, and rate limiting. | The Redis-compatible service atomically counts each user's attempts within a window using `INCR`. |
| fLLM | A language-model service receives prompts and generates responses, often behind a metered API. | The supplied fake LLM provides a local chat backend so approved requests can be tested without an external model or API account. |

## 2. Starting Point

The starter file is `Gateway.java`. It is a Java 21 single-source program and
contains seven methods marked `TODO`:

- `serve()`
- `readRequest()`
- `readLine()`
- `currentWindow()`
- `lookupTier()`
- `increment()`
- The five-argument `writeResponse()` overload

The rest of the file is provided. In particular:

- `main()` creates the listening socket and virtual-thread executors.
- `readBackendLine()` reads a bounded CRLF-terminated Redis response line.
- `writeSafely()` handles a client that disconnects during an error response.
- `GatewayConfig` handles command-line configuration.
- `FakeLLMClient` handles the fLLM's HTTP and JSON protocol.

Do not add a JSON dependency or replace the supplied fLLM client. The networking
and metering code belongs in the existing TODO methods.

## 3. Running the Local Services

This project requires Java 21 or newer.

Start the four supplied services in one terminal:

```bash
./start.sh
```

The launcher uses these non-standard local ports:

| Service | Address | Protocol |
|---|---|---|
| Time | `127.0.0.1:21037` | RFC 868 over TCP |
| WHOIS | `127.0.0.1:21043` | CRLF-terminated text over TCP |
| Redis | `127.0.0.1:21379` | RESP2 over TCP |
| fLLM | `http://127.0.0.1:21904/v1/chat/completions` | HTTP and JSON |

Leave the service terminal running. Run the gateway in a second terminal:

```bash
java Gateway.java
```

The default gateway endpoint is:

```text
http://127.0.0.1:8080/chat
```

The gateway's `--help` option lists its configurable port, fLLM endpoint, model,
and timeout. The default fLLM request timeout is two seconds.

## 4. Complete Request Flow

The required dependency order is:

```text
read and validate HTTP request
            |
            v
       query Time
            |
            v
build usage:<window>:<UserID>
            |
            +-------------------+
            |                   |
            v                   v
      query WHOIS         Redis INCR
            |                   |
            +---------+---------+
                      |
                      v
              compare attempt
                with tier limit
                 /           \
                /             \
          allowed             denied
             |                   |
             v                   v
         query fLLM          return 429
             |
             v
         return 200
```

Time must finish before the Redis request because the Time result is part of the
Redis key. WHOIS and Redis do not depend on each other and must run concurrently.
The fLLM is downstream of the decision and must not be called for a denied
request.

Redis is the only mutable application state. Do not store usage counters or chat
history in the gateway.

## 5. Inbound HTTP/1.1 Request

The gateway supports one request form:

```http
POST /chat HTTP/1.1
X-User-ID: alice
Content-Type: text/plain; charset=utf-8
Content-Length: 18

How are you today?
```

The blank line between the headers and body is required. Every HTTP line ends
with the two bytes CRLF: carriage return (`\r`) followed by line feed (`\n`).

### Request line

The request line contains exactly three parts separated by spaces:

```text
<method> <path> <version>
```

The supported values are:

- Method: `POST`
- Path: `/chat`
- Version: `HTTP/1.1`

The parser must reject a malformed request line or another HTTP version with a
`RequestException`. After parsing succeeds, `serve()` returns `404` for another
path and `405` for another method. A `405` response must include `Allow: POST`.

### Headers

Read headers until the first empty line. Each nonempty header line has a name,
a colon, and a value:

```text
Header-Name: value
```

Header names are case-insensitive. Convert names to lowercase before storing
them. Strip surrounding whitespace from names and values. Reject a line with no
header name or colon, and reject duplicate names after case normalization.

The cumulative request-line and header limit is `MAX_HEADER_BYTES`, including
CRLF bytes. `readLine()` receives a shared `consumed` counter so that the limit
applies to the complete header section rather than separately to each line.

The required headers are:

| Header | Rule |
|---|---|
| `X-User-ID` | Must match `[A-Za-z0-9._-]{1,64}` |
| `Content-Type` | Media type must be `text/plain`, case-insensitively |
| `Content-Length` | Must be a decimal integer from 0 through `MAX_QUERY_BYTES` |

`Content-Type` parameters are allowed. For example,
`text/plain; charset=utf-8` is valid because its media type is `text/plain`.

### Body

TCP is a byte stream. One call to `read()` is not guaranteed to return the
complete body. Read exactly the number of bytes declared by `Content-Length`.
If EOF arrives first, the request is incomplete.

Interpret the body as UTF-8 after all declared bytes have been read. A blank
query is invalid. Chunked request bodies are not part of this project's protocol.

The gateway processes one request per client connection and then closes the
connection. It does not implement HTTP pipelining or persistent inbound
connections.

## 6. HTTP Response

Every response has this general form:

```http
HTTP/1.1 <status> <reason>
Content-Type: text/plain; charset=utf-8
Content-Length: <body byte length>
Connection: close
<optional extra headers>

<body>
```

Use CRLF for every header line and for the empty line separating headers from
the body. The `Content-Length` value is the length of the UTF-8 byte array, not
the number of Java characters in the `String`.

The five-argument `writeResponse()` method must:

- Encode the body as UTF-8.
- Build the status line and required headers.
- Append every entry from `extraHeaders`.
- Append the empty line that ends the header section.
- Write the ASCII header bytes followed by the UTF-8 body bytes.
- Flush the output stream.

The four-argument overload is supplied for responses with no extra headers.

### Required status behavior

| Status | Condition | Body or additional requirement |
|---|---|---|
| `200 OK` | Request is allowed and fLLM succeeds | fLLM text followed by `\n` |
| `400 Bad Request` | Request is malformed or invalid | `RequestException` message followed by `\n` |
| `404 Not Found` | Path is not `/chat` | `Not found.\n` |
| `405 Method Not Allowed` | `/chat` method is not `POST` | `Use POST for /chat.\n` and `Allow: POST` |
| `429 Too Many Requests` | Attempt exceeds the tier limit | `Rate limit exceeded for <UserID>.\n` |
| `502 Bad Gateway` | A backend fails | `A backend service failed.\n` |

The reason phrase is part of the status line. The supplied error handling in
`serve()` maps `RequestException` to 400 and other request-processing failures
to 502.

## 7. RFC 868 Time Client

`currentWindow()` connects to the Time service over TCP. The client sends no
request bytes. The server immediately returns exactly four bytes and closes the
connection.

Those four bytes are an unsigned, big-endian integer containing the number of
seconds since January 1, 1900. Java's `ByteBuffer.getInt()` reads the network
byte order correctly, but the resulting Java `int` is signed. Convert it to an
unsigned `long` before performing the window calculation.

```text
window = unsignedSecondsSince1900 / SECONDS_PER_WINDOW
```

`SECONDS_PER_WINDOW` is 60, so all requests during the same fixed minute produce
the same window number.

Use a three-second socket timeout. Fewer than four response bytes is a backend
failure. Close the socket on every path; try-with-resources is appropriate.

## 8. WHOIS Tier Client

`lookupTier()` opens a TCP connection to the WHOIS service and sends:

```text
<UserID>\r\n
```

The server closes its side of the connection after sending the response, so EOF
frames the complete record. Read no more than 8,193 bytes. Receiving more than
8,192 bytes means the response is too large.

A registered record resembles:

```text
UserID: alice
Tier: REGISTERED
```

Search the response one line at a time for a `Tier:` field. Match the field name
and value case-insensitively. The accepted tier values are `DEFAULT` and
`REGISTERED`.

An unknown caller produces a response beginning with:

```text
No such domain:
```

This is a successful lookup and maps to `Tier.DEFAULT`. A present record with no
tier or with an unknown tier is malformed and must fail.

Use ASCII for the request and response, a three-second socket timeout, and
try-with-resources for the socket.

## 9. Redis RESP2 Client

The Redis key is:

```text
usage:<window>:<UserID>
```

`increment()` must send one Redis `INCR` command encoded as a RESP2 array of two
bulk strings:

```text
*2\r\n
$4\r\n
INCR\r\n
$<key-byte-length>\r\n
<key>\r\n
```

RESP uses byte counts. The usage key contains only ASCII characters, so its
ASCII byte length is the correct bulk-string length.

A successful Redis integer reply looks like:

```text
:1\r\n
```

The supplied `readBackendLine()` method removes the final CRLF and returns `:1`.
Require the `:` prefix and parse the remaining text as a Java `long`. Any Redis
error or malformed integer is a backend failure.

Use a three-second socket timeout. Write and flush the complete command before
reading the reply, and close the socket afterward.

Do not implement the limiter as `GET`, local addition, and `SET`. That sequence
is not atomic across concurrent gateway requests. `INCR` both changes the value
and returns the new attempt number as one atomic Redis operation.

## 10. Tiered Rate-Limit Decision

The limits are:

| Tier | Attempts allowed per 60-second window |
|---|---:|
| `DEFAULT` or no WHOIS record | 3 |
| `REGISTERED` | 10 |

Use the value returned by `INCR` as the current request's attempt number:

```text
allow when attempt <= tier limit
deny when attempt > tier limit
```

For a default caller, attempts 1, 2, and 3 are allowed. Attempt 4 and every later
attempt in that window is denied. For a registered caller, attempts 1 through 10
are allowed and attempt 11 is the first denial.

Every valid request that reaches Redis is counted. This includes denied
requests. Do not decrement the counter when the fLLM later fails. Since WHOIS
and Redis run concurrently, Redis might also complete its increment before a
WHOIS failure becomes known. No rollback is required.

Malformed inbound requests are rejected before Time, WHOIS, Redis, or fLLM is
called.

## 11. Concurrent WHOIS and Redis Calls

The executor named `backends` creates one virtual thread per submitted task.
Submit the WHOIS lookup and Redis increment before waiting for either result.
If you submit one task and immediately wait for it before submitting the other,
the calls are sequential rather than concurrent.

Each submitted task returns a `Future`. Obtain both results before making the
decision. If a task fails:

- Cancel both futures.
- Convert the failure into an `IOException` so `serve()` returns 502.
- If the handler catches `InterruptedException`, cancel both futures, restore
  the current thread's interrupted status, and report the interruption as an
  I/O failure.

The gateway does not synchronize a local counter. Correctness comes from using
Redis's atomic `INCR` result. Under a concurrent burst for one default user,
requests can finish in any order, but exactly three may be approved within one
window.

## 12. fLLM Boundary

For an allowed request, call:

```java
fllm.query(query)
```

`FakeLLMClient` sends one OpenAI-compatible completion request and returns the
assistant's text. It may return different valid ELIZA-style responses to the
same query. Do not make the rate-limit decision depend on response wording.

Do not call the fLLM for a denied request. The purpose of the gateway is to
protect that resource.

Each inbound request is an independent model query. The gateway does not retain
conversation history. A production chat application usually resends relevant
history in each model request; that state is intentionally outside this project.
Reusing a TCP connection would not preserve model context because conversation
state belongs to the application protocol, not TCP.

The default complete fLLM request timeout is two seconds. A different timeout
must be selected explicitly with `--timeout SECONDS`; external models do not
automatically receive more time.

## 13. Suggested Development Order

Implement and test one boundary at a time:

1. `writeResponse()`
2. `readLine()`
3. `readRequest()`
4. `currentWindow()`
5. `lookupTier()`
6. `increment()`
7. `serve()`

This order leaves the concurrent composition until each individual protocol
client is working.

Do not treat one successful request as sufficient testing. Check malformed
framing, unknown callers, both tiers, the first over-limit attempt, backend
failure, and multiple simultaneous requests for the same caller.

## 14. Manual Checks

### Registered caller

`alice` is registered and has a limit of 10:

```bash
curl -i \
  -X POST \
  -H "X-User-ID: alice" \
  -H "Content-Type: text/plain" \
  --data "How are you today?" \
  http://127.0.0.1:8080/chat
```

### Default caller

Any caller without a WHOIS record uses the default limit:

```bash
curl -i \
  -X POST \
  -H "X-User-ID: unknown-user" \
  -H "Content-Type: text/plain" \
  --data "Tell me about networks." \
  http://127.0.0.1:8080/chat
```

Run the default request four times within one window. The first three requests
should return 200 and the fourth should return 429.

When testing concurrency, use a new caller ID so earlier manual requests do not
affect the count. Send the complete burst within one minute to avoid crossing a
window boundary.

## 15. Completion Checklist

- `Gateway.java` runs with Java 21 source-file mode.
- Request and response lines use CRLF.
- Header names are handled case-insensitively.
- `Content-Length` controls the exact body read and reports UTF-8 body bytes.
- Invalid requests do not contact backend services.
- RFC 868 time is decoded as an unsigned four-byte value.
- Unknown WHOIS users map to the default tier.
- Redis receives one correctly framed `INCR` command per valid attempt.
- The returned `INCR` value, not a separate read, determines access.
- WHOIS and Redis execute concurrently.
- Default and registered limits are enforced at 3 and 10.
- Denied requests return 429 without calling the fLLM.
- Backend failures return 502.
- Every client socket and backend socket is closed.
- The gateway survives malformed requests and disconnected clients.

## 16. Further Reading

Use these sections when you need additional background. The project contract in
this document remains authoritative for the exact required behavior.

### Network applications and sockets

- Peterson and Davie, [Section 1.4.1: Socket API](https://book.systemsapproach.org/foundation/software.html#socket-api) explains sockets, passive server opens, active client opens, ports, and stream I/O.
- Peterson and Davie, [Section 1.4.2: Example Client/Server](https://book.systemsapproach.org/foundation/software.html#example-client-server) traces a complete TCP client/server exchange.
- Javanotes, [Section 11.4.3: Sockets in Java](https://math.hws.edu/javanotes/c11/s4.html#IO.4.3) covers `ServerSocket`, `Socket`, input streams, output streams, and blocking calls.

### HTTP framing

- Peterson and Davie, [Section 9.1.2: World Wide Web (HTTP)](https://book.systemsapproach.org/applications/traditional.html#world-wide-web-http) describes HTTP's start line, CRLF-terminated headers, blank line, and message body.
- Peterson and Davie, [Request Messages](https://book.systemsapproach.org/applications/traditional.html#request-messages) and [Response Messages](https://book.systemsapproach.org/applications/traditional.html#response-messages) explain methods, paths, status lines, status classes, and headers.
- Peterson and Davie, [TCP Connections](https://book.systemsapproach.org/applications/traditional.html#tcp-connections) explains persistent HTTP/1.1 connections. P1 intentionally uses the simpler one-request-per-connection model.

### Concurrency and latency

- Peterson and Davie, [Section 1.5.1: Bandwidth and Latency](https://book.systemsapproach.org/foundation/performance.html#bandwidth-and-latency) explains why independent network waits contribute to response time.
- Javanotes, [Section 12.4.1: The Blocking I/O Problem](https://math.hws.edu/javanotes/c12/s4.html#threads.4.1) explains why network servers use threads around blocking socket operations.
- Javanotes, [Section 12.4.3: A Threaded Network Server](https://math.hws.edu/javanotes/c12/s4.html#threads.4.3) shows the thread-per-client server pattern.
- Javanotes, [Section 12.1.5: Atomic Variables](https://math.hws.edu/javanotes/c12/s1.html#threads.1.5) explains atomic increment-and-return operations and the races they prevent.
- Oracle, [Virtual Threads](https://docs.oracle.com/en/java/javase/21/core/virtual-threads.html) covers Java 21 virtual-thread executors, blocking I/O, and fan-out with futures.

### Backend wire protocols

- [RFC 868: Time Protocol](https://www.rfc-editor.org/rfc/rfc868.html) defines the four-byte seconds-since-1900 response.
- [RFC 3912: WHOIS Protocol Specification](https://www.rfc-editor.org/rfc/rfc3912.html) defines the CRLF query and server-closed response framing on which the supplied registry is modeled.
- Redis, [Redis serialization protocol specification](https://redis.io/docs/latest/develop/reference/protocol-spec/) defines RESP arrays, bulk strings, errors, and integer replies.
- Redis, [`INCR` command](https://redis.io/docs/latest/commands/incr/) documents the atomic increment operation and its returned value.
