# P1 Model Access Gateway

This directory contains the starting environment for the P1 model access
gateway. The gateway accepts plain-text HTTP requests, meters each caller with
the provided Time, WHOIS, and Redis services, and forwards approved requests to
the provided local fLLM.

## Requirements

- Java 21 or newer
- Bash for the convenience launcher

## Contents

- `Gateway.java` - the gateway source file
- `PROMPT.md` - project requirements and implementation guide
- `services/time/` - RFC 868 Time service
- `services/whois/` - WHOIS account registry and records
- `services/redis-resp/` - Redis-compatible RESP2 service
- `services/fllm/eliza-provider.jar` - packaged local fLLM provider
- `start.sh` - starts and stops all four provided services

The fLLM provider is supplied only as a runnable JAR. Its implementation source
is not part of the distribution.

## Running Locally

Start the provided services:

```bash
./start.sh
```

Leave that terminal running. In another terminal, run the gateway:

```bash
java Gateway.java
```

The gateway listens at `http://127.0.0.1:8080/chat`. For example:

```bash
curl -i \
  -X POST \
  -H "X-User-ID: alice" \
  -H "Content-Type: text/plain" \
  --data "How are you today?" \
  http://127.0.0.1:8080/chat
```

Press `Ctrl-C` in the service terminal to stop every provided service.

## Local Configuration

The launcher recognizes `WHOIS_PORT`, `REDIS_PORT`, `TIME_PORT`, and
`FLLM_PORT`. Their non-standard defaults are 21043, 21379, 21037, and 21904
respectively. If you override them, use the same environment values when
launching `Gateway.java`.

The gateway defaults to the local fLLM, model `local-eliza`, and a two-second
fLLM request timeout. Run `java Gateway.java --help` to see its options.
