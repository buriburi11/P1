import java.io.ByteArrayOutputStream;
import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

//~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
// Gateway meters plain-text chat requests using three heterogeneous protocol
// services before forwarding approved queries to the supplied fLLM.
//
// Implement the methods marked TODO. The remaining code supplies startup,
// configuration, small defensive helpers, data types, and the fLLM adapter so
// that your work stays focused on network protocols and concurrent metering.
//
public class Gateway {
    private static final int DEFAULT_PORT = 8080;
    private static final int DEFAULT_LIMIT = 3;
    private static final int REGISTERED_LIMIT = 10;
    private static final int MAX_HEADER_BYTES = 16 * 1024;
    private static final int MAX_QUERY_BYTES = 64 * 1024;
    private static final long SECONDS_PER_WINDOW = 60;

    private static final String BACKEND_HOST = "127.0.0.1";
    private static final int WHOIS_PORT = environmentPort("WHOIS_PORT", 21_043);
    private static final int REDIS_PORT = environmentPort("REDIS_PORT", 21_379);
    private static final int TIME_PORT = environmentPort("TIME_PORT", 21_037);

    //`````````````````````````````````````````````````````````````````````
    // PROVIDED: Parses configuration, opens the listener, and dispatches each
    // connection to a virtual thread. The separate backend executor allows one
    // request handler to overlap independent network operations.
    //
    public static void main(String[] args) throws Exception {
        GatewayConfig config = GatewayConfig.parse(args);
        if (config.help()) {
            printUsage();
            return;
        }
        var server = new ServerSocket(config.port());
        var fllm = new FakeLLMClient(
                config.server(), config.model(), Duration.ofSeconds(config.timeoutSeconds()));
        var clients = Executors.newVirtualThreadPerTaskExecutor();
        var backends = Executors.newVirtualThreadPerTaskExecutor();

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            try {
                server.close();
            } catch (IOException ignored) {
            }
            clients.close();
            backends.close();
        }));

        System.out.printf("GATEWAY_READY http://127.0.0.1:%d/chat model=%s%n",
                config.port(), config.model());
        while (true) {
            Socket client = server.accept();
            clients.submit(() -> serve(client, backends, fllm));
        }
    }

    //`````````````````````````````````````````````````````````````````````
    // TODO: Handle one client connection from request framing through response.
    //
    // Accept only POST /chat with a valid X-User-ID, text/plain content, and a
    // nonblank UTF-8 query. Use 404 for another path and 405 with Allow: POST for
    // another method. Obtain the Time window before building the key
    // usage:<window>:<UserID>. WHOIS and Redis are independent and must run
    // concurrently; both results are required before comparing INCR's atomic
    // result with the caller's tier limit. Cancel both tasks if the join fails
    // and preserve the interrupt status when interrupted. Return 429 above the
    // limit; call fLLM and return 200 only at or below it. The supplied catches
    // and finally block standardize errors and close every client socket.
    //
    private static void serve(Socket client, ExecutorService backends, FakeLLMClient fllm) {
        try {
            client.setSoTimeout(5_000);
            IncomingRequest request = readRequest(client.getInputStream());

            // Use request, backends, and fllm to implement the behavior above.
            throw new UnsupportedOperationException("TODO: implement serve");
        } catch (RequestException exception) {
            writeSafely(client, 400, "Bad Request", exception.getMessage() + "\n");
        } catch (Exception exception) {
            System.err.println("Gateway request failed: " + exception.getMessage());
            writeSafely(client, 502, "Bad Gateway", "A backend service failed.\n");
        } finally {
            try {
                client.close();
            } catch (IOException ignored) {
            }
        }
    }

    //`````````````````````````````````````````````````````````````````````
    // TODO: Parse one request from the supported HTTP/1.1 subset.
    //
    // The request line has method, path, and an exact HTTP/1.1 version. Header
    // names are case-insensitive and duplicate names are invalid. Content-Length
    // is required, bounded by MAX_QUERY_BYTES, and determines exactly how many
    // body bytes to read. Return immutable headers in an IncomingRequest.
    //
    private static IncomingRequest readRequest(InputStream input) throws IOException {
        throw new UnsupportedOperationException("TODO: implement readRequest");
    }

    //`````````````````````````````````````````````````````````````````````
    // TODO: Read one ASCII HTTP line ending in CRLF.
    //
    // A bare LF, EOF inside a line, or cumulative request-line/header usage over
    // MAX_HEADER_BYTES is invalid. Include both CRLF bytes in the shared consumed
    // count, but do not include them in the returned string.
    //
    private static String readLine(InputStream input, int[] consumed) throws IOException {
        throw new UnsupportedOperationException("TODO: implement readLine");
    }

    //`````````````````````````````````````````````````````````````````````
    // TODO: Obtain the current fixed window from the RFC 868 Time service.
    //
    // The TCP response is exactly four bytes containing an unsigned, big-endian
    // count of seconds since 1900. Reject an incomplete response and divide the
    // decoded value by SECONDS_PER_WINDOW. Bound socket inactivity to 3 seconds.
    //
    private static long currentWindow() throws IOException {
        throw new UnsupportedOperationException("TODO: implement currentWindow");
    }

    //`````````````````````````````````````````````````````````````````````
    // TODO: Query WHOIS for one caller's account tier.
    //
    // Send the ASCII user ID followed by CRLF, then read the response until EOF.
    // "No such domain:" maps to DEFAULT and is not an error. Otherwise find the
    // case-insensitive Tier field and accept only DEFAULT or REGISTERED. Enforce
    // the 8,192-byte response limit, use a 3-second socket timeout, and reject
    // malformed records.
    //
    private static Tier lookupTier(String userId) throws IOException {
        throw new UnsupportedOperationException("TODO: implement lookupTier");
    }

    //`````````````````````````````````````````````````````````````````````
    // TODO: Atomically reserve and return this request's attempt number.
    //
    // Encode one RESP2 array containing INCR and the supplied ASCII key. RESP
    // bulk strings use byte lengths and every protocol line ends in CRLF. A
    // successful reply begins with ':' and contains a signed decimal integer;
    // treat every other reply as a backend failure. Use a 3-second socket timeout
    // and do not use GET plus SET.
    //
    private static long increment(String key) throws IOException {
        throw new UnsupportedOperationException("TODO: implement increment");
    }

    //`````````````````````````````````````````````````````````````````````
    // PROVIDED: Reads one bounded CRLF-terminated RESP line. INCR needs only the
    // integer response type, so the gateway does not need a general RESP parser.
    //
    private static String readBackendLine(InputStream input) throws IOException {
        var line = new ByteArrayOutputStream();
        while (line.size() <= 1_024) {
            int next = input.read();
            if (next == -1) {
                throw new EOFException("Backend response ended inside a line");
            }
            if (next == '\r') {
                if (input.read() != '\n') {
                    throw new IOException("Backend line does not end with CRLF");
                }
                return line.toString(StandardCharsets.US_ASCII);
            }
            line.write(next);
        }
        throw new IOException("Backend response line is too long");
    }

    //`````````````````````````````````````````````````````````````````````
    // PROVIDED: Reports an error when possible without allowing a disconnected
    // client to escape the request handler or obscure the original failure.
    //
    private static void writeSafely(Socket client, int status, String reason, String body) {
        try {
            writeResponse(client.getOutputStream(), status, reason, body);
        } catch (IOException ignored) {
        }
    }

    //`````````````````````````````````````````````````````````````````````
    // PROVIDED: Supplies the common response form when no additional HTTP
    // headers are needed.
    //
    private static void writeResponse(OutputStream output, int status, String reason, String body)
            throws IOException {
        writeResponse(output, status, reason, body, Map.of());
    }

    //`````````````````````````````````````````````````````````````````````
    // TODO: Frame one UTF-8 plain-text HTTP/1.1 response.
    //
    // Include the status line, Content-Type, byte-oriented Content-Length,
    // Connection: close, any extra headers, and the blank line before the body.
    // Flush the complete header and body to the supplied stream.
    //
    private static void writeResponse(OutputStream output, int status, String reason, String body,
                                       Map<String, String> extraHeaders) throws IOException {
        throw new UnsupportedOperationException("TODO: implement writeResponse");
    }

    //`````````````````````````````````````````````````````````````````````
    // PROVIDED: Documents the source-file execution options and local defaults.
    //
    private static void printUsage() {
        System.out.println("Usage: java Gateway.java [options]");
        System.out.println("  --port PORT       Gateway port (default: 8080)");
        System.out.println("  --server URL      OpenAI-compatible endpoint");
        System.out.println("                    (default: http://127.0.0.1:21904/v1/chat/completions)");
        System.out.println("  --model MODEL     Model name (default: local-eliza)");
        System.out.println("  --timeout SECONDS fLLM request timeout (default: 2)");
        System.out.println("  --help            Show this help text");
    }

    //`````````````````````````````````````````````````````````````````````
    // PROVIDED: Reads a backend port override while preserving its local default.
    //
    private static int environmentPort(String name, int fallback) {
        int port = Integer.parseInt(System.getenv().getOrDefault(name, Integer.toString(fallback)));
        if (port < 1 || port > 65_535) {
            throw new IllegalArgumentException(name + " must be between 1 and 65535");
        }
        return port;
    }

    private enum Tier { DEFAULT, REGISTERED }

    // PROVIDED: Command-line configuration for the gateway and fLLM.
    //
    private record GatewayConfig(
            int port, URI server, String model, int timeoutSeconds, boolean help) {
        private GatewayConfig {
            if (port < 1 || port > 65_535) {
                throw new IllegalArgumentException("port must be between 1 and 65535");
            }
            if (server.getScheme() == null
                    || !(server.getScheme().equals("http") || server.getScheme().equals("https"))) {
                throw new IllegalArgumentException("server must be an HTTP or HTTPS URL");
            }
            if (model.isBlank()) {
                throw new IllegalArgumentException("model cannot be blank");
            }
            if (timeoutSeconds < 1) {
                throw new IllegalArgumentException("timeout must be at least 1 second");
            }
        }

        // Parses named options for local ELIZA, another fLLM, or the grading fixture.
        //
        private static GatewayConfig parse(String[] args) {
            int port = DEFAULT_PORT;
            URI server = URI.create("http://127.0.0.1:21904/v1/chat/completions");
            String model = "local-eliza";
            int timeoutSeconds = 2;
            boolean help = false;
            for (int index = 0; index < args.length; index++) {
                switch (args[index]) {
                    case "--port" -> port = integer(value(args, ++index, "--port"), "port");
                    case "--server" -> server = URI.create(value(args, ++index, "--server"));
                    case "--model" -> model = value(args, ++index, "--model");
                    case "--timeout" -> timeoutSeconds = integer(
                            value(args, ++index, "--timeout"), "timeout");
                    case "--help", "-h" -> help = true;
                    default -> throw new IllegalArgumentException("unknown option: " + args[index]);
                }
            }
            return new GatewayConfig(port, server, model, timeoutSeconds, help);
        }

        private static String value(String[] args, int index, String option) {
            if (index >= args.length) {
                throw new IllegalArgumentException("missing value for " + option);
            }
            return args[index];
        }

        private static int integer(String value, String label) {
            try {
                return Integer.parseInt(value);
            } catch (NumberFormatException exception) {
                throw new IllegalArgumentException(label + " must be an integer");
            }
        }
    }

    private record IncomingRequest(
            String method, String path, Map<String, String> headers, byte[] body) {
    }

    private static class RequestException extends IOException {
        private RequestException(String message) {
            super(message);
        }
    }
}

//~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
// PROVIDED - DO NOT MODIFY
//
// FakeLLMClient is supplied infrastructure inside the single source file. It
// lets the gateway act as an HTTP client to an OpenAI-compatible fLLM without
// making JSON request construction and response parsing student work.
//
final class FakeLLMClient {
    private final HttpClient client = HttpClient.newBuilder()
            .connectTimeout(Duration.ofSeconds(3))
            .build();
    private final URI endpoint;
    private final String model;
    private final Duration requestTimeout;

    FakeLLMClient(URI endpoint, String model, Duration requestTimeout) {
        this.endpoint = endpoint;
        this.model = model;
        this.requestTimeout = requestTimeout;
    }

    //`````````````````````````````````````````````````````````````````````
    // Sends one non-streaming completion request and returns only the assistant
    // text needed by the gateway's plain-text response contract.
    //
    String query(String query) throws IOException, InterruptedException {
        String body = "{\"model\":\"" + escapeJson(model)
                + "\",\"stream\":false,\"messages\":[{\"role\":\"user\","
                + "\"content\":\"" + escapeJson(query) + "\"}]}";
        HttpRequest request = HttpRequest.newBuilder(endpoint)
                .timeout(requestTimeout)
                .header("Content-Type", "application/json")
                .POST(HttpRequest.BodyPublishers.ofString(body, StandardCharsets.UTF_8))
                .build();
        HttpResponse<String> response = client.send(
                request, HttpResponse.BodyHandlers.ofString(StandardCharsets.UTF_8));
        if (response.statusCode() != 200) {
            throw new IOException("fLLM returned HTTP " + response.statusCode());
        }
        return readJsonStringField(response.body(), "content");
    }

    //`````````````````````````````````````````````````````````````````````
    // Escapes a Java string for use as a JSON string value without introducing
    // a JSON-library dependency into the student source file.
    //
    private static String escapeJson(String value) {
        var escaped = new StringBuilder();
        for (int index = 0; index < value.length(); index++) {
            char character = value.charAt(index);
            switch (character) {
                case '"' -> escaped.append("\\\"");
                case '\\' -> escaped.append("\\\\");
                case '\b' -> escaped.append("\\b");
                case '\f' -> escaped.append("\\f");
                case '\n' -> escaped.append("\\n");
                case '\r' -> escaped.append("\\r");
                case '\t' -> escaped.append("\\t");
                default -> {
                    if (character < 0x20) {
                        escaped.append("\\u%04x".formatted((int) character));
                    } else {
                        escaped.append(character);
                    }
                }
            }
        }
        return escaped.toString();
    }

    //`````````````````````````````````````````````````````````````````````
    // Extracts and unescapes the known string field from the constrained
    // OpenAI-compatible response. This is intentionally supplied code rather
    // than a general JSON parser students must implement.
    //
    private static String readJsonStringField(String json, String field) throws IOException {
        int name = json.indexOf("\"" + field + "\"");
        if (name < 0) {
            throw new IOException("fLLM response has no " + field + " field");
        }
        int colon = json.indexOf(':', name + field.length() + 2);
        int quote = colon + 1;
        while (quote < json.length() && Character.isWhitespace(json.charAt(quote))) {
            quote++;
        }
        if (colon < 0 || quote >= json.length() || json.charAt(quote) != '"') {
            throw new IOException("fLLM " + field + " field is not a string");
        }

        var value = new StringBuilder();
        for (int index = quote + 1; index < json.length(); index++) {
            char character = json.charAt(index);
            if (character == '"') {
                return value.toString();
            }
            if (character != '\\') {
                value.append(character);
                continue;
            }
            if (++index >= json.length()) {
                break;
            }
            char escaped = json.charAt(index);
            switch (escaped) {
                case '"', '\\', '/' -> value.append(escaped);
                case 'b' -> value.append('\b');
                case 'f' -> value.append('\f');
                case 'n' -> value.append('\n');
                case 'r' -> value.append('\r');
                case 't' -> value.append('\t');
                case 'u' -> {
                    if (index + 4 >= json.length()) {
                        throw new IOException("invalid Unicode escape in fLLM response");
                    }
                    try {
                        value.append((char) Integer.parseInt(
                                json.substring(index + 1, index + 5), 16));
                    } catch (NumberFormatException exception) {
                        throw new IOException("invalid Unicode escape in fLLM response", exception);
                    }
                    index += 4;
                }
                default -> throw new IOException("invalid escape in fLLM response");
            }
        }
        throw new IOException("unterminated string in fLLM response");
    }
}
