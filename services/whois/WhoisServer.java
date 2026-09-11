import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.stream.Stream;
import java.util.regex.Pattern;

//~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
// WhoisServer implements a simplified WHOIS client query lookup over TCP.
// The client sends a domain-like query followed by CRLF. The server searches
// registry/ for a file matching the query and returns its contents.
//
public class WhoisServer {
    private static final int DEFAULT_PORT = 4300;
    private static final Path REGISTRY = Paths.get("registry");
    private static final Pattern SAFE_CHARS = Pattern.compile("^[a-zA-Z0-9._-]+$");

    //`````````````````````````````````````````````````````````````````````
    // Opens the TCP listener and handles each connection by reading a query
    // and returning the corresponding registry file contents.
    //
    public static void main(String[] args) throws Exception {
        int port = parsePort(args);
        var server = new ServerSocket(port);

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            try {
                server.close();
            } catch (IOException ignored) {
            }
        }));

        System.out.printf("WHOIS_READY tcp=%d%n", port);
        try (var clients = java.util.concurrent.Executors.newVirtualThreadPerTaskExecutor()) {
            while (true) {
                Socket client = server.accept();
                clients.submit(() -> serve(client));
            }
        }
    }

    //`````````````````````````````````````````````````````````````````````
    // Reads one query, looks up the registry file, and returns its contents
    // or a not-found message. The connection is always closed after response.
    //
    private static void serve(Socket client) {
        try (client) {
            String query = readLine(client.getInputStream());
            if (query == null) {
                return;
            }
            if (!SAFE_CHARS.matcher(query).matches()) {
                reply(client.getOutputStream(), "Invalid query format\r\n");
                return;
            }
            Path result = findFile(query);
            if (result != null) {
                Files.copy(result, client.getOutputStream());
            } else {
                reply(client.getOutputStream(), "No such domain: " + query + "\r\n");
            }
        } catch (IOException exception) {
            System.err.println("Whois client failed: " + exception.getMessage());
        }
    }

    //`````````````````````````````````````````````````````````````````````
    // Searches registry/ for a file whose base name matches the query.
    // Returns the first match or null if not found.
    //
    private static Path findFile(String query) throws IOException {
        if (!Files.exists(REGISTRY)) {
            return null;
        }
        try (Stream<Path> files = Files.list(REGISTRY)) {
            return files.filter(p -> {
                String name = p.getFileName().toString();
                int dot = name.lastIndexOf('.');
                String base = dot > 0 ? name.substring(0, dot) : name;
                return base.equals(query);
            }).findFirst().orElse(null);
        }
    }

    //`````````````````````````````````````````````````````````````````````
    // Reads one CRLF-terminated ASCII line.
    //
    private static String readLine(InputStream input) throws IOException {
        var line = new java.io.ByteArrayOutputStream();
        while (true) {
            int next = input.read();
            if (next == -1) {
                if (line.size() == 0) {
                    return null;
                }
                throw new IOException("connection ended inside a query");
            }
            if (next == '\r') {
                if (input.read() != '\n') {
                    throw new IOException("queries must end with CRLF");
                }
                return line.toString(StandardCharsets.US_ASCII);
            }
            if (next == '\n') {
                throw new IOException("queries must end with CRLF");
            }
            line.write(next);
        }
    }

    private static void reply(OutputStream output, String message) throws IOException {
        output.write(message.getBytes(StandardCharsets.US_ASCII));
    }

    private static int parsePort(String[] args) {
        if (args.length > 1) {
            throw new IllegalArgumentException("Usage: java WhoisServer.java [port]");
        }
        int port = args.length == 0 ? DEFAULT_PORT : Integer.parseInt(args[0]);
        if (port < 1 || port > 65_535) {
            throw new IllegalArgumentException("Port must be between 1 and 65535");
        }
        return port;
    }
}
