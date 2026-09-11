import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.ByteArrayOutputStream;
import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;

//~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
// RedisRespServer implements a useful subset of Redis using the RESP2 wire
// protocol. Clients send arrays of byte-counted bulk strings and share one
// binary-safe, in-memory key/value store. Data survives connections but not a
// server restart.
//
public class RedisRespServer {
    private static final int DEFAULT_PORT = 16_379;
    private static final int MAX_ARGUMENTS = 128;
    private static final int MAX_BULK_BYTES = 1_048_576;
    private static final int MAX_REQUEST_BYTES = 2_097_152;
    private static final int MAX_NUMBER_BYTES = 20;
    private static final ConcurrentHashMap<String, byte[]> STORE = new ConcurrentHashMap<>();

    //`````````````````````````````````````````````````````````````````````
    // Opens the TCP listener and gives every connection a virtual thread. The
    // static concurrent map remains shared by all client handlers.
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

        System.out.printf("REDIS_RESP_READY tcp=%d%n", port);
        try (var clients = Executors.newVirtualThreadPerTaskExecutor()) {
            while (true) {
                Socket client = server.accept();
                clients.submit(() -> serve(client));
            }
        }
    }

    //`````````````````````````````````````````````````````````````````````
    // Reads and executes commands until the client sends QUIT or closes the
    // connection. This loop also supports pipelining: several complete RESP
    // commands may already be waiting in the TCP byte stream.
    //
    private static void serve(Socket client) {
        try (client;
             var input = new BufferedInputStream(client.getInputStream());
             var output = new BufferedOutputStream(client.getOutputStream())) {
            while (true) {
                List<byte[]> command;
                try {
                    command = readCommand(input);
                } catch (ProtocolException exception) {
                    writeError(output, "Protocol error: " + exception.getMessage());
                    output.flush();
                    return;
                }
                if (command == null) {
                    return;
                }

                boolean keepConnection = execute(command, output);
                output.flush();
                if (!keepConnection) {
                    return;
                }
            }
        } catch (IOException exception) {
            System.err.println("Redis RESP client failed: " + exception.getMessage());
        }
    }

    //`````````````````````````````````````````````````````````````````````
    // Parses one RESP2 command encoded as an array of non-null bulk strings.
    // Length prefixes, rather than TCP reads, identify every record boundary.
    //
    private static List<byte[]> readCommand(InputStream input) throws IOException {
        int type = input.read();
        if (type == -1) {
            return null;
        }
        if (type != '*') {
            throw new ProtocolException("expected an array");
        }

        int count = readLength(input, "array length");
        if (count < 1 || count > MAX_ARGUMENTS) {
            throw new ProtocolException(
                    "array length must be between 1 and " + MAX_ARGUMENTS);
        }

        var command = new ArrayList<byte[]>(count);
        int requestBytes = 0;
        for (int index = 0; index < count; index++) {
            if (input.read() != '$') {
                throw new ProtocolException("command elements must be bulk strings");
            }
            int length = readLength(input, "bulk string length");
            if (length < 0 || length > MAX_BULK_BYTES) {
                throw new ProtocolException(
                        "bulk string length must be between 0 and " + MAX_BULK_BYTES);
            }
            requestBytes += length;
            if (requestBytes > MAX_REQUEST_BYTES) {
                throw new ProtocolException("request exceeds " + MAX_REQUEST_BYTES + " bytes");
            }

            byte[] value = input.readNBytes(length);
            if (value.length != length) {
                throw new EOFException("connection ended inside a bulk string");
            }
            requireCrlf(input);
            command.add(value);
        }
        return command;
    }

    //`````````````````````````````````````````````````````````````````````
    // Executes the supported Redis command subset and writes one RESP reply.
    // Command errors preserve framing, so the connection remains usable.
    //
    private static boolean execute(List<byte[]> arguments, OutputStream output)
            throws IOException {
        String command = new String(arguments.getFirst(), StandardCharsets.US_ASCII)
                .toUpperCase(Locale.ROOT);
        return switch (command) {
            case "PING" -> ping(arguments, output);
            case "ECHO" -> echo(arguments, output);
            case "SET" -> set(arguments, output);
            case "GET" -> get(arguments, output);
            case "INCR" -> increment(arguments, output);
            case "DEL" -> delete(arguments, output);
            case "EXISTS" -> exists(arguments, output);
            case "QUIT" -> quit(arguments, output);
            default -> {
                writeError(output, "unknown command");
                yield true;
            }
        };
    }

    private static boolean ping(List<byte[]> arguments, OutputStream output) throws IOException {
        if (arguments.size() == 1) {
            writeSimpleString(output, "PONG");
        } else if (arguments.size() == 2) {
            writeBulkString(output, arguments.get(1));
        } else {
            writeArityError(output, "ping");
        }
        return true;
    }

    private static boolean echo(List<byte[]> arguments, OutputStream output) throws IOException {
        if (arguments.size() != 2) {
            writeArityError(output, "echo");
        } else {
            writeBulkString(output, arguments.get(1));
        }
        return true;
    }

    private static boolean set(List<byte[]> arguments, OutputStream output) throws IOException {
        if (arguments.size() != 3) {
            writeArityError(output, "set");
        } else {
            STORE.put(key(arguments.get(1)), arguments.get(2));
            writeSimpleString(output, "OK");
        }
        return true;
    }

    private static boolean get(List<byte[]> arguments, OutputStream output) throws IOException {
        if (arguments.size() != 2) {
            writeArityError(output, "get");
        } else {
            byte[] value = STORE.get(key(arguments.get(1)));
            if (value == null) {
                writeNullBulkString(output);
            } else {
                writeBulkString(output, value);
            }
        }
        return true;
    }

    private static boolean increment(List<byte[]> arguments, OutputStream output)
            throws IOException {
        if (arguments.size() != 2) {
            writeArityError(output, "incr");
            return true;
        }

        long[] result = new long[1];
        try {
            STORE.compute(key(arguments.get(1)), (ignored, current) -> {
                long value = 0;
                if (current != null) {
                    try {
                        value = Long.parseLong(new String(current, StandardCharsets.US_ASCII));
                    } catch (NumberFormatException exception) {
                        throw new InvalidIntegerException();
                    }
                }
                if (value == Long.MAX_VALUE) {
                    throw new InvalidIntegerException();
                }
                result[0] = value + 1;
                return Long.toString(result[0]).getBytes(StandardCharsets.US_ASCII);
            });
            writeInteger(output, result[0]);
        } catch (InvalidIntegerException exception) {
            writeError(output, "value is not an integer or out of range");
        }
        return true;
    }

    private static boolean delete(List<byte[]> arguments, OutputStream output) throws IOException {
        if (arguments.size() < 2) {
            writeArityError(output, "del");
        } else {
            long deleted = 0;
            for (int index = 1; index < arguments.size(); index++) {
                if (STORE.remove(key(arguments.get(index))) != null) {
                    deleted++;
                }
            }
            writeInteger(output, deleted);
        }
        return true;
    }

    private static boolean exists(List<byte[]> arguments, OutputStream output) throws IOException {
        if (arguments.size() < 2) {
            writeArityError(output, "exists");
        } else {
            long found = 0;
            for (int index = 1; index < arguments.size(); index++) {
                if (STORE.containsKey(key(arguments.get(index)))) {
                    found++;
                }
            }
            writeInteger(output, found);
        }
        return true;
    }

    private static boolean quit(List<byte[]> arguments, OutputStream output) throws IOException {
        if (arguments.size() != 1) {
            writeArityError(output, "quit");
            return true;
        }
        writeSimpleString(output, "OK");
        return false;
    }

    //`````````````````````````````````````````````````````````````````````
    // ISO-8859-1 maps every byte to one distinct character, giving byte-array
    // keys stable value equality without changing their wire representation.
    //
    private static String key(byte[] value) {
        return new String(value, StandardCharsets.ISO_8859_1);
    }

    private static int readLength(InputStream input, String label) throws IOException {
        String text = readLine(input);
        try {
            return Integer.parseInt(text);
        } catch (NumberFormatException exception) {
            throw new ProtocolException(label + " is not an integer");
        }
    }

    private static String readLine(InputStream input) throws IOException {
        var line = new ByteArrayOutputStream();
        while (line.size() <= MAX_NUMBER_BYTES) {
            int next = input.read();
            if (next == -1) {
                throw new EOFException("connection ended inside a length prefix");
            }
            if (next == '\r') {
                if (input.read() != '\n') {
                    throw new ProtocolException("length prefixes must end with CRLF");
                }
                return line.toString(StandardCharsets.US_ASCII);
            }
            if (next == '\n') {
                throw new ProtocolException("length prefixes must end with CRLF");
            }
            line.write(next);
        }
        throw new ProtocolException("length prefix is too long");
    }

    private static void requireCrlf(InputStream input) throws IOException {
        if (input.read() != '\r' || input.read() != '\n') {
            throw new ProtocolException("bulk strings must end with CRLF");
        }
    }

    private static void writeSimpleString(OutputStream output, String value) throws IOException {
        writeAscii(output, "+" + value + "\r\n");
    }

    private static void writeError(OutputStream output, String message) throws IOException {
        writeAscii(output, "-ERR " + message + "\r\n");
    }

    private static void writeArityError(OutputStream output, String command) throws IOException {
        writeError(output, "wrong number of arguments for '" + command + "' command");
    }

    private static void writeInteger(OutputStream output, long value) throws IOException {
        writeAscii(output, ":" + value + "\r\n");
    }

    private static void writeBulkString(OutputStream output, byte[] value) throws IOException {
        writeAscii(output, "$" + value.length + "\r\n");
        output.write(value);
        writeAscii(output, "\r\n");
    }

    private static void writeNullBulkString(OutputStream output) throws IOException {
        writeAscii(output, "$-1\r\n");
    }

    private static void writeAscii(OutputStream output, String value) throws IOException {
        output.write(value.getBytes(StandardCharsets.US_ASCII));
    }

    private static int parsePort(String[] args) {
        if (args.length > 1) {
            throw new IllegalArgumentException("Usage: java RedisRespServer.java [port]");
        }
        int port = args.length == 0 ? DEFAULT_PORT : Integer.parseInt(args[0]);
        if (port < 1 || port > 65_535) {
            throw new IllegalArgumentException("Port must be between 1 and 65535");
        }
        return port;
    }

    private static class ProtocolException extends IOException {
        private ProtocolException(String message) {
            super(message);
        }
    }

    private static class InvalidIntegerException extends RuntimeException {
    }
}
