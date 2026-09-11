import java.io.IOException;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.time.Instant;
import java.util.concurrent.Executors;

//~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
// TimeServer implements the RFC 868 Time Protocol over both TCP and UDP.
// Each response is a four-byte unsigned count of seconds since the beginning
// of January 1, 1900 UTC, encoded in network byte order.
//
public class TimeServer {
    private static final int DEFAULT_PORT = 13037;
    private static final long SECONDS_1900_TO_1970 = 2_208_988_800L;

    //`````````````````````````````````````````````````````````````````````
    // Opens TCP and UDP listeners on the same numeric port, then keeps the
    // main thread in the TCP accept loop while a platform thread serves UDP.
    //
    public static void main(String[] args) throws Exception {
        int port = parsePort(args);
        var tcp = new ServerSocket(port);
        var udp = new DatagramSocket(port);

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            try {
                tcp.close();
            } catch (IOException ignored) {
            }
            udp.close();
        }));

        Thread.ofPlatform().start(() -> serveUdp(udp));
        System.out.printf("TIME_READY tcp=%d udp=%d%n", port, port);

        try (var clients = Executors.newVirtualThreadPerTaskExecutor()) {
            while (true) {
                Socket client = tcp.accept();               // wait for one TCP peer
                clients.submit(() -> serveTcp(client));     // send its four-byte reply
            }
        }
    }

    //`````````````````````````````````````````````````````````````````````
    // Sends the current four-byte timestamp as soon as a TCP client connects.
    // Closing the connection marks the end of the RFC 868 response.
    //
    private static void serveTcp(Socket client) {
        try (client) {
            client.getOutputStream().write(timeMessage());
        } catch (IOException exception) {
            System.err.println("Time TCP request failed: " + exception.getMessage());
        }
    }

    //`````````````````````````````````````````````````````````````````````
    // Treats any received datagram as a time request and sends the four-byte
    // timestamp to the source address and source port of that datagram.
    //
    private static void serveUdp(DatagramSocket socket) {
        byte[] requestBuffer = new byte[65_507];
        while (!socket.isClosed()) {
            try {
                var request = new DatagramPacket(requestBuffer, requestBuffer.length);
                socket.receive(request);
                byte[] responseBody = timeMessage();
                var response = new DatagramPacket(
                        responseBody, responseBody.length, request.getAddress(), request.getPort());
                socket.send(response);
            } catch (IOException exception) {
                if (!socket.isClosed()) {
                    System.err.println("Time UDP request failed: " + exception.getMessage());
                }
            }
        }
    }

    //`````````````````````````````````````````````````````````````````````
    // Converts Java's 1970-based Unix time to the RFC 868 epoch. Casting to an
    // int preserves the low 32 bits that make up the protocol's wire value.
    //
    private static byte[] timeMessage() {
        long seconds = Instant.now().getEpochSecond() + SECONDS_1900_TO_1970;
        return ByteBuffer.allocate(Integer.BYTES).putInt((int) seconds).array();
    }

    //`````````````````````````````````````````````````````````````````````
    // Uses the service's unprivileged default port unless the command line
    // supplies one valid TCP/UDP port number.
    //
    private static int parsePort(String[] args) {
        if (args.length > 1) {
            throw new IllegalArgumentException("Usage: java TimeServer.java [port]");
        }
        int port = args.length == 0 ? DEFAULT_PORT : Integer.parseInt(args[0]);
        if (port < 1 || port > 65_535) {
            throw new IllegalArgumentException("Port must be between 1 and 65535");
        }
        return port;
    }
}
