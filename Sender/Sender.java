import java.io.*;
import java.net.*;
import java.nio.file.Files;
import java.util.*;

public class Sender {

    private static final int PACKET_SIZE = 128;

    public static void main(String[] args) throws Exception {
        if (args.length < 5) {
            System.out.println("Usage: java Sender <rcv_ip> <rcv_data_port> <sender_ack_port> <input_file> <timeout_ms> [window_size]");
            return;
        }

        InetAddress rcvIP = InetAddress.getByName(args[0]);
        int rcvPort = Integer.parseInt(args[1]);
        int ackPort = Integer.parseInt(args[2]);
        String inputFile = args[3];
        int timeoutMs = Integer.parseInt(args[4]);
        boolean useGBN = args.length == 6;
        int windowSize = useGBN ? Integer.parseInt(args[5]) : 1;

        DatagramSocket sendSocket = new DatagramSocket();
        DatagramSocket ackSocket = new DatagramSocket(ackPort);
        ackSocket.setSoTimeout(timeoutMs);

        // Read file bytes
        File file = new File(inputFile);
        byte[] fileBytes = file.length() > 0 ? Files.readAllBytes(file.toPath()) : new byte[0];

        long startTime = System.currentTimeMillis();

        // ---- PHASE 1: HANDSHAKE ----
        DSPacket sot = new DSPacket(DSPacket.TYPE_SOT, 0, new byte[0]);
        sendPacket(sendSocket, sot, rcvIP, rcvPort);

        while (true) {
            try {
                DSPacket ack = receiveACK(ackSocket);
                if (ack.getType() == DSPacket.TYPE_ACK && ack.getSeqNum() == 0) break;
            } catch (SocketTimeoutException e) {
                sendPacket(sendSocket, sot, rcvIP, rcvPort);
            }
        }

        // ---- PHASE 2: DATA TRANSFER ----
        if (fileBytes.length == 0) {
            // Empty file → send EOT immediately
            DSPacket eot = new DSPacket(DSPacket.TYPE_EOT, 1, new byte[0]);
            sendPacket(sendSocket, eot, rcvIP, rcvPort);
            while (true) {
                try {
                    DSPacket ack = receiveACK(ackSocket);
                    if (ack.getType() == DSPacket.TYPE_ACK && ack.getSeqNum() == 1) break;
                } catch (SocketTimeoutException e) {
                    sendPacket(sendSocket, eot, rcvIP, rcvPort);
                }
            }
        } else if (!useGBN) {
            // ---- STOP-AND-WAIT ----
            int seq = 1;
            for (int offset = 0; offset < fileBytes.length; offset += DSPacket.MAX_PAYLOAD_SIZE) {
                int len = Math.min(DSPacket.MAX_PAYLOAD_SIZE, fileBytes.length - offset);
                byte[] payload = Arrays.copyOfRange(fileBytes, offset, offset + len);
                DSPacket data = new DSPacket(DSPacket.TYPE_DATA, seq, payload);

                int timeoutCount = 0;
                while (true) {
                    sendPacket(sendSocket, data, rcvIP, rcvPort);
                    try {
                        DSPacket ack = receiveACK(ackSocket);
                        if (ack.getType() == DSPacket.TYPE_ACK && ack.getSeqNum() == seq) break;
                    } catch (SocketTimeoutException e) {
                        timeoutCount++;
                        if (timeoutCount >= 3) {
                            System.err.println("Critical failure: 3 consecutive timeouts. Transfer aborted.");
                            sendSocket.close();
                            ackSocket.close();
                            return;
                        }
                    }
                }
                seq = (seq + 1) % 128;
            }
        } else {
            // ---- GO-BACK-N ----
            int base = 1, nextSeq = 1;
            int totalPackets = (fileBytes.length + DSPacket.MAX_PAYLOAD_SIZE - 1) / DSPacket.MAX_PAYLOAD_SIZE;
            DSPacket[] packets = new DSPacket[totalPackets + 1]; // index 1..totalPackets

            for (int i = 0; i < totalPackets; i++) {
                int offset = i * DSPacket.MAX_PAYLOAD_SIZE;
                int len = Math.min(DSPacket.MAX_PAYLOAD_SIZE, fileBytes.length - offset);
                byte[] payload = Arrays.copyOfRange(fileBytes, offset, offset + len);
                packets[i + 1] = new DSPacket(DSPacket.TYPE_DATA, (i + 1) % 128, payload);
            }

            int timeoutCount = 0;
            while (base <= totalPackets) {
                // send window
                List<DSPacket> window = new ArrayList<>();
                for (int i = nextSeq; i < base + windowSize && i <= totalPackets; i++) {
                    window.add(packets[i]);
                }
                // apply ChaosEngine permutation for every 4 packets
                if (window.size() == 4) {
                    window = ChaosEngine.permutePackets(window);
                }

                for (DSPacket p : window) sendPacket(sendSocket, p, rcvIP, rcvPort);
                nextSeq = base + window.size();

                // wait for ACK
                try {
                    DSPacket ack = receiveACK(ackSocket);
                    int ackSeq = ack.getSeqNum();
                    if (betweenModulo(base, nextSeq, ackSeq)) {
                        int shift = (ackSeq - base + 128) % 128 + 1;
                        base += shift;
                        timeoutCount = 0;
                    }
                } catch (SocketTimeoutException e) {
                    timeoutCount++;
                    if (timeoutCount >= 3) {
                        System.err.println("Critical failure: 3 consecutive timeouts. Transfer aborted.");
                        sendSocket.close();
                        ackSocket.close();
                        return;
                    }
                }
            }
        }

        // ---- PHASE 3: TEARDOWN ----
        int lastSeq = fileBytes.length == 0 ? 1 : ((fileBytes.length - 1) / DSPacket.MAX_PAYLOAD_SIZE + 1 + 1) % 128;
        DSPacket eot = new DSPacket(DSPacket.TYPE_EOT, lastSeq, new byte[0]);
        sendPacket(sendSocket, eot, rcvIP, rcvPort);

        while (true) {
            try {
                DSPacket ack = receiveACK(ackSocket);
                if (ack.getType() == DSPacket.TYPE_ACK && ack.getSeqNum() == lastSeq) break;
            } catch (SocketTimeoutException e) {
                sendPacket(sendSocket, eot, rcvIP, rcvPort);
            }
        }

        long endTime = System.currentTimeMillis();
        System.out.printf("Total Transmission Time: %.2f seconds%n", (endTime - startTime) / 1000.0);

        sendSocket.close();
        ackSocket.close();
    }

    private static void sendPacket(DatagramSocket socket, DSPacket packet, InetAddress ip, int port) throws IOException {
        byte[] data = packet.toBytes();
        DatagramPacket dp = new DatagramPacket(data, data.length, ip, port);
        socket.send(dp);
    }

    private static DSPacket receiveACK(DatagramSocket socket) throws IOException {
        byte[] buf = new byte[PACKET_SIZE];
        DatagramPacket dp = new DatagramPacket(buf, buf.length);
        socket.receive(dp);
        return new DSPacket(dp.getData());
    }

    private static boolean betweenModulo(int base, int nextSeq, int ackSeq) {
        int mod = 128;
        return ((ackSeq - base + mod) % mod) < ((nextSeq - base + mod) % mod);
    }
}