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

        if (useGBN) {
            if (windowSize <= 0 || windowSize > 128 || windowSize % 4 != 0) {
                System.out.println("window_size must be a positive multiple of 4 and <= 128");
                return;
            }
        }
        
        DatagramSocket sendSocket = new DatagramSocket();
        DatagramSocket ackSocket = new DatagramSocket(ackPort);
        ackSocket.setSoTimeout(timeoutMs);

        // Read file bytes
        File file = new File(inputFile);
        byte[] fileBytes = file.length() > 0 ? Files.readAllBytes(file.toPath()) : new byte[0];

        long startTime = System.currentTimeMillis();

        // ---- PHASE 1: HANDSHAKE ----
        DSPacket sot = new DSPacket(DSPacket.TYPE_SOT, 0, new byte[0]);

        if (!sendUntilAck(sendSocket, ackSocket, sot, rcvIP, rcvPort, 0)) {
            sendSocket.close();
            ackSocket.close();
            return;
        }

        // ---- PHASE 2: DATA TRANSFER ----
        if (fileBytes.length == 0) {
            DSPacket eot = new DSPacket(DSPacket.TYPE_EOT, 1, new byte[0]);

            if (!sendUntilAck(sendSocket, ackSocket, eot, rcvIP, rcvPort, 1)) {
                sendSocket.close();
                ackSocket.close();
                return;
            }
        }
        
        } else if (!useGBN) {
            int seq = 1;

            for (int offset = 0; offset < fileBytes.length; offset += DSPacket.MAX_PAYLOAD_SIZE) {
                int len = Math.min(DSPacket.MAX_PAYLOAD_SIZE, fileBytes.length - offset);
                byte[] payload = Arrays.copyOfRange(fileBytes, offset, offset + len);
                DSPacket data = new DSPacket(DSPacket.TYPE_DATA, seq, payload);

                int timeoutCount = 0;
                boolean acked = false;

                while (!acked) {
                    sendPacket(sendSocket, data, rcvIP, rcvPort);

                    try {
                        DSPacket ack = receiveACK(ackSocket);

                        if (ack.getType() == DSPacket.TYPE_ACK && ack.getSeqNum() == seq) {
                            acked = true;
                        }
                    } catch (SocketTimeoutException e) {
                        timeoutCount++;

                        if (timeoutCount >= 3) {
                            System.out.println("Unable to transfer file.");
                            sendSocket.close();
                            ackSocket.close();
                            return;
                        }
                    }
                }

                seq = (seq + 1) % 128;
            }
    
        } else {
            int totalPackets = (fileBytes.length + DSPacket.MAX_PAYLOAD_SIZE - 1) / DSPacket.MAX_PAYLOAD_SIZE;
            DSPacket[] packets = new DSPacket[totalPackets + 1]; // use indices 1..totalPackets

            for (int i = 1; i <= totalPackets; i++) {
                int offset = (i - 1) * DSPacket.MAX_PAYLOAD_SIZE;
                int len = Math.min(DSPacket.MAX_PAYLOAD_SIZE, fileBytes.length - offset);
                byte[] payload = Arrays.copyOfRange(fileBytes, offset, offset + len);
                packets[i] = new DSPacket(DSPacket.TYPE_DATA, i % 128, payload);
            }

            int base = 1;
            int nextToSend = 1;
            int timeoutCount = 0;

            while (base <= totalPackets) {

                // Send all unsent packets that fit in the current window
                if (nextToSend < base + windowSize && nextToSend <= totalPackets) {
                    List<DSPacket> batch = new ArrayList<>();

                    int limit = Math.min(base + windowSize - 1, totalPackets);
                    while (nextToSend <= limit) {
                        batch.add(packets[nextToSend]);
                        nextToSend++;
                    }

                    sendBatchWithChaos(sendSocket, batch, rcvIP, rcvPort);
                }

                try {
                    DSPacket ack = receiveACK(ackSocket);

                    if (ack.getType() == DSPacket.TYPE_ACK) {
                        int ackIndex = mapAckSeqToPacketIndex(base, nextToSend - 1, ack.getSeqNum(), packets);

                        if (ackIndex >= base) {
                            base = ackIndex + 1;
                            timeoutCount = 0;
                        }
                    }
                } catch (SocketTimeoutException e) {
                    timeoutCount++;

                    if (timeoutCount >= 3) {
                        System.out.println("Unable to transfer file.");
                        sendSocket.close();
                        ackSocket.close();
                        return;
                    }

                    List<DSPacket> resendBatch = new ArrayList<>();
                    for (int i = base; i < nextToSend; i++) {
                        resendBatch.add(packets[i]);
                    }

                    sendBatchWithChaos(sendSocket, resendBatch, rcvIP, rcvPort);
                }
            }
        }

        // ---- PHASE 3: TEARDOWN ----
        int lastSeq = fileBytes.length == 0
                ? 1
                : (((fileBytes.length - 1) / DSPacket.MAX_PAYLOAD_SIZE) + 2) % 128;

        DSPacket eot = new DSPacket(DSPacket.TYPE_EOT, lastSeq, new byte[0]);

        if (!sendUntilAck(sendSocket, ackSocket, eot, rcvIP, rcvPort, lastSeq)) {
            sendSocket.close();
            ackSocket.close();
            return;
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
    
    private static boolean sendUntilAck(DatagramSocket sendSocket,
                                        DatagramSocket ackSocket,
                                        DSPacket packet,
                                        InetAddress ip,
                                        int port,
                                        int expectedAckSeq) throws IOException {
        int timeoutCount = 0;
        while (true) {
            sendPacket(sendSocket, packet, ip, port);
            try {
                DSPacket ack = receiveACK(ackSocket);
                if (ack.getType() == DSPacket.TYPE_ACK && ack.getSeqNum() == expectedAckSeq) {
                    return true;
                }
            } catch (SocketTimeoutException e) {
                timeoutCount++;
                if (timeoutCount >= 3) {
                    System.out.println("Unable to transfer file.");
                    return false;
                }
            }
        }
    }

    private static void sendBatchWithChaos(DatagramSocket socket,
                                           List<DSPacket> batch,
                                           InetAddress ip,
                                           int port) throws IOException {
        int i = 0;

        while (i < batch.size()) {
            int remaining = batch.size() - i;

            if (remaining >= 4) {
                List<DSPacket> four = new ArrayList<>();
                four.add(batch.get(i));
                four.add(batch.get(i + 1));
                four.add(batch.get(i + 2));
                four.add(batch.get(i + 3));

                List<DSPacket> permuted = ChaosEngine.permutePackets(four);
                for (DSPacket p : permuted) {
                    sendPacket(socket, p, ip, port);
                }

                i += 4;
            } else {
                sendPacket(socket, batch.get(i), ip, port);
                i++;
            }
        }
    }

    private static int mapAckSeqToPacketIndex(int base, int end, int ackSeq, DSPacket[] packets) {
        for (int i = base; i <= end; i++) {
            if (packets[i].getSeqNum() == ackSeq) {
                return i;
            }
        }
        return -1;
    }

}
