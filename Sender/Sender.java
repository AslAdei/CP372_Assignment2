import java.io.*;
import java.net.*;
import java.nio.file.Files;
import java.util.*;

public class Sender {

    private static final int PACKET_SIZE = 128;
    private static final int MOD = 128;

    public static void main(String[] args) throws Exception {
        if (args.length < 5) {
            System.out.println("Usage: java Sender <rcv_ip> <rcv_data_port> <sender_ack_port> <input_file> <timeout_ms> [window_size]");
            return;
        }

        InetAddress rcvIP = InetAddress.getByName(args[0]);
        int rcvDataPort = Integer.parseInt(args[1]);
        int senderAckPort = Integer.parseInt(args[2]);
        String inputFile = args[3];
        int timeoutMs = Integer.parseInt(args[4]);

        boolean useGBN = (args.length == 6);
        int windowSize = useGBN ? Integer.parseInt(args[5]) : 1;

        if (useGBN) {
            if (windowSize <= 0 || windowSize > 128 || windowSize % 4 != 0) {
                System.out.println("window_size must be a positive multiple of 4 and <= 128");
                return;
            }
        }

        DatagramSocket sendSocket = new DatagramSocket();
        DatagramSocket ackSocket = new DatagramSocket(senderAckPort);
        ackSocket.setSoTimeout(timeoutMs);

        File file = new File(inputFile);
        byte[] fileBytes = (file.length() > 0) ? Files.readAllBytes(file.toPath()) : new byte[0];

        long startTime = System.currentTimeMillis();

        // Handshake: SOT seq 0
        DSPacket sot = new DSPacket(DSPacket.TYPE_SOT, 0, new byte[0]);
        if (!sendUntilSpecificAck(sendSocket, ackSocket, sot, rcvIP, rcvDataPort, 0)) {
            sendSocket.close();
            ackSocket.close();
            return;
        }

        // Empty file: send EOT seq 1 immediately after handshake
        if (fileBytes.length == 0) {
            DSPacket eot = new DSPacket(DSPacket.TYPE_EOT, 1, new byte[0]);
            if (!sendUntilSpecificAck(sendSocket, ackSocket, eot, rcvIP, rcvDataPort, 1)) {
                sendSocket.close();
                ackSocket.close();
                return;
            }

            long endTime = System.currentTimeMillis();
            System.out.printf("Total Transmission Time: %.2f seconds%n", (endTime - startTime) / 1000.0);
            sendSocket.close();
            ackSocket.close();
            return;
        }

        // Build DATA packets: first DATA seq = 1, modulo 128
        int totalPackets = (fileBytes.length + DSPacket.MAX_PAYLOAD_SIZE - 1) / DSPacket.MAX_PAYLOAD_SIZE;
        DSPacket[] packets = new DSPacket[totalPackets + 1]; // use 1..totalPackets

        for (int i = 1; i <= totalPackets; i++) {
            int offset = (i - 1) * DSPacket.MAX_PAYLOAD_SIZE;
            int len = Math.min(DSPacket.MAX_PAYLOAD_SIZE, fileBytes.length - offset);
            byte[] payload = Arrays.copyOfRange(fileBytes, offset, offset + len);
            int seq = i % MOD;
            packets[i] = new DSPacket(DSPacket.TYPE_DATA, seq, payload);
        }

        if (!useGBN) {
            runStopAndWait(sendSocket, ackSocket, rcvIP, rcvDataPort, packets, totalPackets);
        } else {
            runGBN(sendSocket, ackSocket, rcvIP, rcvDataPort, packets, totalPackets, windowSize);
        }

        // EOT = (last DATA seq + 1) mod 128
        int lastDataSeq = packets[totalPackets].getSeqNum();
        int eotSeq = (lastDataSeq + 1) % MOD;

        DSPacket eot = new DSPacket(DSPacket.TYPE_EOT, eotSeq, new byte[0]);
        if (!sendUntilSpecificAck(sendSocket, ackSocket, eot, rcvIP, rcvDataPort, eotSeq)) {
            sendSocket.close();
            ackSocket.close();
            return;
        }

        long endTime = System.currentTimeMillis();
        System.out.printf("Total Transmission Time: %.2f seconds%n", (endTime - startTime) / 1000.0);

        sendSocket.close();
        ackSocket.close();
    }

    private static void runStopAndWait(DatagramSocket sendSocket,
                                       DatagramSocket ackSocket,
                                       InetAddress rcvIP,
                                       int rcvDataPort,
                                       DSPacket[] packets,
                                       int totalPackets) throws IOException {

        for (int i = 1; i <= totalPackets; i++) {
            DSPacket data = packets[i];
            int timeoutCount = 0;
            boolean acked = false;

            while (!acked) {
                sendPacket(sendSocket, data, rcvIP, rcvDataPort);

                try {
                    DSPacket ack = receiveACK(ackSocket);
                    if (ack.getType() == DSPacket.TYPE_ACK && ack.getSeqNum() == data.getSeqNum()) {
                        acked = true;
                    }
                } catch (SocketTimeoutException e) {
                    timeoutCount++;
                    if (timeoutCount >= 3) {
                        System.out.println("Unable to transfer file.");
                        sendSocket.close();
                        ackSocket.close();
                        System.exit(0);
                    }
                }
            }
        }
    }

    private static void runGBN(DatagramSocket sendSocket,
                               DatagramSocket ackSocket,
                               InetAddress rcvIP,
                               int rcvDataPort,
                               DSPacket[] packets,
                               int totalPackets,
                               int windowSize) throws IOException {

        int base = 1;
        int nextSeq = 1;
        int timeoutCount = 0;

        while (base <= totalPackets) {
            // Send new packets while nextSeq < base + N
            if (nextSeq < base + windowSize && nextSeq <= totalPackets) {
                List<DSPacket> batch = new ArrayList<>();
                int limit = Math.min(base + windowSize - 1, totalPackets);

                while (nextSeq <= limit) {
                    batch.add(packets[nextSeq]);
                    nextSeq++;
                }

                sendBatchWithChaos(sendSocket, batch, rcvIP, rcvDataPort);
            }

            try {
                DSPacket ack = receiveACK(ackSocket);

                if (ack.getType() == DSPacket.TYPE_ACK) {
                    int ackIndex = mapAckSeqToPacketIndex(base, nextSeq - 1, ack.getSeqNum(), packets);

                    // Cumulative ACK: move base past the acknowledged packet.
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
                    System.exit(0);
                }

                // Retransmit entire window from base
                List<DSPacket> resendBatch = new ArrayList<>();
                for (int i = base; i < nextSeq; i++) {
                    resendBatch.add(packets[i]);
                }

                sendBatchWithChaos(sendSocket, resendBatch, rcvIP, rcvDataPort);
            }
        }
    }

    private static boolean sendUntilSpecificAck(DatagramSocket sendSocket,
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

    private static void sendPacket(DatagramSocket socket,
                                   DSPacket packet,
                                   InetAddress ip,
                                   int port) throws IOException {
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

    private static void sendBatchWithChaos(DatagramSocket socket,
                                           List<DSPacket> batch,
                                           InetAddress ip,
                                           int port) throws IOException {
        int i = 0;

        while (i < batch.size()) {
            int remaining = batch.size() - i;

            if (remaining >= 4) {
                List<DSPacket> group = new ArrayList<>();
                group.add(batch.get(i));
                group.add(batch.get(i + 1));
                group.add(batch.get(i + 2));
                group.add(batch.get(i + 3));

                List<DSPacket> permuted = ChaosEngine.permutePackets(group);
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

    private static int mapAckSeqToPacketIndex(int start, int end, int ackSeq, DSPacket[] packets) {
        for (int i = start; i <= end; i++) {
            if (packets[i].getSeqNum() == ackSeq) {
                return i;
            }
        }
        return -1;
    }
}
