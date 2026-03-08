import java.io.*;
import java.net.*;
import java.util.*;

public class Receiver {

    private static final int PACKET_SIZE = 128;
    private static final int MOD = 128;

    public static void main(String[] args) throws Exception {
        if (args.length != 5) {
            System.out.println("Usage: java Receiver <sender_ip> <sender_ack_port> <rcv_data_port> <output_file> <RN>");
            return;
        }

        InetAddress senderIP = InetAddress.getByName(args[0]);
        int senderAckPort = Integer.parseInt(args[1]);
        int rcvDataPort = Integer.parseInt(args[2]);
        String outputFile = args[3];
        int RN = Integer.parseInt(args[4]);

        DatagramSocket dataSocket = new DatagramSocket(rcvDataPort);
        DatagramSocket ackSocket = new DatagramSocket();

        FileOutputStream fos = new FileOutputStream(outputFile);

        int expectedSeq = 0;       // becomes 1 after SOT
        int ackCount = 0;
        boolean connected = false;
        boolean running = true;

        // Buffer only for out-of-order DATA packets.
        Map<Integer, DSPacket> buffer = new HashMap<>();

        System.out.println("Receiver started on port " + rcvDataPort);

        while (running) {
            byte[] buf = new byte[PACKET_SIZE];
            DatagramPacket udpPacket = new DatagramPacket(buf, buf.length);
            dataSocket.receive(udpPacket);

            DSPacket pkt = new DSPacket(udpPacket.getData());
            int type = pkt.getType();
            int seq = pkt.getSeqNum();

            System.out.println("Received packet: type=" + type + " seq=" + seq);

            if (type == DSPacket.TYPE_SOT) {
                // Only accept the required SOT seq=0
                if (seq == 0) {
                    connected = true;
                    expectedSeq = 1;
                    buffer.clear();

                    ackCount++;
                    if (!ChaosEngine.shouldDrop(ackCount, RN)) {
                        sendACK(0, senderIP, senderAckPort, ackSocket);
                        System.out.println("Sent SOT ACK for seq=0");
                    } else {
                        System.out.println("Dropped SOT ACK for seq=0");
                    }
                }
                continue;
            }

            // Ignore anything before successful SOT.
            if (!connected) {
                continue;
            }

            if (type == DSPacket.TYPE_DATA) {
                // Stop-and-Wait behavior is naturally covered when sender only has
                // one in-flight packet. For GBN, we buffer out-of-order packets and
                // always send cumulative ACKs.

                if (seq == expectedSeq) {
                    // In-order packet: write immediately, then flush buffered run.
                    fos.write(pkt.getPayload(), 0, pkt.getLength());
                    expectedSeq = inc(expectedSeq);

                    while (buffer.containsKey(expectedSeq)) {
                        DSPacket buffered = buffer.remove(expectedSeq);
                        fos.write(buffered.getPayload(), 0, buffered.getLength());
                        expectedSeq = inc(expectedSeq);
                    }
                } else {
                    // Out-of-order or duplicate:
                    // - duplicate / below-window => do not write
                    // - future packet => buffer it if not already present
                    //
                    // Since the receiver CLI provides no window_size, we keep the
                    // acceptance broad enough to support the sender's legal GBN runs
                    // without altering the required CLI.
                    if (isFutureSeq(seq, expectedSeq) && !buffer.containsKey(seq)) {
                        buffer.put(seq, pkt);
                        System.out.println("Buffered out-of-order packet seq=" + seq);
                    }
                }

                // Always send cumulative ACK = last contiguous in-order delivered.
                int ackSeq = lastInOrder(expectedSeq);
                ackCount++;
                if (!ChaosEngine.shouldDrop(ackCount, RN)) {
                    sendACK(ackSeq, senderIP, senderAckPort, ackSocket);
                    System.out.println("Sent cumulative ACK for seq=" + ackSeq);
                } else {
                    System.out.println("Dropped DATA ACK for seq=" + ackSeq);
                }

            } else if (type == DSPacket.TYPE_EOT) {
                // ACK EOT and terminate.
                ackCount++;
                if (!ChaosEngine.shouldDrop(ackCount, RN)) {
                    sendACK(seq, senderIP, senderAckPort, ackSocket);
                    System.out.println("Sent EOT ACK for seq=" + seq);
                } else {
                    System.out.println("Dropped EOT ACK for seq=" + seq);
                }
                running = false;
            }
        }

        fos.close();
        dataSocket.close();
        ackSocket.close();
        System.out.println("File transfer complete. Closing sockets.");
    }

    private static void sendACK(int seq, InetAddress ip, int port, DatagramSocket sock) throws IOException {
        DSPacket ack = new DSPacket(DSPacket.TYPE_ACK, seq, new byte[0]);
        byte[] data = ack.toBytes();
        DatagramPacket dp = new DatagramPacket(data, data.length, ip, port);
        sock.send(dp);
    }

    private static int inc(int seq) {
        return (seq + 1) % MOD;
    }

    private static int lastInOrder(int expectedSeq) {
        return (expectedSeq - 1 + MOD) % MOD;
    }

    private static int distanceForward(int from, int to) {
        return (to - from + MOD) % MOD;
    }

    private static boolean isFutureSeq(int seq, int expectedSeq) {
        // True if seq is ahead of expectedSeq in modulo-128 space.
        // Excludes duplicate/current packet.
        return distanceForward(expectedSeq, seq) > 0;
    }
}

