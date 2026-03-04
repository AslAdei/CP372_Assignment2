import java.io.*;
import java.net.*;
import java.util.*;

public class Receiver {

    private static final int PACKET_SIZE = 128;

    public static void main(String[] args) throws Exception {

        if (args.length != 5) {
            System.out.println("Usage: java Receiver <sender_ip> <sender_ack_port> <rcv_data_port> <output_file> <RN>");
            return;
        }

        InetAddress senderIP = InetAddress.getByName(args[0]);
        int senderAckPort = Integer.parseInt(args[1]);
        int rcvPort = Integer.parseInt(args[2]);
        String outputFile = args[3];
        int RN = Integer.parseInt(args[4]);

        DatagramSocket dataSocket = new DatagramSocket(rcvPort);
        DatagramSocket ackSocket = new DatagramSocket(); // Single socket for all ACKs

        FileOutputStream fos = new FileOutputStream(outputFile);

        int expectedSeq = 0;
        int ackCount = 0;
        Map<Integer, DSPacket> buffer = new TreeMap<>(); // GBN buffering

        System.out.println("Receiver started on port " + rcvPort);

        boolean running = true;

        while (running) {
            byte[] buf = new byte[PACKET_SIZE];
            DatagramPacket dp = new DatagramPacket(buf, buf.length);
            dataSocket.receive(dp);

            DSPacket pkt = new DSPacket(dp.getData());
            
            int type = pkt.getType();
            int seq = pkt.getSeqNum();

            System.out.println("Received packet: type=" + type + " seq=" + seq);

            if (type == DSPacket.TYPE_SOT) {
                ackCount++;
                if (!ChaosEngine.shouldDrop(ackCount, RN)) {
                    sendACK(seq, senderIP, senderAckPort, ackSocket);
                    System.out.println("Sent SOT ACK for seq=" + seq);
                }
                expectedSeq = 1; // first DATA seq
            } 
            else if (type == DSPacket.TYPE_DATA) {
                if (seq == expectedSeq) {
                    fos.write(pkt.getPayload(), 0, pkt.getLength());
                    expectedSeq = (expectedSeq + 1) % 128;

                    // Deliver buffered packets in order
                    while (buffer.containsKey(expectedSeq)) {
                        DSPacket p = buffer.remove(expectedSeq);
                        fos.write(p.getPayload(), 0, p.getLength());
                        expectedSeq = (expectedSeq + 1) % 128;
                    }

                    ackCount++;
                    if (!ChaosEngine.shouldDrop(ackCount, RN)) {
                        int ackSeq = (expectedSeq - 1 + 128) % 128;
                        sendACK(ackSeq, senderIP, senderAckPort, ackSocket);
                        System.out.println("Sent ACK for seq=" + ackSeq);
                    }
                } else {
                    if (!buffer.containsKey(seq)) {
                        buffer.put(seq, pkt);
                        System.out.println("Buffered out-of-order packet seq=" + seq);
                    }

                    ackCount++;
                    if (!ChaosEngine.shouldDrop(ackCount, RN)) {
                        int ackSeq = (expectedSeq - 1 + 128) % 128;
                        sendACK(ackSeq, senderIP, senderAckPort, ackSocket);
                        System.out.println("Sent cumulative ACK for seq=" + ackSeq);
                    }
                }
            } 
            else if (type == DSPacket.TYPE_EOT) {
                ackCount++;
                if (!ChaosEngine.shouldDrop(ackCount, RN)) {
                    sendACK(seq, senderIP, senderAckPort, ackSocket);
                    System.out.println("Sent EOT ACK for seq=" + seq);
                }
                running = false;
            }
        }

        System.out.println("File transfer complete. Closing sockets.");
        fos.close();
        dataSocket.close();
        ackSocket.close();
    }

    private static void sendACK(int seq, InetAddress ip, int port, DatagramSocket sock) throws IOException {
        DSPacket ack = new DSPacket(DSPacket.TYPE_ACK, seq, new byte[0]);
        byte[] data = ack.toBytes();
        DatagramPacket dp = new DatagramPacket(data, data.length, ip, port);
        sock.send(dp);
    }
}