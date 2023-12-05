import java.net.*;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.*;

// Sliding window flow control
// Detection and ARQ based retransmission

public class GUDPSocket implements GUDPSocketAPI {


    DatagramSocket datagramSocket;
    // Two threads for sender/receiver operations
    Thread sendThread;
    Thread recvThread;
    

    // HashMap to keep track of different endpoint
    private HashMap<InetSocketAddress, myGUDPEndPoint> endPoints;

    public GUDPSocket(DatagramSocket socket) {
        datagramSocket = socket;
        endPoints = new HashMap<>();


        // sender thread loops through all endPoints to process each unique endPoint and its packetbuffer and send packets according to its window
        sendThread = new Thread(() -> {
            while (true) {
                try {
                    while(!endPoints.isEmpty()) {
                        for(myGUDPEndPoint endPoint : endPoints.values()) {
                            while(!endPoint.isEmptyBuffer() && endPoint.windowSize > 0){
                                    // Extract the packet that is next to be sent and update window
                                    if(endPoint.nextSeq < endPoint.baseSeq + endPoint.windowSize) {
                                        GUDPPacket gudpDATApack = endPoint.getPacket(endPoint.nextSeq);
                                        DatagramPacket udppacket = gudpDATApack.pack();
                                        datagramSocket.send(udppacket);
                                        System.out.println("Sent packet: " + gudpDATApack.getSeqno());

                                        // Start timer if first packet in window, if expired, retransmit all packets in window
                                        if(endPoint.baseSeq == endPoint.nextSeq) {
                                            endPoint.startTimer();
                                        }
                                        endPoint.nextSeq++;
                                        endPoint.setWindowSize(endPoint.windowSize - 1);
                                    }
                                    else {
                                        break;
                                    }

                            }
                        }

                    }
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        });
        sendThread.start();
    }

    // Extract dst socket address from incoming App packet, create endPoint and add the packet to its buffer
    public void send(DatagramPacket packet) throws IOException {
        InetSocketAddress sockAddr = new InetSocketAddress(packet.getAddress(), packet.getPort());
        System.out.println(sockAddr);
        myGUDPEndPoint endPoint = endPoints.get(sockAddr);
        if(endPoint == null) {
            endPoint = new myGUDPEndPoint(sockAddr);
            endPoints.put(sockAddr, endPoint);

            GUDPPacket gudpBSNpack = bsnPacket(sockAddr);
            endPoint.baseSeq = gudpBSNpack.getSeqno();
            endPoint.nextSeq = gudpBSNpack.getSeqno();
            endPoint.lastSeq = gudpBSNpack.getSeqno();
            endPoint.add(gudpBSNpack);
        }
        // Encapsulate packet into gudp packet, set its seqnum and add it to the endpoint buffer
        GUDPPacket gudpDATApack = GUDPPacket.encapsulate(packet);
        endPoint.setLastSeq(endPoint.getLastSeq() + 1);
        gudpDATApack.setSeqno(endPoint.getLastSeq());
        endPoint.add(gudpDATApack);
        System.out.println(endPoints);
    }

    public void receive(DatagramPacket packet) throws IOException {
        byte[] buf = new byte[GUDPPacket.MAX_DATAGRAM_LEN];
        DatagramPacket udppacket = new DatagramPacket(buf, buf.length);
        datagramSocket.receive(udppacket);
        GUDPPacket gudppacket = GUDPPacket.unpack(udppacket);
        gudppacket.decapsulate(packet);

    }

    public void finish() throws IOException {
        ;
    }

    public void close() throws IOException {
        ;
    }

    private GUDPPacket bsnPacket(InetSocketAddress sockAddr) {
        ByteBuffer buffer = ByteBuffer.allocate(GUDPPacket.HEADER_SIZE);
        buffer.order(ByteOrder.BIG_ENDIAN);
        GUDPPacket gudpBSNpacket = new GUDPPacket(buffer);
        gudpBSNpacket.setType(GUDPPacket.TYPE_BSN);
        gudpBSNpacket.setVersion(GUDPPacket.GUDP_VERSION);
        gudpBSNpacket.setSocketAddress(sockAddr);

        // Initial seq number randomized
        Random rnd = new Random();
        int bsn = Math.abs(rnd.nextInt());
        gudpBSNpacket.setSeqno(bsn);

        return gudpBSNpacket;
    }


    // Custom implementation of GUDPEndPoint to handle each unique endpoint
    private class myGUDPEndPoint {
        public static final int MAX_WINDOW_SIZE = 3;
        public static final int MAX_RETRY = 7;
        public static final long TIMEOUT_DURATION = 3000L; // (3 seconds)

        private InetSocketAddress remoteEndPoint;
        //private LinkedList<GUDPPacket> bufferList = new LinkedList<>();
        // Buffers to handle incoming/outgoing app datagrams
        private LinkedList<GUDPPacket> sendBuffer = new LinkedList<>();
        private LinkedList<GUDPPacket> recvBuffer = new LinkedList<>();

        private int windowSize;
        private int maxRetry;
        private long timeoutDuration;
        private int retry = 0;

        private int baseSeq;            // seq of sent packet not yet acked (i.e., base)
        private int nextSeq;      // seq of next packet to send (i.e., nextseqnum)
        private int lastSeq;            // seq of last packet in bufferList
        private int expectedSeq;  // seq of next packet to receive


        private myGUDPEndPoint(InetSocketAddress socketaddr) {
            this.remoteEndPoint = socketaddr;
            this.windowSize = MAX_WINDOW_SIZE;
            this.maxRetry = MAX_RETRY;
            this.timeoutDuration = TIMEOUT_DURATION;
            this.nextSeq = -1;
        }


        private InetSocketAddress getRemoteEndPoint() {
            return this.remoteEndPoint;
        }

        public int getLastSeq() {
            return this.lastSeq;
        }

        public void setLastSeq(int seq) {
            this.lastSeq = seq;
        }


        public void setWindowSize(int size) {
            if (this.windowSize > 0)
                this.windowSize = size;
            else{
                System.out.println("Window size cannot be negative");
            }
        }

        public void add(GUDPPacket gpacket) {
            synchronized (sendBuffer) {
                sendBuffer.add(gpacket);
            }
        }

        public void addBSN(GUDPPacket gpacket) {
            synchronized (sendBuffer) {
                sendBuffer.addFirst(gpacket);
            }
        }

        public void remove(GUDPPacket gpacket) {
            synchronized (sendBuffer) {
                sendBuffer.remove(gpacket);
            }
        }

        /*
         * Retrieve and remove the first packet from the bufferList
         */
        public GUDPPacket remove() {
            synchronized (sendBuffer) {
                return sendBuffer.remove();
            }
        }

        /*
         * Get the packet with the given sequence number from bufferList
         * IMPORTANT: the packet is still in the bufferList!
         */
        public GUDPPacket getPacket(int seq) {
            synchronized (sendBuffer) {
                for (int i = 0; i < sendBuffer.size(); i++) {
                    if (sendBuffer.get(i).getSeqno() == seq) {
                        return sendBuffer.get(i);
                    }
                }
                return null;
            }
        }

        /*
         * Remove all packets with sequence number below ACK from bufferList
         * Assuming those packets were successfully received
         */
        public void removeAllACK(int ack) {
            synchronized (sendBuffer) {
                while (sendBuffer.size() > 0) {
                    if (sendBuffer.getFirst().getSeqno() <= ack) {
                        sendBuffer.removeFirst();
                    } else {
                        break;
                    }
                }
                //notify();
            }
        }

        /*
         * Remove all packets from bufferList
         */
        public void removeAll() {
            synchronized (sendBuffer) {
                while (sendBuffer.size() > 0) {
                    sendBuffer.removeFirst();
                }
            }
        }

        public boolean isEmptyBuffer() {
            if (sendBuffer.size() == 0)
                return true;
            else
                return false;
        }

        /*
         * Timer uses for sending timeout
         */
        Timer timer;
        public void startTimer() {
            timer = new Timer("Timer");
            TimerTask task = new TimerTask() {
                public void run() {
                    System.out.print("TIMEOUT: " + remoteEndPoint.getAddress() + ":" + remoteEndPoint.getPort());
                    System.out.println("Retransmitting packets from: " + baseSeq);
                    nextSeq = baseSeq;
                    windowSize = MAX_WINDOW_SIZE;
                    timer.cancel();
                }
            };
            timer.schedule(task, timeoutDuration);
        }

        public void stopTimer() {
            timer.cancel();
        }

    }
    public static void main(String[] args) {
        try {
            // Create a DatagramSocket
            DatagramSocket socket = new DatagramSocket();

            // Create a GUDPSocket with the DatagramSocket
            GUDPSocket gudpSocket = new GUDPSocket(socket);

            // Create a DatagramPacket to send
            String msg = "testcase";
            byte[] buf = msg.getBytes();
            InetAddress address = InetAddress.getByName("localhost");
            int port = 1234;
            DatagramPacket packet = new DatagramPacket(buf, buf.length, address, port);

            // Send the packet
            gudpSocket.send(packet);
            System.out.print("Sent: " + msg);
            /*
            // Create a DatagramPacket for receiving data
            byte[] recBuf = new byte[256];
            DatagramPacket recPacket = new DatagramPacket(recBuf, recBuf.length);

            // Receive data into the packet
            gudpSocket.receive(recPacket);

            // Print the received data
            String received = new String(recPacket.getData(), 0, recPacket.getLength());
            System.out.println("Received: " + received);

             */
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}

