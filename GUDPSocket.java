/*
Problem: Guaranteed UDP (GUDP) to provide reliable data transfer over UDP with sliding window flow control and detection and ARQ based retransmissions
By: Jens Ekenblad, ekenblad@kth.se
Date: 01/10-23
 */


import java.net.*;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.*;


public class GUDPSocket implements GUDPSocketAPI {


    DatagramSocket datagramSocket;
    // Threads for sender/receiver operations
    Thread sendThread;
    Thread recvThread;
    

    // HashMaps to keep track of different endpoint both incoming and outgoing packets
    private HashMap<InetSocketAddress, myGUDPEndPoint> endPointsSend;
    private HashMap<InetSocketAddress, myGUDPEndPoint> endPointsRecv;


    public GUDPSocket(DatagramSocket socket) {
        datagramSocket = socket;
        this.endPointsSend = new HashMap<>();
        this.endPointsRecv = new HashMap<>();


        // sender thread loops through all endPoints to process each unique endPoint and its packetbuffer and send packets according to its window
        sendThread = new Thread(() -> {
            boolean running = true;
            while (running) {
                try {
                    synchronized (endPointsSend){
                        if(!endPointsSend.isEmpty()) {
                            for(myGUDPEndPoint endPoint : endPointsSend.values()) {
                                while(!endPoint.isEmptyBuffer() && endPoint.getPacket(endPoint.nextSeq) != null) {

                                    // Extract the packet that is next to be sent and update window
                                    if ((endPoint.nextSeq < endPoint.baseSeq + endPoint.windowSize)) {

                                        GUDPPacket gudpPacket = endPoint.getPacket(endPoint.nextSeq);
                                        DatagramPacket udppacket = gudpPacket.pack();
                                        datagramSocket.send(udppacket);


                                        System.out.println("Sent packet: " + "(" + gudpPacket.getType() + ")" + " " + gudpPacket.getSeqno());

                                        // Start timer if first packet in window, if expired, retransmit all packets in window
                                        if (endPoint.baseSeq == endPoint.nextSeq) {
                                            endPoint.startTimer();
                                        }
                                        endPoint.nextSeq++;
                                    } else {
                                        break;
                                    }
                                }
                                endPointsSend.notifyAll();
                            }
                            // Check if all endpoints are finished and empty or if timeout, if so terminate the thread
                            boolean allFinishedAndEmpty = true;
                            for(myGUDPEndPoint endPoint : endPointsSend.values()) {
                                if(endPoint.getTimeout()) {
                                    endPoint.setFinished(true);
                                    endPoint.removeAll();
                                }

                                if (!endPoint.isFinished() || !endPoint.isEmptyBuffer()) {
                                    allFinishedAndEmpty = false;
                                    break;
                                }
                            }
                            if(allFinishedAndEmpty) {
                                running = false;
                            }
                        }
                    }
                } catch (Exception e) {
                    System.out.println("Error sending packet: " + e);
                }
            }
        });sendThread.start();


        // receiver thread will listend for incoming packets and process it accordingly to which type it is and destination endPoint
        recvThread = new Thread(() -> {
            boolean running = true;
            while (running) {
                try {
                    // Read from network socket and extract and process packets as GUDP packets
                    byte[] buf = new byte[GUDPPacket.MAX_DATAGRAM_LEN];
                    DatagramPacket udppacket = new DatagramPacket(buf, buf.length);
                    datagramSocket.receive(udppacket);

                    InetSocketAddress sockAddr = new InetSocketAddress(udppacket.getAddress(), udppacket.getPort());
                    GUDPPacket gudppacket = GUDPPacket.unpack(udppacket);

                    // Sender side processing ACK
                    if (gudppacket.getType() == GUDPPacket.TYPE_ACK) {
                        synchronized (endPointsSend) {
                            endPointsSend.wait();
                            myGUDPEndPoint endPoint = endPointsSend.get(sockAddr);
                            System.out.println("Received ACK: " + gudppacket.getSeqno() + " from endpoint: " + endPoint.getRemoteEndPoint());
                            endPoint.removeAllACK(gudppacket.getSeqno() - 1);


                            // If FIN packet is acked, start closing else update window/timer
                            if(endPoint.getFinSeq() == gudppacket.getSeqno() - 1) {
                                endPoint.setFinished(true);
                                endPoint.stopTimer();
                            }
                            else {
                                endPoint.setBaseSeq(gudppacket.getSeqno()); // +1 ?
                                if (endPoint.getBaseSeq() == endPoint.getNextSeq()) {
                                    endPoint.stopTimer();
                                    endPoint.setRetry(0);
                                } else {
                                    endPoint.startTimer();
                                }
                            }
                            // Check if all endpoints are finished and empty, if so terminate the thread
                            boolean allFinishedAndEmpty = true;
                            for(myGUDPEndPoint endPoint2 : endPointsSend.values()) {
                                if(endPoint2.getTimeout()) {
                                    endPoint2.setFinished(true);
                                    endPoint2.removeAll();
                                }
                                if (!endPoint2.isFinished() || !endPoint2.isEmptyBuffer()) {
                                    allFinishedAndEmpty = false;
                                    break;
                                }
                            }
                            if(allFinishedAndEmpty) {
                                running = false;
                            }
                        }
                    }else{
                    // Receiver side processing BSN, DATA and FIN
                    synchronized (endPointsRecv) {
                        myGUDPEndPoint endPoint = endPointsRecv.get(sockAddr);

                        // Check packet type and process accordingly
                        switch (gudppacket.getType()) {

                            case GUDPPacket.TYPE_BSN: {
                                if (endPoint == null) {
                                    endPoint = new myGUDPEndPoint(sockAddr);
                                    endPointsRecv.put(sockAddr, endPoint);
                                }
                                // Extract seqnum from BSN packet and set +1 as expected packet
                                int bsnSeq = gudppacket.getSeqno();
                                endPoint.setExpectedSeq(bsnSeq + 1);

                                // Send ACK packet on the BSN with expectedSeq
                                GUDPPacket gudpACKpack = ackPacket(sockAddr, bsnSeq + 1);
                                DatagramPacket udpACKpack = gudpACKpack.pack();
                                datagramSocket.send(udpACKpack);

                                System.out.println("Received BSN: "  + gudppacket.getSeqno() +  " adding endpoint: " + endPoint.getRemoteEndPoint() + " Sending ACK seq: " + endPoint.getExpectedSeq());
                            }
                            break;

                            case GUDPPacket.TYPE_DATA: {
                                if (endPoint != null && endPoint.getExpectedSeq() == gudppacket.getSeqno()) {

                                    // Add packet to receiver buffer and extract seqnum from DATA packet and set +1 as expected packet
                                    endPoint.add(gudppacket);
                                    int dataSeq = gudppacket.getSeqno();
                                    endPoint.setExpectedSeq(dataSeq + 1);

                                    // Send ACK packet on the DATA with expectedSeq
                                    GUDPPacket gudpACKpack = ackPacket(sockAddr, dataSeq + 1);
                                    DatagramPacket udpACKpack = gudpACKpack.pack();
                                    datagramSocket.send(udpACKpack);

                                    System.out.println("Received DATA: " + gudppacket.getSeqno() + " Sending ACK seq: " + gudpACKpack.getSeqno());
                                }
                            }
                            break;

                            case GUDPPacket.TYPE_FIN: {
                                if (endPoint != null) {
                                    int finSeq = gudppacket.getSeqno();
                                    endPoint.setExpectedSeq(finSeq + 1);
                                    endPoint.setFinished(true);
                                    System.out.println("Received FIN:  "  + gudppacket.getSeqno() + " Sending ACK seq: " + endPoint.getExpectedSeq());

                                    // Send ACK packet on the FIN with expectedSeq
                                    GUDPPacket gudpACKpack = ackPacket(sockAddr, finSeq + 1);
                                    DatagramPacket udpACKpack = gudpACKpack.pack();
                                    datagramSocket.send(udpACKpack);
                                }
                            }
                            break;
                        }
                    }
                    }
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        });recvThread.start();

    }

    // Extract dst socket address from incoming App packet, create endPoint and add the packet to its buffer
    public void send(DatagramPacket packet) throws IOException {

        InetSocketAddress sockAddr = new InetSocketAddress(packet.getAddress(), packet.getPort());
        synchronized (endPointsSend){
            myGUDPEndPoint endPoint = endPointsSend.get(sockAddr);
            if(endPoint == null) {
                endPoint = new myGUDPEndPoint(sockAddr);
                GUDPPacket gudpBSNpack = bsnPacket(sockAddr);
                endPoint.baseSeq = gudpBSNpack.getSeqno();
                endPoint.nextSeq = gudpBSNpack.getSeqno();
                endPoint.lastSeq = gudpBSNpack.getSeqno();
                endPoint.add(gudpBSNpack);
                endPointsSend.put(sockAddr, endPoint);
                System.out.println("Sending BSN to: " + endPoint.getRemoteEndPoint() + " with seq: " + gudpBSNpack.getSeqno());
            }
            // Encapsulate packet into gudp packet, set its seqnum and add it to the endpoint buffer
            GUDPPacket gudpDATApack = GUDPPacket.encapsulate(packet);
            endPoint.setLastSeq(endPoint.getLastSeq() + 1);
            gudpDATApack.setSeqno(endPoint.getLastSeq());
            endPoint.add(gudpDATApack);

        }
    }

    // Incoming receiving requests from receiver application, remove the first packet in the buffer of each endPoint destination
    public void receive(DatagramPacket packet) {

        try {
            synchronized (endPointsRecv) {
                // If multiple sender endpoints in endPointsRecv, loop through all and retrieve first packet in buffer and receiver application will handle context
                for(myGUDPEndPoint endPoint : endPointsRecv.values()) {

                    if(endPoint.isFinished() && endPoint.isEmptyBuffer()) {
                        System.out.println("Transfer complete from: " + endPoint.getRemoteEndPoint());
                        endPointsRecv.remove(endPoint.getRemoteEndPoint());
                    } else if (!endPoint.isEmptyBuffer()) {
                        GUDPPacket gudpPacket = endPoint.remove();

                        // Extract application payload into a DatagramPacket, with data and socket address.
                        gudpPacket.decapsulate(packet);
                        break;
                    }
                }
            }
        }catch (IOException e) {
            System.out.println("Error receiving packet: " + e);
        }
    }

    // When called from sender application, prepare FIN packets and put in each endPoints outgoing buffer.
    public void finish() {
        synchronized (endPointsSend){
            for(myGUDPEndPoint endPoint : endPointsSend.values()) {
                // Create a FIN packet to send to each endPoint and indicate no more data
                GUDPPacket gudpFINpack = finPacket(endPoint.getRemoteEndPoint(), endPoint.getLastSeq() + 1);
                endPoint.setFinSeq(gudpFINpack.getSeqno());

                endPoint.add(gudpFINpack);
            }
        }
    }

    // Called from sender application to shut down socket, wait for all endpoints to finish and the two threads to be terminated then exit
    public void close() throws IOException {

        // Wait for sendThread and recvThread to finish
        try {
            sendThread.join();
            recvThread.join();
        } catch (InterruptedException e) {
            System.out.println("Error terminating threads: " + e);
        }

        synchronized (endPointsSend){
            // Remove all endPoints
            while(!endPointsSend.isEmpty()) {

                Iterator<myGUDPEndPoint> iterator = endPointsSend.values().iterator();

                while (iterator.hasNext()) {
                    myGUDPEndPoint endPoint = iterator.next();

                    if (endPoint.isFinished() && endPoint.isEmptyBuffer()) {
                        iterator.remove();
                        System.out.println("Transfer complete to: " + endPoint.getRemoteEndPoint());
                    }
                }
            }
            System.out.println("Transfer complete to all endpoints, closing GUDPSocket");
            datagramSocket.close();
            System.exit(0);
        }
    }



    // Create a BSN packet with randomized seqnum -100000 to handle if bsn is set to maxInt value
    private GUDPPacket bsnPacket(InetSocketAddress sockAddr) {
        ByteBuffer buffer = ByteBuffer.allocate(GUDPPacket.HEADER_SIZE);
        buffer.order(ByteOrder.BIG_ENDIAN);
        GUDPPacket gudpBSNpacket = new GUDPPacket(buffer);
        gudpBSNpacket.setType(GUDPPacket.TYPE_BSN);
        gudpBSNpacket.setVersion(GUDPPacket.GUDP_VERSION);
        gudpBSNpacket.setSocketAddress(sockAddr);
        gudpBSNpacket.setPayloadLength(0);

        // Initial seq number randomized
        Random rnd = new Random();
        int bsn = Math.abs(rnd.nextInt() - 100000);
        gudpBSNpacket.setSeqno(bsn);

        return gudpBSNpacket;
    }

    // Create ACK packet with given seqnum
    private GUDPPacket ackPacket(InetSocketAddress sockAddr, int seq) {
        ByteBuffer buffer = ByteBuffer.allocate(GUDPPacket.HEADER_SIZE);
        buffer.order(ByteOrder.BIG_ENDIAN);
        GUDPPacket gudpACKpacket = new GUDPPacket(buffer);
        gudpACKpacket.setType(GUDPPacket.TYPE_ACK);
        gudpACKpacket.setVersion(GUDPPacket.GUDP_VERSION);
        gudpACKpacket.setSocketAddress(sockAddr);
        gudpACKpacket.setPayloadLength(0);
        gudpACKpacket.setSeqno(seq);

        return gudpACKpacket;
    }

    // Create a FIN packet to send to each endPoint and indicate no more data
    private GUDPPacket finPacket(InetSocketAddress sockAddr,  int seq) {
        ByteBuffer buffer = ByteBuffer.allocate(GUDPPacket.HEADER_SIZE);
        buffer.order(ByteOrder.BIG_ENDIAN);
        GUDPPacket gudpFINpacket = new GUDPPacket(buffer);
        gudpFINpacket.setType(GUDPPacket.TYPE_FIN);
        gudpFINpacket.setVersion(GUDPPacket.GUDP_VERSION);
        gudpFINpacket.setSocketAddress(sockAddr);
        gudpFINpacket.setPayloadLength(0);
        gudpFINpacket.setSeqno(seq);

        return gudpFINpacket;
    }


    // Custom implementation of GUDPEndPoint to handle each unique endpoint
    private class myGUDPEndPoint {
        public static final int MAX_WINDOW_SIZE = 3;
        public static final int MAX_RETRY = 7;
        public static final long TIMEOUT_DURATION = 3000L; // (3 seconds)

        private InetSocketAddress remoteEndPoint;
        //private LinkedList<GUDPPacket> bufferList = new LinkedList<>();
        // Buffers to handle incoming/outgoing app datagrams
        private LinkedList<GUDPPacket> bufferList = new LinkedList<>();
        //private LinkedList<GUDPPacket> recvBuffer = new LinkedList<>();

        private int windowSize;
        private int maxRetry;
        private long timeoutDuration;
        private int retry;

        private int baseSeq;            // seq of sent packet not yet acked (i.e., base)
        private int nextSeq;      // seq of next packet to send (i.e., nextseqnum)
        private int lastSeq;            // seq of last packet in bufferList
        private int expectedSeq;  // seq of next packet to receive
        private int finSeq;

        private boolean finished;
        private boolean timeout;


        private myGUDPEndPoint(InetSocketAddress socketaddr) {
            this.remoteEndPoint = socketaddr;
            this.windowSize = MAX_WINDOW_SIZE;
            this.maxRetry = MAX_RETRY;
            this.timeoutDuration = TIMEOUT_DURATION;
            this.baseSeq = -1;
            this.nextSeq = -1;
            this.lastSeq = -1;
            this.expectedSeq = -1;
            this.finSeq = -1;
            this.retry = 0;
            this.finished = false;
            this.timeout = false;
        }


        private InetSocketAddress getRemoteEndPoint() {
            return this.remoteEndPoint;
        }

        public int getBaseSeq() {
            return this.baseSeq;
        }

        public void setBaseSeq(int seq) {
            this.baseSeq = seq;
        }

        public int getNextSeq() {
            return this.nextSeq;
        }

        public void setNextSeq(int seq) {
            this.nextSeq = seq;
        }


        public int getLastSeq() {
            return this.lastSeq;
        }

        public void setLastSeq(int seq) {
            this.lastSeq = seq;
        }

        public int getFinSeq() {
            return this.finSeq;
        }

        public void setFinSeq(int seq) {
            this.finSeq = seq;
        }

        public int getExpectedSeq() {
            return this.expectedSeq;
        }

        public void setExpectedSeq(int seq) {
            this.expectedSeq = seq;
        }

        public int getRetry() {
            return this.retry;
        }

        public void setRetry(int num) {
            this.retry = num;
        }

        public boolean isFinished() {
            return this.finished;
        }

        public void setFinished(boolean fin) {
            this.finished = fin;
        }


        public void setWindowSize(int size) {
            if (this.windowSize > 0)
                this.windowSize = size;
            else{
                System.out.println("Window size cannot be negative");
            }
        }

        public void add(GUDPPacket gpacket) {
            synchronized (bufferList) {
                bufferList.add(gpacket);
            }
        }

        public void addBSN(GUDPPacket gpacket) {
            synchronized (bufferList) {
                bufferList.addFirst(gpacket);
            }
        }

        public void remove(GUDPPacket gpacket) {
            synchronized (bufferList) {
                bufferList.remove(gpacket);
            }
        }

        /*
         * Retrieve and remove the first packet from the bufferList
         */
        public GUDPPacket remove() {
            synchronized (bufferList) {
                return bufferList.remove();
            }
        }

        /*
         * Get the packet with the given sequence number from bufferList
         * IMPORTANT: the packet is still in the bufferList!
         */
        public GUDPPacket getPacket(int seq) {
            synchronized (bufferList) {
                for (int i = 0; i < bufferList.size(); i++) {
                    if (bufferList.get(i).getSeqno() == seq) {
                        return bufferList.get(i);
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
            synchronized (bufferList) {
                while (bufferList.size() > 0) {
                    if (bufferList.getFirst().getSeqno() <= ack) {
                        bufferList.removeFirst();
                    } else {
                        break;
                    }
                }
                //notify();
            }
        }


        public boolean isEmptyBuffer() {
            if (bufferList.size() == 0)
                return true;
            else
                return false;
        }

        /*
         * Remove all packets from bufferList
         */
        public void removeAll() {
            synchronized (bufferList) {
                while (bufferList.size() > 0) {
                    bufferList.removeFirst();
                }
            }
        }

        public boolean getTimeout() {
            return this.timeout;
        }

        public void setTimeout(boolean timeout) {
            this.timeout = timeout;
        }

        // When a timer expires it resets nextSeq to the baseSeq and which will allow for retransmission of all packets in window
        Timer timer;
        public void startTimer() {
            timer = new Timer("Timer");
            TimerTask task = new TimerTask() {
                public void run() {
                    System.out.print("TIMEOUT: " + remoteEndPoint.getAddress() + ":" + remoteEndPoint.getPort() + "  ");
                    System.out.println("Retransmitting packets from seq: " + baseSeq);
                    nextSeq = baseSeq;

                    if(getRetry() == maxRetry) {
                        System.out.println("Max retry reached, closing GUDPSocket endPoint");
                        setTimeout(true);
                       // datagramSocket.close();
                       // System.exit(0);
                    }
                    else {
                        setRetry(getRetry() + 1);
                        timer.cancel();
                    }
                }
            };
            timer.schedule(task, timeoutDuration);
        }

        public void stopTimer() {
            timer.cancel();
        }

    }
    /*
    public static void main(String[] args) {
        try {
            // Create a DatagramSocket for sending
            DatagramSocket sendSocket = new DatagramSocket();
            GUDPSocket gudpSendSocket = new GUDPSocket(sendSocket);

            String msg = "testcase";
            byte[] buf = msg.getBytes();
            InetAddress address = InetAddress.getByName("127.0.0.1");
            int port = 5000;
            DatagramPacket sendPacket = new DatagramPacket(buf, buf.length, address, port);

            // Send the packet
            gudpSendSocket.send(sendPacket);

            // Create a DatagramSocket for receiving
            DatagramSocket receiveSocket = new DatagramSocket(port);
            GUDPSocket gudpReceiveSocket = new GUDPSocket(receiveSocket);

            byte[] recBuf = new byte[256];
            DatagramPacket receivePacket = new DatagramPacket(recBuf, recBuf.length);
            gudpReceiveSocket.receive(receivePacket);

           // String received = new String(receivePacket.getData(), 0, receivePacket.getLength());
          //  System.out.println("Received: " + received);

        } catch (IOException e) {
            e.printStackTrace();
        }
    }*/
}

