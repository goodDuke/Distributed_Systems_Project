import java.io.*;
import java.net.*;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.Objects;
import java.util.Queue;

public class ActionsForPublishers extends Thread {
    ObjectInputStream in;
    ObjectOutputStream out;
    private HashMap<Integer, Broker> brokers;
    private int[][] topics;
    private HashMap<Integer, Queue<byte[]>> queues = new HashMap<>();

    public ActionsForPublishers(Socket connection, HashMap<Integer, Broker> brokers, int[][] topics, String ip, int port) {
        try {
            this.brokers = brokers;
            this.topics = topics;
            initializeQueues(ip, port);
            out = new ObjectOutputStream(connection.getOutputStream());
            in = new ObjectInputStream(connection.getInputStream());
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public void run() {
        try {
            // Check which broker contains the requested topic only if the
            // current broker is the first one the publisher connected to
            boolean firstConnection = in.readBoolean();
            if (firstConnection)
                getBroker();

            receiveData();

        } catch (IOException | ClassNotFoundException e) {
            e.printStackTrace();
        } finally {
            try {
                in.close();
                out.close();
            } catch (IOException ioException) {
                ioException.printStackTrace();
            }
        }
    }

    private void initializeQueues(String ip, int port) {
        int currentBroker = -1;
        for (int b: brokers.keySet()) {
            if (Objects.equals(brokers.get(b).getIp(), ip) && brokers.get(b).getPort() == port) {
                currentBroker = b;
            }
        }

        for (int topicCode: topics[currentBroker]) {
            Queue<byte[]> queue = new LinkedList<>();
            if (topicCode != 0)
                queues.put(topicCode, queue);
        }
    }

    private void receiveData() throws IOException, ClassNotFoundException {
        int topicCode = in.readInt();
        int blockCount = in.readInt();
        for (int i = 1; i <= blockCount; i++){
            byte[] chunk = (byte[]) in.readObject();
            queues.get(topicCode).add(chunk);
        }
    }

    // Find the broker that contains the requested topic
    private void getBroker() {
        int matchedBroker = 0;
        try {
            int requestedTopic = in.readInt();
            for (int i = 0; i < brokers.size(); i++) {
                for (int topic : topics[i]) {
                    if (requestedTopic == topic) {
                        matchedBroker = i + 1;
                        break;
                    }
                }
                if (matchedBroker != 0) {
                    out.writeObject(brokers.get(matchedBroker));
                    out.flush();
                    break;
                }
            }
            if (matchedBroker == 0) {
                out.writeObject(null);
                out.flush();
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}

// TODO first chunk must have info for the amount of chunks to follow
// TODO broker must see the length and create an appropriate array
