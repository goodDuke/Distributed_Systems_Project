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
    // Create a queue for each topic. Find the queue in the HashMap by the topic code
    private HashMap<Integer, Queue<byte[]>> queues = new HashMap<>();

    public ActionsForPublishers(Socket connection, HashMap<Integer, Broker> brokers,
                                int[][] topics, String ip, int port) {
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
            boolean changedBroker = false;

            if (firstConnection) {
                getBroker();
                changedBroker = in.readBoolean();
            }

            if (!changedBroker)
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

    // Initialize Broker's queues. For each topic there is one queue
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

    // Collecting the chunks for a specific file and adding them to the correct queue
    private void receiveData() throws IOException, ClassNotFoundException {
        int topicCode = in.readInt();
        int blockCount = in.readInt();
        for (int i = 1; i <= blockCount; i++){
            byte[] chunk = (byte[]) in.readObject();
            queues.get(topicCode).add(chunk);
        }
        recreateFile(topicCode, blockCount);
    }

    private void recreateFile(int topicCode, int blockCount) throws IOException {
        String filepath = "./recreated_files";
        File file = new File(filepath);

        OutputStream stream = new FileOutputStream(file);
        byte[] completeFile = new byte[512 * 1024 * blockCount];
        int i = 0;
        for (byte[] chunk: queues.get(topicCode)) {
            for (byte b: chunk)
                completeFile[i] = b;
        }
        stream.write(completeFile);
        stream.close();
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
