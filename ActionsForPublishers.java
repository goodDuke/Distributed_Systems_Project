import java.io.*;
import java.net.*;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.Objects;
import java.util.Queue;

public class ActionsForPublishers extends Thread {
    private ObjectInputStream in;
    private ObjectOutputStream out;
    private HashMap<Integer, Broker> brokers;
    private int[][] topics;
    // Create a queue for each topic. Find the queue in the HashMap by the topic code
    private HashMap<Integer, Queue<byte[]>> queues = new HashMap<>();
    private String ip;
    private int port;

    public ActionsForPublishers(Socket connection, HashMap<Integer, Broker> brokers,
                                int[][] topics, String ip, int port,
                                ObjectOutputStream out, ObjectInputStream in) {
        this.brokers = brokers;
        this.topics = topics;
        this.ip = ip;
        this.port = port;
        this.out = out;
        this.in = in;
    }

    public void run() {
        try {
            initializeQueues(ip, port);
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
            if (topicCode != 0) {
                queues.put(topicCode, queue);
            }
        }
    }

    // Collecting the chunks for a specific file and adding them to the correct queue
    private void receiveData() throws IOException, ClassNotFoundException {
        int topicCode = in.readInt(); // 5
        byte[] extension = (byte[]) in.readObject(); // 6
        queues.get(topicCode).add(extension);
        byte[] blockCount = (byte[]) in.readObject(); // 7
        queues.get(topicCode).add(blockCount);

        // Converting blockCount to integer
        int chunkCount = 0;
        for (byte b: blockCount)
            chunkCount += b;

        // Saving chunks in the corresponding queue
        for (int i = 1; i <= chunkCount; i++) {
            byte[] chunk = (byte[]) in.readObject(); // 8
            queues.get(topicCode).add(chunk);
        }
        recreateFile(topicCode, chunkCount);
    }

    private void recreateFile(int topicCode, int chunkCount) throws IOException {
        String filepath = ".\\src\\recreated_files\\mnm.txt";
        File file = new File(filepath);
        OutputStream stream = new FileOutputStream(file);
        byte[] completeFile = new byte[512 * 1024 * chunkCount];
        int i = 0;
        // Skip the first to chunks which contain the total chunkCount of the file and its
        int skipChunks = 2;
        for (byte[] chunk: queues.get(topicCode)) {
            if (skipChunks == 0) {
                for (byte b : chunk) {
                    completeFile[i] = b;
                    i++;
                }
            } else
                skipChunks--;
        }
        stream.write(completeFile);
        stream.close();
    }
}
