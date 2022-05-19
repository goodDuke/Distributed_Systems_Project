import java.io.*;
import java.util.HashMap;
import java.util.Queue;

public class ActionsForPublishers extends Thread implements Serializable{
    private ObjectInputStream inPublisher;
    private ObjectOutputStream outPublisher;
    private HashMap<Integer, Broker> brokers;
    private int[][] topics;
    // Create a queue for each topic. Find the queue
    // in the HashMap by the topic code
    private HashMap<Integer, Queue<byte[]>> queues;


    private String ip;
    private int port;

    public ActionsForPublishers(HashMap<Integer, Broker> brokers,
                                int[][] topics, String ip, int port,
                                ObjectOutputStream out, ObjectInputStream in,
                                HashMap<Integer, Queue<byte[]>> queues) {
        this.brokers = brokers;
        this.topics = topics;
        this.ip = ip;
        this.port = port;
        this.outPublisher = out;
        this.inPublisher = in;
        this.queues = queues;
    }

    public void run() {
        try {
            receiveData();
        } catch (IOException | ClassNotFoundException e) {
            e.printStackTrace();
        }
    }

    // Collecting the chunks for a specific file and adding them to the correct queue
    private synchronized void receiveData() throws IOException, ClassNotFoundException {
        int topicCode = inPublisher.readInt(); // 2P
        byte[] fileName = (byte[]) inPublisher.readObject(); // 3P
        queues.get(topicCode).add(fileName);
        byte[] blockCountChunk = (byte[]) inPublisher.readObject(); // 4P
        queues.get(topicCode).add(blockCountChunk);
        byte[] publisherId = (byte[]) inPublisher.readObject(); // 5P
        queues.get(topicCode).add(publisherId);

        // Converting blockCount to integer
        int blockCount = 0;
        for (byte b: blockCountChunk)
            blockCount += b;

        // Saving chunks in the corresponding queue
        for (int i = 1; i <= blockCount; i++) {
            byte[] chunk = (byte[]) inPublisher.readObject(); // 6P
            queues.get(topicCode).add(chunk);
        }
    }
}
