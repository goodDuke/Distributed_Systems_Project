import java.io.*;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.Objects;
import java.util.Queue;

public class ActionsForPublishers extends Thread {
    private ObjectInputStream in;
    private ObjectOutputStream out;
    private HashMap<Integer, Broker> brokers;
    private int[][] topics;
    private HashMap<Integer, Queue<byte[]>> queues;
    // Create a queue for each topic. Find the queue in the HashMap by the topic code

    private String ip;
    private int port;

    public ActionsForPublishers(HashMap<Integer, Broker> brokers,
                                int[][] topics, String ip, int port,
                                ObjectOutputStream out, ObjectInputStream in, HashMap<Integer, Queue<byte[]>> queues) {
        this.brokers = brokers;
        this.topics = topics;
        this.ip = ip;
        this.port = port;
        this.out = out;
        this.in = in;
        this.queues = queues;
    }

    public void run() {
        try {
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
    }
}
