/*
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.HashMap;
import java.util.Queue;

public class ActionsForConsumers extends Thread {
    private ObjectInputStream in;
    private ObjectOutputStream out;
    private HashMap<Integer, Broker> brokers;
    private int[][] topics;
    // Create a queue for each topic. Find the queue in the HashMap by the topic code
    private HashMap<Integer, Queue<byte[]>> queues = new HashMap<>();
    private String ip;
    private int port;

    public ActionsForConsumers(HashMap<Integer, Broker> brokers,
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

        }
    }
}
*/
