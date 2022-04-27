import java.io.*;
import java.net.*;
import java.util.*;

public class Broker implements Serializable {
    private int requestedTopic;
    private ObjectInputStream in;
    private ObjectOutputStream out;
    private static HashMap<Integer, Broker> brokers = new HashMap<Integer, Broker>();
    private static int[][] topics;
    private String ip;
    private int port;
    private ServerSocket providerSocket;
    private Socket connection = null;
    private boolean publisherMode;
    private Thread t;
    private int currentBroker;
    private HashMap<Integer, Queue<byte[]>> queues = new HashMap<>();

    public static void main(String[] args) {
        // TODO set port and IP manually
        int port = 1100;
        String ip = "127.0.0.1";
        new Broker(ip, port).acceptConnection();
    }

    // The broker will wait on the given port for a user to connect
    private void acceptConnection() {
        try {
            // Read ports, IPs, topics from txt files
            ReadFromFile fileReader = new ReadFromFile();
            ArrayList<Integer> availablePorts = fileReader.getPorts();
            ArrayList<String> availableIps = fileReader.getIps();
            ArrayList<String> availableTopics = fileReader.getTopics();

            // TODO set the number of brokers. It must match the number of ports and IPs in the txts.
            int brokersNum = 3;

            brokers = matchBrokerToAddress(availableIps, availablePorts, brokersNum);
            topics = matchTopicToBroker(availableTopics, brokersNum);
            initializeQueues(ip, port);
            providerSocket = new ServerSocket(port);

            while (true) {
                // Open connection on port and connect to publisher
                System.out.println("Waiting for connection on port " + port);
                connection = providerSocket.accept();
                out = new ObjectOutputStream(connection.getOutputStream());
                in = new ObjectInputStream(connection.getInputStream());
                System.out.println("Connected on port: " + port);
                System.out.println("Connected user: " + connection.getInetAddress().getHostName());
                // Check which broker contains the requested topic only if the
                // current broker is the first one the publisher connected to
                int matchedBroker;
                while (true) {
                    boolean firstConnection = in.readBoolean(); // 1
                    if (firstConnection) {
                        matchedBroker = getBroker();
                        if (matchedBroker != -1 || requestedTopic == 81)
                            break;
                    } else if (requestedTopic != 81) {
                        matchedBroker = currentBroker;
                        break;
                    }
                }
                if (requestedTopic != 81 && currentBroker == matchedBroker) {
                    publisherMode = in.readBoolean(); // 4
                    if (publisherMode) {
                        t = new ActionsForPublishers(brokers, topics, getIp(), getPort(), out, in, queues);
                        t.start();
                        t.join();
                    }
                }
            }
        } catch (IOException | InterruptedException ioException) {
            ioException.printStackTrace();
        } finally {
            try {
                providerSocket.close();
            } catch (IOException ioException) {
                ioException.printStackTrace();
            }
        }
    }

    /*private void pull(int topicCode) {
        for (byte[] chunk: queues.get(topicCode) {
            out.writeObject(chunk); // 8
            out.flush();
        }
    }*/

    // Match each broker to address
    private HashMap<Integer, Broker> matchBrokerToAddress(ArrayList<String> availableIps, ArrayList<Integer> availablePorts, int brokersNum) {
        HashMap<Integer, Broker> brokerAddresses = new HashMap<Integer, Broker>();
        for (int i = 0; i < availablePorts.size(); i++) {
            brokerAddresses.put(i, new Broker(availableIps.get(i), availablePorts.get(i)));
            if (availableIps.get(i).equals(ip) && availablePorts.get(i) == port)
                currentBroker = i;
        }
        return brokerAddresses;
    }

    // Match each topic to a broker
    private static int[][] matchTopicToBroker(ArrayList<String> availableTopics, int brokersNum) {
        int length = availableTopics.size();
        int[][] registeredTopics = new int[brokersNum][length];
        for (String topic: availableTopics) {
            int code = topic.hashCode();
            int broker = Math.abs(code) % brokersNum;
            for (int i = 0; i < length; i++) {
                if (registeredTopics[broker][i] == 0) {
                    registeredTopics[broker][i] = code;
                    break;
                }
            }
        }
        /*for (int i = 0; i < brokersNum; i++) {
            for (int t: registeredTopics[i]) {
                if (t != 0)
                    System.out.println(i + " " + t);
            }
        }*/
        return registeredTopics;
    }

    // Find the broker that contains the requested topic
    private int getBroker() {
        int matchedBroker = -1;
        try {
            requestedTopic = in.readInt(); // 2
            for (int i = 0; i < brokers.size(); i++) {
                for (int topic : topics[i]) {
                    if (requestedTopic == topic) {
                        matchedBroker = i;
                        break;
                    }
                }
                if (matchedBroker != -1) {
                    out.writeObject(brokers.get(matchedBroker)); // 3
                    out.flush();
                    break;
                }
            }
            if (matchedBroker == -1) {
                out.writeObject(null); // 3
                out.flush();
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        return matchedBroker;
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

    public Broker(String ip, int port) {
        this.ip = ip;
        this.port = port;
    }

    public int getPort() {return port;}

    public String getIp() {return ip;}
}
