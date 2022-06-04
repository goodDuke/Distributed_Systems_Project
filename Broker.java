import java.io.*;
import java.net.*;
import java.util.*;

public class Broker extends Thread implements Serializable {
    private String ip;
    private int port;
    private ServerSocket providerSocket;
    private Socket connectionUser = null;
    private Socket connectionPublisher = null;
    private Socket connectionConsumer = null;
    private ArrayList<String[]> topicsAndUsers;
    private ObjectOutputStream outUser;
    private ObjectInputStream inUser;
    private ObjectInputStream inPublisher;
    private ObjectOutputStream outPublisher;
    private ObjectInputStream inConsumer;
    private ObjectOutputStream outConsumer;
    private HashMap<Integer, Queue<byte[]>> queues = new HashMap<>();
    private static HashMap<Integer, Broker> brokers = new HashMap<>();
    private static int[][] topics;
    private int currentBroker;
    private static Broker b;


    public static void main(String[] args) {
        // TODO set port and IP manually
        int port = 1300;
        String ip = "192.168.68.108";
        b = new Broker(ip, port);
        b.acceptConnection();
    }

    // The broker will wait on the given port for a user to connect
    private void acceptConnection() {
        try {
            // Read ports, IPs, topics from txt files
            ReadFromFile fileReader = new ReadFromFile();
            ArrayList<Integer> availablePorts = fileReader.getPorts();
            ArrayList<String> availableIps = fileReader.getIps();
            ArrayList<String> availableTopics = fileReader.getTopics();
            topicsAndUsers = fileReader.getTopicsAndUsers();

            // TODO set the number of brokers. It must match the number of ports and IPs in the txts.
            int brokersNum = 3;

            brokers = matchBrokerToAddress(availableIps, availablePorts, brokersNum);
            topics = matchTopicToBroker(availableTopics, brokersNum);
            initializeQueues(ip, port);
            providerSocket = new ServerSocket(port);

            while (true) {
                // Open connection on port and connect to publisher
                System.out.println("Waiting for connection on port " + port);
                connectionUser = providerSocket.accept();
                connectionPublisher = providerSocket.accept();
                connectionConsumer = providerSocket.accept();
                outUser = new ObjectOutputStream(connectionUser.getOutputStream());
                inUser = new ObjectInputStream(connectionUser.getInputStream());
                outPublisher = new ObjectOutputStream(connectionPublisher.getOutputStream());
                inPublisher = new ObjectInputStream(connectionPublisher.getInputStream());
                outConsumer = new ObjectOutputStream(connectionConsumer.getOutputStream());
                inConsumer = new ObjectInputStream(connectionConsumer.getInputStream());
                System.out.println("Connected on port: " + port);
                System.out.println("Connected user: " + connectionPublisher.getInetAddress().getHostName());
                new BrokerActions(inUser, outUser, inPublisher, outPublisher, inConsumer, outConsumer,
                        topicsAndUsers, brokers, topics, b, queues, currentBroker).start();
            }
        } catch (IOException ioException) {
            ioException.printStackTrace();
        } finally {
            try {
                inUser.close();
                outUser.close();
                inPublisher.close();
                outPublisher.close();
                outConsumer.close();
                inConsumer.close();
                providerSocket.close();
            } catch (IOException ioException) {
                ioException.printStackTrace();
            }
        }
    }

    // Match each broker to address
    private HashMap<Integer, Broker> matchBrokerToAddress(ArrayList<String> availableIps, ArrayList<Integer> availablePorts, int brokersNum) {
        HashMap<Integer, Broker> brokerAddresses = new HashMap<>();
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
        return registeredTopics;
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