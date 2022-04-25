import java.io.*;
import java.net.*;
import java.util.ArrayList;
import java.util.HashMap;

public class Broker implements Serializable {
    private static HashMap<Integer, Broker> brokers = new HashMap<Integer, Broker>();
    private static int[][] topics;
    private String ip;
    private int port;
    private ServerSocket providerSocket;
    private Socket connection = null;

    public static void main(String args[]) {

        // Read ports, Ips, topics from txt files
        ReadFromFile fileReader = new ReadFromFile();
        ArrayList<Integer> availablePorts = fileReader.getPorts();
        ArrayList<String> availableIps = fileReader.getIps();
        ArrayList<String> availableTopics = fileReader.getTopics();
        // TODO set the number of brokers. It must match the number of ports and IPs in the txts.
        int brokersNum = 3;
        brokers = matchBrokerToAddress(availableIps, availablePorts, brokersNum);
        topics = matchTopicToBroker(availableTopics, brokersNum);

        // TODO set port and IP manually
        int port = 1200;
        String ip = "127.0.0.1";
        new Broker(ip, port).acceptConnection();
    }

    // The broker will wait on the given port for a user to connect
    private void acceptConnection() {
        try {
            providerSocket = new ServerSocket(port);

            while (true) {
                // Open connection on port and connect to publisher
                System.out.println("Waiting for connection on port " + port);
                connection = providerSocket.accept();
                System.out.println("Connected on port: " + port);
                System.out.println("Connected user: " + connection.getInetAddress().getHostName());
                Thread t = new ActionsForPublishers(connection, brokers, topics, getIp(), getPort());
                t.start();
            }
        } catch (IOException ioException) {
            ioException.printStackTrace();
        } finally {
            try {
                providerSocket.close();
            } catch (IOException ioException) {
                ioException.printStackTrace();
            }
        }
    }

    // Match each broker to address
    private static HashMap<Integer, Broker> matchBrokerToAddress(ArrayList<String> availableIps, ArrayList<Integer> availablePorts, int brokersNum) {
        int i = 0;
        HashMap<Integer, Broker> brokerAddresses = new HashMap<Integer, Broker>();
        for (int j = 0; j < availablePorts.size(); j++) {
            brokerAddresses.put(i, new Broker(availableIps.get(j), availablePorts.get(j)));
            i++;
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
        for (int i = 0; i < brokersNum; i++) {
            for (int t: registeredTopics[i]) {
                if (t != 0)
                    System.out.println(i + " " + t);
            }
        }
        return registeredTopics;
    }

    public Broker(String ip, int port) {
        this.ip = ip;
        this.port = port;
    }

    public int getPort() {return port;}

    public String getIp() {return ip;}
}
