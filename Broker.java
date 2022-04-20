import java.io.*;
import java.net.*;
import java.util.ArrayList;
import java.util.HashMap;

public class Broker {
    private static HashMap<Integer, Integer> brokers = new HashMap<Integer, Integer>();
    private static int[][] topics;

    private ServerSocket providerSocket;
    private Socket connection = null;

    public static void main(String args[]) {
        ReadFromFile fileReader = new ReadFromFile();
        ArrayList<Integer> availablePorts = fileReader.getPorts();
        ArrayList<String> availableTopics = fileReader.getTopics();
        // TODO set the number of brokers
        int brokersNum = 3;
        brokers = matchBrokerToPort(availablePorts, brokersNum);
        topics = matchTopicToBroker(availableTopics, brokersNum);

        new Broker().acceptConnection();
    }

    // The broker will wait on the given port for a user to connect
    private void acceptConnection() {
        // TODO set port manually
        int port = 1200;
        try {
            providerSocket = new ServerSocket(port);

            while (true) {
                System.out.println("Waiting for connection on port " + port);
                connection = providerSocket.accept();
                System.out.println("Connected on port: " + port);
                System.out.println("Connected user: " + connection.getInetAddress().getHostName());
                Thread t = new ActionsForUsers(connection, brokers, topics);
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

    // Match each broker to a port
    private static HashMap<Integer, Integer> matchBrokerToPort(ArrayList<Integer> availablePorts, int brokersNum) {
        int i = 1;
        HashMap<Integer, Integer> brokerPorts = new HashMap<Integer, Integer>();
        for (int port: availablePorts) {
            brokerPorts.put(i, port);
            i++;
        }
        return brokerPorts;
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
}
