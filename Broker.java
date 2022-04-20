import java.io.*;
import java.net.*;
import java.util.ArrayList;
import java.util.HashMap;

public class Broker {
    ServerSocket providerSocket;
    Socket connection = null;

    public static void main(String args[]) {
        ReadFromFile fileReader = new ReadFromFile();
        ArrayList<Integer> availablePorts = fileReader.getPorts();
        ArrayList<String> availableTopics = fileReader.getTopics();
        // TODO set the number of brokers
        int brokersNum = 3;
        matchTopicToBroker(availableTopics, brokersNum);

        new Broker().acceptConnection();
    }

    // The broker will wait on the given port for a user to connect
    void acceptConnection() {
        // TODO set port manually
        int port = 1100;
        try {
            providerSocket = new ServerSocket(port);

            while (true) {
                System.out.println("Waiting for connection on port " + port);
                connection = providerSocket.accept();
                System.out.println("Connected on port: " + port);
                System.out.println("Connected user: " + connection.getInetAddress().getHostName());
                Thread t = new ActionsForUsers(connection);
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

    static HashMap<> matchPortToBroker(ArrayList<Integer> availablePorts, int brokersNum) {

    }

    // Match each topic to a broker
    static int[][] matchTopicToBroker(ArrayList<String> availableTopics, int brokersNum) {
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
