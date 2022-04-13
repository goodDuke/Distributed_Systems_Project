import java.io.*;
import java.net.*;
import java.util.ArrayList;

public class Broker {
    public static void main(String args[]) {
        ReadFromFile fileReader = new ReadFromFile();
        ArrayList<Integer> availablePorts = fileReader.getPorts();

        new Broker().openBroker();
    }

    ServerSocket providerSocket;
    Socket connection = null;

    // The broker will wait on the given port for a user to connect
    void openBroker() {
        // Set port manually
        int port = 1100;
        try {
            providerSocket = new ServerSocket(port);
            System.out.println("Waiting for connection on port " + port);

            while (true) {
                connection = providerSocket.accept();
                Thread t = new ActionsForClients(connection);
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
}
