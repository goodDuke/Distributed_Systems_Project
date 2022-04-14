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
        // TODO set port manually
        int port = 1100;
        try {
            providerSocket = new ServerSocket(port);

            while (true) {
                System.out.println("Waiting for connection on port " + port);
                connection = providerSocket.accept();
                System.out.println("Connected on port " + port);
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
