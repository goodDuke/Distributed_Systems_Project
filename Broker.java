import java.io.*;
import java.net.*;

public class Broker {
    public static void main(String args[]) {
        new Broker().openBroker();
    }

    ServerSocket providerSocket;
    Socket connection = null;

    void openBroker() {
        ReadFromFile fileReader = new ReadFromFile();
        int port = fileReader.getPort();
        try {
            if (port == -1) {
                System.exit(-1);
            }

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
