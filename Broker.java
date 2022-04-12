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
                try {
                    System.out.println("No ports are available right now.");
                    providerSocket.close();
                    System.exit(-1);
                } catch (IOException ioException) {
                    ioException.printStackTrace();
                }
            }

            providerSocket = new ServerSocket(port);

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
                fileReader.releasePort(port);
            } catch (IOException ioException) {
                ioException.printStackTrace();
            }
        }
    }
}
