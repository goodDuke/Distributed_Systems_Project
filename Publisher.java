import java.util.Scanner;
import java.io.*;
import java.net.*;

public class Publisher extends Thread {
    Broker b;
    int topic = -534107947;

    public static void main(String args[]) {
        Broker b1 = new Broker("127.0.0.1", 1100);
        Broker b2 = new Broker("127.0.0.1", 1200);
        new Publisher(b1).start();
        new Publisher(b2).start();
        //topic = getTopic();
    }

    public void run() {
        Socket requestSocket = null;
        ObjectOutputStream out = null;
        ObjectInputStream in = null;
        try {
            requestSocket = new Socket(b.getIp(), b.getPort());
            System.out.println("Connected to host: " + b.getIp() + " on port: " + b.getPort());
            out = new ObjectOutputStream(requestSocket.getOutputStream());
            in = new ObjectInputStream(requestSocket.getInputStream());

            out.writeInt(topic);
            out.flush();

            Broker matchedBroker = (Broker) in.readObject();
            System.out.println(matchedBroker.getIp() + " " + matchedBroker.getPort());
        } catch (UnknownHostException unknownHost) {
            System.err.println("You are trying to connect to an unknown host!");
        } catch (IOException ioException) {
            System.out.println("An error occurred while trying to connect to host: " + b.getIp() + " on port: " +
                    b.getPort() + ". Check the IP address and the port.");
            ioException.printStackTrace();
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        } finally {
            try {
                in.close();
                out.close();
                requestSocket.close();
                System.out.println("Connection closed");
            } catch (IOException ioException) {
                ioException.printStackTrace();
            }
        }
    }

    private static int getTopic() {
        Scanner s = new Scanner(System.in);
        System.out.print("Enter the topic: ");
        String topic = s.nextLine();
        System.out.println(topic);

        int code = topic.hashCode();
        return code;
    }

    Publisher(Broker b) {
        this.b = b;
    }
}
