import java.util.Objects;
import java.util.Scanner;
import java.io.*;
import java.net.*;

public class Publisher extends Thread {
    private Broker b;
    private Socket requestSocket;
    private int topicCode;
    private String topicString;

    public static void main(String args[]) {
        Broker b1 = new Broker("127.0.0.1", 1100);
        Broker b2 = new Broker("127.0.0.1", 1200);
        new Publisher(b1).start();
        //new Publisher(b2).start();
    }

    public void run() {
        ObjectOutputStream out = null;
        ObjectInputStream in = null;
        try {
            requestSocket = new Socket(b.getIp(), b.getPort());
            System.out.println("Connected to broker: " + b.getIp() + " on port: " + b.getPort());
            topicCode = getTopic();
            out = new ObjectOutputStream(requestSocket.getOutputStream());
            in = new ObjectInputStream(requestSocket.getInputStream());

            out.writeInt(topicCode);
            out.flush();

            Broker matchedBroker = (Broker) in.readObject();
            if (matchedBroker == null)
                System.out.println("The topic \"" + topicString + "\" doesn't exist.");
            else
                connectToMatchedBroker(matchedBroker);

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
                System.out.println("Connection to broker: " + b.getIp() + " on port: " + b.getPort() + " closed");
            } catch (IOException ioException) {
                ioException.printStackTrace();
            }
        }
    }

    // Create a hash code for the given topic
    private int getTopic() {
        Scanner s = new Scanner(System.in);
        System.out.println("Enter the topic: ");
        topicString = s.nextLine();

        int code = topicString.hashCode();
        return code;
    }

    // Check if the current broker is the correct one
    // Otherwise close the current connection and connect to the right one
    private void connectToMatchedBroker(Broker matchedBroker) throws IOException{
        if (!Objects.equals(b.getIp(), matchedBroker.getIp()) || !Objects.equals(b.getPort(), matchedBroker.getPort())) {
            requestSocket.close();
            System.out.println("Connection to broker: " + b.getIp() + " on port: " + b.getPort() + " closed");
            b = matchedBroker;
            requestSocket = new Socket(b.getIp(), b.getPort());
            System.out.println("Connected to broker: " + b.getIp() + " on port: " + b.getPort());
        }
    }

    Publisher(Broker b) {
        this.b = b;
    }
}
