import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.Socket;
import java.net.UnknownHostException;
import java.util.Objects;
import java.util.Scanner;

public class User {
    private String ip;
    private int port;
    private Broker b;
    private Socket requestSocket;
    private ObjectOutputStream out;
    private ObjectInputStream in;
    private int topicCode;
    private String topicString;
    private boolean firstConnection = true;
    boolean publisherMode = false;

    public static void main(String args[]) throws InterruptedException {
        Broker b1 = new Broker("127.0.0.1", 1100);
        //Broker b2 = new Broker("127.0.0.1", 1200);
        //Broker b3 = new Broker("127.0.0.1", 1300);

        // TODO set port and IP manually
        int port = 1200;
        String ip = "127.0.0.1";
        new User(ip, port, b1).connect();
    }

    private void connect() {
        try {
            requestSocket = new Socket(b.getIp(), b.getPort());
            out = new ObjectOutputStream(requestSocket.getOutputStream());
            in = new ObjectInputStream(requestSocket.getInputStream());
            System.out.println("Connected to broker: " + b.getIp() + " on port: " + b.getPort());
            while (true) {
                topicCode = getTopic();
                if (topicCode == 81)
                    break;

                out.writeBoolean(firstConnection); // 1
                out.flush();

                out.writeInt(topicCode); // 2
                out.flush();

                // Get broker object which contains the requested topic
                Broker matchedBroker = (Broker) in.readObject(); // 3

                if (matchedBroker == null)
                    System.out.println("The topic \"" + topicString + "\" doesn't exist.");
                else {
                    connectToMatchedBroker(matchedBroker);
                    break;
                }
            }

            if (topicCode != 81) {
                Scanner s = new Scanner(System.in);
                System.out.println("Press p to enter publisher mode: ");
                String publisherInput = s.nextLine();
                if (publisherInput.equals("p"))
                    publisherMode = true;

                out.writeBoolean(publisherMode); //4
                out.flush();
                if (publisherMode)
                    new Publisher(b, topicCode, requestSocket, out, in).start();
            }

        } catch (UnknownHostException unknownHost) {
            System.err.println("You are trying to connect to an unknown host!");
        } catch (ClassNotFoundException | IOException e) {
            System.out.println("An error occurred while trying to connect to host: " + b.getIp() + " on port: " +
                    b.getPort() + ". Check the IP address and the port.");
            e.printStackTrace();
        }
    }

    // Create a hash code for the given topic
    private int getTopic() {
        Scanner s = new Scanner(System.in);
        System.out.println("Enter the topic for port " + b.getPort() + " or press 'Q' to quit connection: ");
        topicString = s.nextLine();
        return topicString.hashCode();
    }

    // Check if the current broker is the correct one
    // Otherwise close the current connection and connect to the right one
    private void connectToMatchedBroker(Broker matchedBroker) throws IOException{
        if (!Objects.equals(b.getIp(), matchedBroker.getIp()) || !Objects.equals(b.getPort(), matchedBroker.getPort())) {
            in.close();
            out.close();
            requestSocket.close();
            System.out.println("Connection to broker: " + b.getIp() + " on port: " + b.getPort() + " closed");
            b = matchedBroker;
            requestSocket = new Socket(b.getIp(), b.getPort());
            System.out.println("Connected to broker: " + b.getIp() + " on port: " + b.getPort());
            out = new ObjectOutputStream(requestSocket.getOutputStream());
            in = new ObjectInputStream(requestSocket.getInputStream());
            firstConnection = false;
            out.writeBoolean(firstConnection); // 1
            out.flush();
        }
    }

    public User(String ip, int port, Broker b) {
        this.ip = ip;
        this.port = port;
        this.b = b;
    }
}