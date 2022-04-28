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
    private int id;
    private Broker b;
    private Socket requestSocket;
    private ObjectOutputStream out;
    private ObjectInputStream in;
    private int topicCode;
    private String topicString;
    private boolean firstConnection = true;
    boolean publisherMode = false;
    private Thread p;
    private Thread c;

    public static void main(String args[]) {
        Broker b1 = new Broker("127.0.0.1", 1100);
        //Broker b2 = new Broker("127.0.0.1", 1200);
        //Broker b3 = new Broker("127.0.0.1", 1300);

        // TODO set port, IP, id manually
        int port = 2200;
        String ip = "127.0.0.1";
        int id = 0;
        new User(ip, port, id, b1).connect();
    }

    private void connect() {
        try {
            requestSocket = new Socket(b.getIp(), b.getPort());
            out = new ObjectOutputStream(requestSocket.getOutputStream());
            in = new ObjectInputStream(requestSocket.getInputStream());
            System.out.println("Connected to broker: " + b.getIp() + " on port: " + b.getPort());
            boolean disconnect = false;
            while (!disconnect) {
                while (true) {
                    topicCode = getTopic();
                    out.writeInt(id); // 1
                    out.flush();

                    out.writeBoolean(firstConnection); // 2
                    out.flush();

                    out.writeObject(topicString); // 3
                    out.flush();

                    out.writeInt(topicCode); // 4
                    out.flush();

                    boolean registeredUser = in.readBoolean(); // 5

                    if (!registeredUser) {
                        System.out.println("You are unable to access the requested topic.");
                        continue;
                    }
                    // Get broker object which contains the requested topic
                    Broker matchedBroker = (Broker) in.readObject(); // 6

                    // If the user pressed "Q" when asked to enter the topic disconnect
                    if (topicCode == 81) {
                        disconnect = true;
                        break;
                    }

                    if (matchedBroker == null)
                        System.out.println("The topic \"" + topicString + "\" doesn't exist.");
                    else {
                        connectToMatchedBroker(matchedBroker);
                        break;
                    }
                }
                while(true) {
                    if (topicCode != 81) {
                        c = new Consumer(b, topicCode, requestSocket, out, in);
                        c.start();
                        c.join();

                        publisherMode = false;
                        Scanner s = new Scanner(System.in);
                        System.out.println("Press 'P' to enter publisher mode.\n" +
                                "Press anything else if you don't want to enter publisher mode:");
                        String publisherInput = s.nextLine();
                        if (publisherInput.equals("P"))
                            publisherMode = true;

                        out.writeBoolean(publisherMode); // 9
                        out.flush();
                        if (publisherMode) {
                            p = new Publisher(b, topicCode, requestSocket, out, in);
                            p.start();
                            p.join();
                        }
                        System.out.println("Press 'T' if you want to connect to a different topic " +
                                "or 'Q' if you want to disconnect from the broker.\n" +
                                "Press anything else if you want to remain in the same topic:");
                        String input = s.nextLine();
                        boolean newTopic = false;
                        switch (input) {
                            case "T":
                                firstConnection = true;
                                newTopic = true;
                                out.writeObject(input); // 13
                                out.flush();
                                break;
                            case "Q":
                                out.writeObject(input); // 12
                                out.flush();
                                disconnect = true;
                                break;
                            default:
                                out.writeObject(input); // 12
                                out.flush();
                                System.out.println("Checking for new messages.");
                        }
                        if (newTopic || disconnect)
                            break;
                    } else
                        break;
                }
            }
        } catch (UnknownHostException unknownHost) {
            System.err.println("You are trying to connect to an unknown host!");
        } catch (ClassNotFoundException | IOException e) {
            System.out.println("An error occurred while trying to connect to host: " + b.getIp() + " on port: " +
                    b.getPort() + ". Check the IP address and the port.");
            e.printStackTrace();
        } catch (InterruptedException e) {
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
        System.out.println("Enter the topic for port " + b.getPort() + " or press 'Q' to disconnect: ");
        topicString = s.nextLine();
        return topicString.hashCode();
    }

    // Check if the current broker is the correct one
    // Otherwise close the current connection and connect to the right one
    private void connectToMatchedBroker(Broker matchedBroker) throws IOException {
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
            out.writeInt(id); // 1
            out.flush();
            out.writeBoolean(firstConnection); // 2
            out.flush();
            out.writeInt(topicCode); // 3
            out.flush();
        }
    }

    public User(String ip, int port, int id, Broker b) {
        this.ip = ip;
        this.port = port;
        this.b = b;
        this.id = id;
    }
}

// Αντί να κρατάμε για κάθε user ποια μηνύματα έχει διαβάσει μπορούμε να του στέλνουμε
// αυτόματα τα 5 τελευταία και να τον ρωτάμε αν θέλει να δει όλα τα υπόλοιπα
