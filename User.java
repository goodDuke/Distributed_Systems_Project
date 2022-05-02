import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.net.Socket;
import java.net.UnknownHostException;
import java.util.Objects;
import java.util.Scanner;

public class User implements Serializable {
    private String ip;
    private int port;
    private int id;
    private Broker b;
    private Socket requestSocketPublisher;
    private Socket requestSocketConsumer;
    private ObjectOutputStream outPublisher;
    private ObjectInputStream inPublisher;
    private ObjectOutputStream outConsumer;
    private ObjectInputStream inConsumer;
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
            requestSocketPublisher = new Socket(b.getIp(), b.getPort());
            requestSocketConsumer = new Socket(b.getIp(), b.getPort());
            outPublisher = new ObjectOutputStream(requestSocketPublisher.getOutputStream());
            inPublisher = new ObjectInputStream(requestSocketPublisher.getInputStream());
            outConsumer = new ObjectOutputStream(requestSocketConsumer.getOutputStream());
            inConsumer = new ObjectInputStream(requestSocketConsumer.getInputStream());
            System.out.println("Connected to broker: " + b.getIp() + " on port: " + b.getPort());
            boolean disconnect = false;
            while (!disconnect) {
                while (true) {
                    topicCode = getTopic();
                    outConsumer.writeInt(id); // 1C
                    outConsumer.flush();

                    outConsumer.writeBoolean(firstConnection); // 2C
                    outConsumer.flush();

                    outConsumer.writeObject(topicString); // 3C
                    outConsumer.flush();

                    outConsumer.writeInt(topicCode); // 4C
                    outConsumer.flush();

                    boolean registeredUser = inConsumer.readBoolean(); // 5C

                    if (!registeredUser) {
                        System.out.println("You are unable to access the requested topic.");
                        continue;
                    }
                    // Get broker object which contains the requested topic
                    Broker matchedBroker = (Broker) inConsumer.readObject(); // 6C

                    // If the user pressed "Q" when asked to enter the topic disconnect
                    if (topicCode == 81) {
                        if (c != null)
                            c.interrupt();
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

                if (topicCode != 81) {
                    c = new Consumer(b, topicCode, requestSocketConsumer, outConsumer, inConsumer);
                    c.start();
                }

                while(true) {
                    if (topicCode != 81) {
                        publisherMode = false;
                        Scanner s = new Scanner(System.in);
                        System.out.println("Press 'P' to enter publisher mode.\n" +
                                "Press anything else if you don't want to enter publisher mode:");
                        String publisherInput = s.nextLine();
                        if (publisherInput.equals("P"))
                            publisherMode = true;

                        outPublisher.writeBoolean(publisherMode); // 1P
                        outPublisher.flush();
                        if (publisherMode) {
                            p = new Publisher(b, topicCode, requestSocketPublisher,
                                    outPublisher, inPublisher, id);
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
                                outPublisher.writeObject(input); // 7P
                                outPublisher.flush();
                                break;
                            case "Q":
                                outPublisher.writeObject(input); // 7P
                                outPublisher.flush();
                                disconnect = true;
                                break;
                            default:
                                outPublisher.writeObject(input); // 7P
                                outPublisher.flush();
                        }
                        if (newTopic || disconnect) {
                            c.interrupt();
                            break;
                        }
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
                inPublisher.close();
                outPublisher.close();
                outConsumer.close();
                inConsumer.close();
                requestSocketPublisher.close();
                requestSocketConsumer.close();
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
            inPublisher.close();
            outPublisher.close();
            outConsumer.close();
            inConsumer.close();
            requestSocketPublisher.close();
            requestSocketConsumer.close();
            System.out.println("Connection to broker: " + b.getIp() + " on port: " + b.getPort() + " closed");
            b = matchedBroker;
            requestSocketPublisher = new Socket(b.getIp(), b.getPort());
            requestSocketConsumer = new Socket(b.getIp(), b.getPort());
            outPublisher = new ObjectOutputStream(requestSocketPublisher.getOutputStream());
            inPublisher = new ObjectInputStream(requestSocketPublisher.getInputStream());
            outConsumer = new ObjectOutputStream(requestSocketConsumer.getOutputStream());
            inConsumer = new ObjectInputStream(requestSocketConsumer.getInputStream());
            System.out.println("Connected to broker: " + b.getIp() + " on port: " + b.getPort());
            firstConnection = false;
            outConsumer.writeInt(id); // 1C
            outConsumer.flush();
            outConsumer.writeBoolean(firstConnection); // 2C
            outConsumer.flush();
            outConsumer.writeInt(topicCode); // 3C
            outConsumer.flush();
        }
    }

    public User(String ip, int port, int id, Broker b) {
        this.ip = ip;
        this.port = port;
        this.b = b;
        this.id = id;
    }
}

// όταν ξανασυνδεόμαστε στο ίδιο τόπικ και πάμε να κάνουμε publish ξανά
//