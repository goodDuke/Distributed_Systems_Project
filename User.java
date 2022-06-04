import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.net.Socket;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Objects;
import java.util.Scanner;

public class User implements Serializable {
    private static int brokerPort;
    private static String brokerIp;
    private String ip;
    private int port;
    private int id;
    private Socket requestSocketUser;
    private Socket requestSocketPublisher;
    private Socket requestSocketConsumer;
    private ObjectOutputStream outUser;
    private ObjectInputStream inUser;
    private ObjectOutputStream outPublisher;
    private ObjectInputStream inPublisher;
    private ObjectOutputStream outConsumer;
    private ObjectInputStream inConsumer;
    private ArrayList<String> userTopics;
    private int topicCode;
    private String topicString;
    private boolean firstConnection = true;
    boolean publisherMode = false;
    private Thread p;
    private Thread c;

    public static void main(String[] args) {
        // TODO set IP
        brokerIp = "192.168.68.108";
        brokerPort = 1200;

        // TODO set port, IP, id manually
        int port = 2200;
        String ip = "192.168.68.108";
        int id = 1;
        new User(ip, port, id).connect();
    }

    private void connect() {
        try {
            requestSocketUser = new Socket(brokerIp, brokerPort);
            requestSocketPublisher = new Socket(brokerIp, brokerPort);
            requestSocketConsumer = new Socket(brokerIp, brokerPort);
            outUser = new ObjectOutputStream(requestSocketUser.getOutputStream());
            inUser = new ObjectInputStream(requestSocketUser.getInputStream());
            outPublisher = new ObjectOutputStream(requestSocketPublisher.getOutputStream());
            inPublisher = new ObjectInputStream(requestSocketPublisher.getInputStream());
            outConsumer = new ObjectOutputStream(requestSocketConsumer.getOutputStream());
            inConsumer = new ObjectInputStream(requestSocketConsumer.getInputStream());
            System.out.println("\033[3mConnected to broker: " + brokerIp + " on port: " + brokerPort + "\033[0m");
            boolean disconnect = false;
            while (!disconnect) {
                while (true) {
                    outUser.writeInt(id); // 1U
                    outUser.flush();

                    userTopics = (ArrayList<String>) inUser.readObject();

                    outUser.writeBoolean(firstConnection); // 2U
                    outUser.flush();

                    topicCode = getTopic();

                    outUser.writeObject(topicString); // 3U
                    outUser.flush();

                    outUser.writeInt(topicCode); // 4U
                    outUser.flush();

                    boolean registeredUser = inUser.readBoolean(); // 5U

                    if (!registeredUser) {
                        System.out.println("\033[3mYou are unable to access the requested topic." +
                                "(not registered user)\033[0m");
                        continue;
                    }

                    // Get broker object which contains the requested topic
                    String matchedBrokerIp = (String) inUser.readObject(); // 6U
                    int matchedBrokerPort = inUser.readInt();

                    // If the user pressed "Q" when asked to enter the topic disconnect
                    if (topicCode == 81) {
                        if (c != null)
                            c.interrupt();
                        disconnect = true;
                        break;
                    }

                    if (matchedBrokerPort == 0)
                        System.out.println("\033[3mThe topic \"" + topicString + "\" doesn't exist.\033[0m");
                    else {
                        connectToMatchedBroker(matchedBrokerIp, matchedBrokerPort);
                        break;
                    }
                }

                if (topicCode != 81) {
                    c = new Consumer(brokerIp, brokerPort, topicCode, requestSocketConsumer, outConsumer, inConsumer);
                    c.start();
                }

                while (true) {
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
                            p = new Publisher(brokerIp, brokerPort, topicCode, requestSocketPublisher,
                                    outPublisher, inPublisher, id);
                            p.start();
                            p.join();
                        }
                        System.out.println("Press 'T' if you want to connect to a different topic.\n" +
                                "Press anything else if you want to remain in the same topic:");
                        String input = s.nextLine();
                        boolean newTopic = false;
                        switch (input) {
                            case "T":
                                firstConnection = true;
                                newTopic = true;
                                outUser.writeObject(input); // 7U
                                outUser.flush();
                                break;
                            default:
                                outUser.writeObject(input); // 7U
                                outUser.flush();
                        }
                        if (newTopic) {
                            c.interrupt();
                            break;
                        }
                    } else
                        break;
                }
                // If the consumer thread is still alive (waiting for an input in the receiveData function)
                // and we try to close it an error will be produced. In order to avoid the error
                // we check whether the thread is still alive and if it is the appropriate messages are send to it.
                // After the execution of the function the c.interrupted command is executed and the thread is interrupted.
                if (c != null && c.isAlive()) {
                    outUser.writeBoolean(true); // 8U
                    outUser.flush();
                } else {
                    outUser.writeBoolean(false); // 8U
                    outUser.flush();
                }
            }
        } catch (UnknownHostException unknownHost) {
            System.err.println("\033[3mYou are trying to connect to an unknown host!\033[0m");
        } catch (ClassNotFoundException | IOException e) {
            System.out.println("\033[3mAn error occurred while trying to connect to host: " + brokerIp + " on port: " +
                    brokerPort + ". Check the IP address and the port.\033[0m");
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        } finally {
            try {
                inUser.close();
                outUser.close();
                inPublisher.close();
                outPublisher.close();
                outConsumer.close();
                inConsumer.close();
                requestSocketPublisher.close();
                requestSocketConsumer.close();
                System.out.println("\033[3mConnection to broker: " + brokerIp + " on port: " + brokerPort + " closed\033[0m");
            } catch (IOException ioException) {
                ioException.printStackTrace();
            }
        }
    }

    // Create a hash code for the given topic
    private int getTopic() {
        Scanner s = new Scanner(System.in);
        System.out.println("Enter the topic for port " + brokerPort + " or press 'Q' to disconnect: ");
        topicString = s.nextLine();
        return topicString.hashCode();
    }

    // Check if the current broker is the correct one
    // Otherwise close the current connection and connect to the right one
    private void connectToMatchedBroker(String matchedBrokerIp, int matchedBrokerPort) throws IOException {
        if (!Objects.equals(brokerIp, matchedBrokerIp) || !Objects.equals(brokerPort, matchedBrokerPort)) {
            inUser.close();
            outUser.close();
            inPublisher.close();
            outPublisher.close();
            outConsumer.close();
            inConsumer.close();
            requestSocketUser.close();
            requestSocketPublisher.close();
            requestSocketConsumer.close();
            System.out.println("\033[3mConnection to broker: " + brokerIp + " on port: " + brokerPort + " closed\033[0m");
            brokerIp = matchedBrokerIp;
            brokerPort = matchedBrokerPort;
            requestSocketUser = new Socket(brokerIp, brokerPort);
            requestSocketPublisher = new Socket(brokerIp, brokerPort);
            requestSocketConsumer = new Socket(brokerIp, brokerPort);
            outUser = new ObjectOutputStream(requestSocketUser.getOutputStream());
            inUser = new ObjectInputStream(requestSocketUser.getInputStream());
            outPublisher = new ObjectOutputStream(requestSocketPublisher.getOutputStream());
            inPublisher = new ObjectInputStream(requestSocketPublisher.getInputStream());
            outConsumer = new ObjectOutputStream(requestSocketConsumer.getOutputStream());
            inConsumer = new ObjectInputStream(requestSocketConsumer.getInputStream());
            System.out.println("\033[3mConnected to broker: " + brokerIp + " on port: " + brokerPort + "\033[0m");
            firstConnection = false;
            outUser.writeInt(id); // 1U
            outUser.flush();
            outUser.writeBoolean(firstConnection); // 2U
            outUser.flush();
            outUser.writeInt(topicCode); // 3U
            outUser.flush();
        }
    }

    public User(String ip, int port, int id) {
        this.ip = ip;
        this.port = port;
        this.id = id;
    }
}