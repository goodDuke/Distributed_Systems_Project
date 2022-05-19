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
    private Socket requestSocketUser;
    private Socket requestSocketPublisher;
    private Socket requestSocketConsumer;
    private ObjectOutputStream outUser;
    private ObjectInputStream inUser;
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
        // TODO set IP
        Broker b1 = new Broker("127.0.0.1", 1300);

        // TODO set port, IP, id manually
        int port = 2300;
        String ip = "127.0.0.1";
        int id = 2;
        new User(ip, port, id, b1).connect();
    }

    private void connect() {
        try {
            requestSocketUser = new Socket(b.getIp(), b.getPort());
            requestSocketPublisher = new Socket(b.getIp(), b.getPort());
            requestSocketConsumer = new Socket(b.getIp(), b.getPort());
            outUser = new ObjectOutputStream(requestSocketUser.getOutputStream());
            inUser = new ObjectInputStream(requestSocketUser.getInputStream());
            outPublisher = new ObjectOutputStream(requestSocketPublisher.getOutputStream());
            inPublisher = new ObjectInputStream(requestSocketPublisher.getInputStream());
            outConsumer = new ObjectOutputStream(requestSocketConsumer.getOutputStream());
            inConsumer = new ObjectInputStream(requestSocketConsumer.getInputStream());
            System.out.println("\033[3mConnected to broker: " + b.getIp() + " on port: " + b.getPort() + "\033[0m");
            boolean disconnect = false;
            while (!disconnect) {
                while (true) {
                    topicCode = getTopic();
                    outUser.writeInt(id); // 1U
                    outUser.flush();

                    outUser.writeBoolean(firstConnection); // 2U
                    outUser.flush();

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
                    Broker matchedBroker = (Broker) inUser.readObject(); // 6U

                    // If the user pressed "Q" when asked to enter the topic disconnect
                    if (topicCode == 81) {
                        if (c != null)
                            c.interrupt();
                        disconnect = true;
                        break;
                    }

                    if (matchedBroker == null)
                        System.out.println("\033[3mThe topic \"" + topicString + "\" doesn't exist.\033[0m");
                    else {
                        connectToMatchedBroker(matchedBroker);
                        break;
                    }
                }

                if (topicCode != 81) {
                    c = new Consumer(b, topicCode, requestSocketConsumer, outConsumer, inConsumer);
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
                            p = new Publisher(b, topicCode, requestSocketPublisher,
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
            System.out.println("\033[3mAn error occurred while trying to connect to host: " + b.getIp() + " on port: " +
                    b.getPort() + ". Check the IP address and the port.\033[0m");
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
                System.out.println("\033[3mConnection to broker: " + b.getIp() + " on port: " + b.getPort() + " closed\033[0m");
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
            inUser.close();
            outUser.close();
            inPublisher.close();
            outPublisher.close();
            outConsumer.close();
            inConsumer.close();
            requestSocketUser.close();
            requestSocketPublisher.close();
            requestSocketConsumer.close();
            System.out.println("\033[3mConnection to broker: " + b.getIp() + " on port: " + b.getPort() + " closed\033[0m");
            b = matchedBroker;
            requestSocketUser = new Socket(b.getIp(), b.getPort());
            requestSocketPublisher = new Socket(b.getIp(), b.getPort());
            requestSocketConsumer = new Socket(b.getIp(), b.getPort());
            outUser = new ObjectOutputStream(requestSocketUser.getOutputStream());
            inUser = new ObjectInputStream(requestSocketUser.getInputStream());
            outPublisher = new ObjectOutputStream(requestSocketPublisher.getOutputStream());
            inPublisher = new ObjectInputStream(requestSocketPublisher.getInputStream());
            outConsumer = new ObjectOutputStream(requestSocketConsumer.getOutputStream());
            inConsumer = new ObjectInputStream(requestSocketConsumer.getInputStream());
            System.out.println("\033[3mConnected to broker: " + b.getIp() + " on port: " + b.getPort() + "\033[0m");
            firstConnection = false;
            outUser.writeInt(id); // 1U
            outUser.flush();
            outUser.writeBoolean(firstConnection); // 2U
            outUser.flush();
            outUser.writeInt(topicCode); // 3U
            outUser.flush();
        }
    }

    public User(String ip, int port, int id, Broker b) {
        this.ip = ip;
        this.port = port;
        this.b = b;
        this.id = id;
    }
}