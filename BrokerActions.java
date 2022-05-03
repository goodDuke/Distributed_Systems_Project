import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.util.*;

public class BrokerActions extends Thread implements Serializable {
    private boolean publisherMode;
    private Thread p;
    private Thread c;
    private int currentBroker;
    private int currentUser;
    private int requestedTopic;
    private ArrayList<String[]> topicsAndUsers;
    private ObjectOutputStream outUser;
    private ObjectInputStream inUser;
    private ObjectInputStream inPublisher;
    private ObjectOutputStream outPublisher;
    private ObjectInputStream inConsumer;
    private ObjectOutputStream outConsumer;
    private static HashMap<Integer, Queue<byte[]>> queues = new HashMap<>();
    private static HashMap<Integer, Broker> brokers = new HashMap<>();
    private static int[][] topics;
    private Broker b;
    static volatile boolean newMessage = false;

    public void run() {
        try {
            boolean disconnect = false;
            while (!disconnect) {
                // Check which broker contains the requested topic only if the
                // current broker is the first one the publisher connected to
                int matchedBroker;
                while (true) {
                    currentUser = inUser.readInt(); // 1U
                    boolean firstConnection = inUser.readBoolean(); // 2U
                    if (firstConnection) {
                        matchedBroker = getBroker();
                        if (matchedBroker != -1 || requestedTopic == 81) {
                            if (c != null) {
                                c.interrupt();
                            }
                            break;
                        }
                    } else if (requestedTopic != 81) {
                        requestedTopic = inUser.readInt(); // 3U
                        matchedBroker = currentBroker;
                        break;
                    }
                }

                if (requestedTopic != 81 && currentBroker == matchedBroker) {
                    c = new ActionsForConsumer(inConsumer, outConsumer, queues, requestedTopic);
                    c.start();
                }

                while (true) {
                    if (requestedTopic != 81 && currentBroker == matchedBroker) {
                        publisherMode = inPublisher.readBoolean(); // 1P
                        if (publisherMode) {
                            p = new ActionsForPublishers(brokers, topics, b.getIp(), b.getPort(),
                                    outPublisher, inPublisher, queues);
                            p.start();
                            p.join();
                            BrokerActions.newMessage = true;
                        }

                        String userInput = (String) inUser.readObject(); // 7U
                        if (userInput.equals("T") || userInput.equals("Q")) {
                            if (userInput.equals("Q"))
                                disconnect = true;
                            c.interrupt();
                            break;
                        }
                    } else if (currentBroker != matchedBroker) {
                        disconnect = true;
                        break;
                    }
                }
            }
        } catch (IOException | InterruptedException | ClassNotFoundException e) {
            e.printStackTrace();
        }
    }

    // Find the broker that contains the requested topic
    private int getBroker() {
        int matchedBroker = -1;
        try {
            String topicString = (String) inUser.readObject(); // 3U
            requestedTopic = inUser.readInt(); // 4U
            boolean registeredUser = checkUser(topicString);
            outUser.writeBoolean(registeredUser); // 5U
            outUser.flush();
            // If the user is registered to use the requested topic search for the corresponding broker
            if (registeredUser) {
                for (int i = 0; i < brokers.size(); i++) {
                    for (int topic : topics[i]) {
                        if (requestedTopic == topic) {
                            matchedBroker = i;
                            break;
                        }
                    }
                    if (matchedBroker != -1) {
                        outUser.writeObject(brokers.get(matchedBroker)); // 6U
                        outUser.flush();
                        break;
                    }
                }
                if (matchedBroker == -1) {
                    outUser.writeObject(null); // 6U
                    outUser.flush();
                }
            }

        } catch (IOException | ClassNotFoundException e) {
            e.printStackTrace();
        }
        return matchedBroker;
    }

    // Check whether the user requesting the topic is registered to use it
    private boolean checkUser(String topicString) {
        // Return false only if the topic exists and the user isn't registered to use it
        // Return true otherwise
        boolean topicExists = false;
        for (String[] x: topicsAndUsers) {
            if (x[0].equals(topicString)) {
                topicExists = true;
                // If there are users registered in the topic
                if (x.length == 2) {
                    for (String id : x[1].split(",")) {
                        if (currentUser == Integer.parseInt(id))
                            return true;
                    }
                }
            }
        }
        if (topicExists) {
            return false;
        } else
            return true;
    }

    BrokerActions(ObjectInputStream inUser, ObjectOutputStream outUser,
                  ObjectInputStream inPublisher, ObjectOutputStream outPublisher,
                  ObjectInputStream inConsumer, ObjectOutputStream outConsumer,
                  ArrayList<String[]> topicsAndUsers, HashMap<Integer,
            Broker> brokers, int[][] topics, Broker b, HashMap<Integer, Queue<byte[]>> queues, int currentBroker) {
        this.inUser = inUser;
        this.outUser = outUser;
        this.inPublisher = inPublisher;
        this.outPublisher = outPublisher;
        this.inConsumer = inConsumer;
        this.outConsumer = outConsumer;
        this.topicsAndUsers = topicsAndUsers;
        BrokerActions.brokers = brokers;
        BrokerActions.topics = topics;
        this.b = b;
        this.queues = queues;
        this.currentBroker = currentBroker;
    }
}