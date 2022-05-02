import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.*;

public class BrokerActions extends Thread{
    private boolean publisherMode;
    private Thread p;
    private Thread c;
    private int currentBroker;
    private int currentUser;
    private int requestedTopic;
    private ArrayList<String[]> topicsAndUsers;
    private ObjectInputStream inPublisher;
    private ObjectOutputStream outPublisher;
    private ObjectInputStream inConsumer;
    private ObjectOutputStream outConsumer;
    private static HashMap<Integer, Queue<byte[]>> queues = new HashMap<>();
    private static HashMap<Integer, Broker> brokers = new HashMap<Integer, Broker>();
    private static int[][] topics;
    private Broker b;

    public void run() {
        try {
            boolean disconnect = false;
            while (!disconnect) {
                // Check which broker contains the requested topic only if the
                // current broker is the first one the publisher connected to
                int matchedBroker;
                while (true) {
                    currentUser = inConsumer.readInt(); // 1C
                    boolean firstConnection = inConsumer.readBoolean(); // 2C
                    if (firstConnection) {
                        matchedBroker = getBroker();
                        if (matchedBroker != -1 || requestedTopic == 81)
                            break;
                    } else if (requestedTopic != 81) {
                        requestedTopic = inConsumer.readInt(); // 3C
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
                        }
                        String userInput = (String) inPublisher.readObject(); // 7P

                        if (userInput.equals("T") || userInput.equals("Q")) {
                            if (userInput.equals("Q"))
                                disconnect = true;
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
            String topicString = (String) inConsumer.readObject(); // 3C
            requestedTopic = inConsumer.readInt(); // 4C
            boolean registeredUser = checkUser(topicString);
            outConsumer.writeBoolean(registeredUser); // 5C
            outConsumer.flush();
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
                        outConsumer.writeObject(brokers.get(matchedBroker)); // 6C
                        outConsumer.flush();
                        break;
                    }
                }
                if (matchedBroker == -1) {
                    outConsumer.writeObject(null); // 6C
                    outConsumer.flush();
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
        if (topicExists)
            return false;
        else
            return true;
    }

    BrokerActions(ObjectInputStream inPublisher, ObjectOutputStream outPublisher,
                  ObjectInputStream inConsumer, ObjectOutputStream outConsumer,
                  ArrayList<String[]> topicsAndUsers, HashMap<Integer,
            Broker> brokers, int[][] topics, Broker b, HashMap<Integer, Queue<byte[]>> queues, int currentBroker) {
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
