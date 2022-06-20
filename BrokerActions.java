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
    private ArrayList<String> userTopics = new ArrayList<>();
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

                    registeredTopics();
                    outUser.writeObject(userTopics);
                    outUser.flush();

                    boolean firstConnection = inUser.readBoolean(); // 2U
                    if (firstConnection) {
                        matchedBroker = getBroker();
                        if (matchedBroker != 0 || requestedTopic == 81) {
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
                        System.out.println("Here");
                        publisherMode = inPublisher.readBoolean(); // 1P
                        System.out.println(publisherMode);
                        if (publisherMode) {
                            p = new ActionsForPublishers(brokers, topics, b.getIp(), b.getPort(),
                                    outPublisher, inPublisher);
                            p.start();
                            p.join();
                            BrokerActions.newMessage = true;
                        }

                        boolean checkBackButton = inUser.readBoolean(); // 7U
                        System.out.println(checkBackButton);
                        if (checkBackButton) {
                            c.interrupt();
                            System.out.println("here");
                            break;
                        }
                    } else if (currentBroker != matchedBroker) {
                        disconnect = true;
                        break;
                    }
                }
                if (currentBroker == matchedBroker && inUser.readBoolean()) { //8U
                    outConsumer.writeObject(null);
                    outConsumer.flush();
                    outConsumer.writeObject(null);
                    outConsumer.flush();
                    outConsumer.writeObject(null);
                    outConsumer.flush();
                }
            }
        } catch (IOException | InterruptedException e) {
            e.printStackTrace();
        }
    }

    // Find the broker that contains the requested topic
    private int getBroker() {
        String matchedBrokerIp = "0";
        int matchedBrokerPort = 0;
        int matchedBroker = -1;
        try {
            String topicString = (String) inUser.readObject(); // 3U
            System.out.println(topicString);
            requestedTopic = inUser.readInt(); // 4U
            System.out.println(requestedTopic);
            //boolean registeredUser = checkUser(topicString);
            //outUser.writeBoolean(registeredUser); // 5U
            //outUser.flush();
            // If the user is registered to use the requested topic search for the corresponding broker
            //if (registeredUser) {
                for (int i = 0; i < brokers.size(); i++) {
                    for (int topic : topics[i]) {
                        if (requestedTopic == topic) {
                            matchedBrokerIp = brokers.get(i).getIp();
                            matchedBrokerPort = brokers.get(i).getPort();
                            matchedBroker = i;
                            break;
                        }
                    }
                    if (matchedBrokerPort != 0) {
                        System.out.println(matchedBrokerIp);
                        outUser.writeObject(matchedBrokerIp); // 6U
                        outUser.flush();
                        outUser.writeInt(matchedBrokerPort); // 6U
                        outUser.flush();
                        break;
                    }
                }
                if (matchedBrokerPort == 0) {
                    outUser.writeObject(matchedBrokerIp); // 6U
                    outUser.flush();
                    outUser.writeInt(matchedBrokerPort); // 6U
                    outUser.flush();
                }
            //}

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

    private void registeredTopics() {
        for (String[] x: topicsAndUsers) {
            for (String id: x[1].split(",")) {
                if (Integer.toString(currentUser).equals(id)) {
                    userTopics.add(x[0]);
                    break;
                }
            }
        }
    }

    public static HashMap<Integer, Queue<byte[]>> getQueues() {
        return queues;
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