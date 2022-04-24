import java.io.*;
import java.net.*;
import java.util.HashMap;

public class ActionsForPublishers extends Thread {
    ObjectInputStream in;
    ObjectOutputStream out;
    private HashMap<Integer, Broker> brokers;
    private int[][] topics;

    public ActionsForPublishers(Socket connection, HashMap<Integer, Broker> brokers, int[][] topics) {
        try {
            this.brokers = brokers;
            this.topics = topics;
            out = new ObjectOutputStream(connection.getOutputStream());
            in = new ObjectInputStream(connection.getInputStream());
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public void run() {
        try {
            boolean firstConnection = in.readBoolean();
            if (firstConnection)
                getBroker();
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            try {
                in.close();
                out.close();
            } catch (IOException ioException) {
                ioException.printStackTrace();
            }
        }
    }

    // Find the broker that contains the requested topic
    private void getBroker() {
        int matchedBroker = 0;
        try {
            int requestedTopic = in.readInt();
            for (int i = 0; i < brokers.size(); i++) {
                for (int topic : topics[i]) {
                    if (requestedTopic == topic) {
                        matchedBroker = i + 1;
                        break;
                    }
                }
                if (matchedBroker != 0) {
                    out.writeObject(brokers.get(matchedBroker));
                    out.flush();
                    break;
                }
            }
            if (matchedBroker == 0) {
                out.writeObject(null);
                out.flush();
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
