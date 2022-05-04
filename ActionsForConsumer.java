import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.HashMap;
import java.util.Queue;

public class ActionsForConsumer extends Thread implements Serializable {
    private ObjectInputStream inConsumer;
    private ObjectOutputStream outConsumer;
    private HashMap<Integer, Queue<byte[]>> queues;
    private int requestedTopic;
    private int pointerChunk;

    public void run() {
        try {
            pullAllData();
            while (!Thread.currentThread().isInterrupted()) {
                outConsumer.writeBoolean(BrokerActions.newMessage); // 4C
                outConsumer.flush();
                System.out.println("HERE");
                if (BrokerActions.newMessage) {
                    System.out.println(2);
                    System.out.println(BrokerActions.newMessage);
                    outConsumer.writeBoolean(BrokerActions.newMessage); // 4C
                    outConsumer.flush();
                    pull();
                    BrokerActions.newMessage = false;
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private void pull() throws IOException {
        boolean isEmpty = queues.get(requestedTopic).isEmpty();
        outConsumer.writeBoolean(isEmpty); // 5C
        outConsumer.flush();
        int i = 0;
        for (byte[] chunk: queues.get(requestedTopic)) {
            if (i >= pointerChunk) {
                outConsumer.writeObject(chunk); // 6C
                outConsumer.flush();
            }
            i++;
        }
        pointerChunk = i;
    }

    private void pullAllData() throws IOException {
        boolean isEmpty = queues.get(requestedTopic).isEmpty();
        System.out.println(1);
        System.out.println(isEmpty);
        outConsumer.writeBoolean(isEmpty); // 1C
        outConsumer.flush();
        if (!isEmpty) {
            outConsumer.writeInt(queues.get(requestedTopic).size()); // 2C
            outConsumer.flush();
            for (byte[] chunk : queues.get(requestedTopic)) {
                outConsumer.writeObject(chunk); // 3C
                outConsumer.flush();
            }
        }
        pointerChunk = queues.get(requestedTopic).size();
    }

    public ActionsForConsumer(ObjectInputStream inConsumer, ObjectOutputStream outConsumer,
                              HashMap<Integer, Queue<byte[]>> queues, int requestedTopic) {
        this.inConsumer = inConsumer;
        this.outConsumer = outConsumer;
        this.queues = queues;
        this.requestedTopic = requestedTopic;
    }
}