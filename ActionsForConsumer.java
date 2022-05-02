import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.util.HashMap;
import java.util.Queue;

public class ActionsForConsumer extends Thread implements Serializable {
    private ObjectInputStream inConsumer;
    private ObjectOutputStream outConsumer;
    private HashMap<Integer, Queue<byte[]>> queues;
    private int requestedTopic;
    private int pointerChunk = 0;
    private boolean newMessage;
    private Broker b;

    public void run() {
        try {
            pullAllData();
            while (!Thread.currentThread().isInterrupted()) {
                outConsumer.writeBoolean(newMessage); // 7C
                outConsumer.flush();
                if (newMessage) {
                    pull();
                    newMessage = false;
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private void pull() throws IOException {
        boolean isEmpty = queues.get(requestedTopic).isEmpty();
        outConsumer.writeBoolean(isEmpty); // 8C
        outConsumer.flush();
        int i = 0;
        for (byte[] chunk: queues.get(requestedTopic)) {
            if (i >= pointerChunk) {
                outConsumer.writeObject(chunk); // 9C
                outConsumer.flush();
            }
            i++;
        }
        pointerChunk = i;
    }

    private void pullAllData() throws IOException {
        boolean isEmpty = queues.get(requestedTopic).isEmpty();
        outConsumer.writeBoolean(isEmpty); // 8C
        outConsumer.flush();
        if (!isEmpty) {
            outConsumer.writeInt(queues.get(requestedTopic).size());
            outConsumer.flush();
            for (byte[] chunk : queues.get(requestedTopic)) {
                outConsumer.writeObject(chunk); // 9C
                outConsumer.flush();
            }
        }
    }

    public ActionsForConsumer(Broker b, ObjectInputStream inConsumer, ObjectOutputStream outConsumer,
                              HashMap<Integer, Queue<byte[]>> queues, int requestedTopic, boolean newMessage) {
        this.inConsumer = inConsumer;
        this.outConsumer = outConsumer;
        this.queues = queues;
        this.requestedTopic = requestedTopic;
        this.newMessage = newMessage;
        this.b = b;
    }
}