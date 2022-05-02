import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.HashMap;
import java.util.Queue;

public class ActionsForConsumer extends Thread{
    private ObjectInputStream inConsumer;
    private ObjectOutputStream outConsumer;
    private HashMap<Integer, Queue<byte[]>> queues;
    private int requestedTopic;
    private int pointerChunk = 0;

    public void run() {
        try {
            pullAllData();
            boolean newMessage = false;
            int length = queues.get(requestedTopic).size();
            while (true) {
                if (length != queues.get(requestedTopic).size()) {
                    newMessage = true;
                    length = queues.get(requestedTopic).size();
                }
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

    public ActionsForConsumer(ObjectInputStream inConsumer, ObjectOutputStream outConsumer,
                              HashMap<Integer, Queue<byte[]>> queues, int requestedTopic) {
        this.inConsumer = inConsumer;
        this.outConsumer = outConsumer;
        this.queues = queues;
        this.requestedTopic = requestedTopic;
    }
}
