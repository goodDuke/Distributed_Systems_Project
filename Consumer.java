import java.io.*;
import java.net.Socket;
import java.util.LinkedList;
import java.util.Queue;

public class Consumer extends Thread{
    private Broker b;
    private Socket requestSocket;
    private ObjectOutputStream out;
    private ObjectInputStream in;
    private int topicCode;
    private Queue<byte[]> queue = new LinkedList<>();

    public void run() {
        try {
            receiveData();
        } catch (IOException | ClassNotFoundException e) {
            e.printStackTrace();
        }
    }

    // Collecting the chunks for a specific file and adding them to the correct queue
    private void receiveData() throws IOException, ClassNotFoundException {
        boolean isEmpty = in.readBoolean(); // 7
        if (!isEmpty) {
            System.out.println("Fetching new messages!");
            byte[] extension = (byte[]) in.readObject(); // 8.1
            queue.add(extension);
            byte[] blockCount = (byte[]) in.readObject(); // 8.2
            queue.add(blockCount);

            // Converting blockCount to integer
            int chunkCount = 0;
            for (byte b : blockCount)
                chunkCount += b;

            // Saving chunks in the corresponding queue
            for (int i = 1; i <= chunkCount; i++) {

                byte[] chunk = (byte[]) in.readObject(); // 8.3
                queue.add(chunk);
            }
            recreateFile(topicCode, chunkCount);
        }
    }

    private void recreateFile(int topicCode, int chunkCount) throws IOException {
        String filepath = ".\\src\\recreated_files\\mnm.txt";
        File file = new File(filepath);
        OutputStream stream = new FileOutputStream(file);
        byte[] completeFile = new byte[512 * 1024 * chunkCount];
        int i = 0;
        // Skip the first to chunks which contain the total chunkCount of the file and its
        int skipChunks = 2;
        for (byte[] chunk: queue) {
            if (skipChunks == 0) {
                for (byte b : chunk) {
                    completeFile[i] = b;
                    i++;
                }
            } else
                skipChunks--;
        }
        stream.write(completeFile);
        stream.close();
    }

    Consumer(Broker b, int topicCode, Socket requestSocket, ObjectOutputStream out, ObjectInputStream in) {
        this.b = b;
        this.topicCode = topicCode;
        this.requestSocket = requestSocket;
        this.out = out;
        this.in = in;
    }
}

