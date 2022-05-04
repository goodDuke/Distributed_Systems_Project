import java.io.*;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;

public class Consumer extends Thread implements Serializable {
    private Broker b;
    private Socket requestSocketConsumer;
    private ObjectOutputStream outConsumer;
    private ObjectInputStream inConsumer;
    private int topicCode;
    private ArrayList<byte[]> history = new ArrayList<>();
    private int pointerChunk = 0;

    public void run() {
        try {
            receiveAllData();
            boolean newMessage;
            while (!Thread.currentThread().isInterrupted()) {
                newMessage = inConsumer.readBoolean(); // 4C
                if (newMessage) {
                    System.out.println(2);
                    receiveData();
                }
            }
        } catch (IOException | ClassNotFoundException e) {
            e.printStackTrace();
        }
    }

    // Collecting the chunks for a specific file and adding them to the correct ArrayList
    private void receiveData() throws IOException, ClassNotFoundException {
        boolean isEmpty = inConsumer.readBoolean(); // 5C
        System.out.println(3);
        System.out.println(isEmpty);
        if (!isEmpty) {
            System.out.println("Fetching new message!");
            byte[] fileNameChunk = (byte[]) inConsumer.readObject(); // 6.1C
            history.add(fileNameChunk);
            byte[] blockCountChunk = (byte[]) inConsumer.readObject(); // 6.2C
            history.add(blockCountChunk);
            byte[] publisherId = (byte[]) inConsumer.readObject(); // 6.3C
            history.add(publisherId);

            // Converting blockCount to integer
            int blockCount = 0;
            for (byte b : blockCountChunk)
                blockCount += b;

            // Saving chunks in the corresponding ArrayList
            for (int i = 1; i <= blockCount; i++) {
                byte[] chunk = (byte[]) inConsumer.readObject(); // 6.4C
                history.add(chunk);
            }
            recreateFile(blockCount, fileNameChunk);
            System.out.println("New message fetched successfully!");
        }
    }

    private void recreateFile(int blockCount, byte[] fileNameChunk) throws IOException {
        String fileName = new String(fileNameChunk, StandardCharsets.UTF_8);
        String filepath = ".\\src\\recreated_files\\" + fileName;
        File file = new File(filepath);
        OutputStream stream = new FileOutputStream(file);
        byte[] completeFile = new byte[512 * 1024 * blockCount];
        int i;
        int j = 0;
        pointerChunk += 3;
        for (i = pointerChunk; i < pointerChunk + blockCount; i++) {
            for (byte b: history.get(i)) {
                completeFile[j] = b;
                j++;
            }
        }
        pointerChunk = i;

        stream.write(completeFile);
        stream.close();
    }

    private void receiveAllData() throws IOException, ClassNotFoundException {
        boolean isEmpty = inConsumer.readBoolean(); // 1C
        System.out.println(1);
        System.out.println(isEmpty);
        if (!isEmpty) {
            System.out.println("Fetching history!");
            int totalChunks = inConsumer.readInt();  // 2C
            System.out.println(totalChunks);
            for (int i = 1; i <= totalChunks; i++) {
                byte[] chunk = (byte[]) inConsumer.readObject(); // 3C
                history.add(chunk);
            }
            recreateAllData();
            System.out.println("History fetched successfully!");
        }
    }

    private void recreateAllData() throws IOException {
        int currentChunk = 0;
        while (currentChunk < history.size()) {
            String fileName = new String(history.get(currentChunk), StandardCharsets.UTF_8);
            String filepath = ".\\src\\recreated_files\\" + fileName;
            File file = new File(filepath);
            OutputStream stream = new FileOutputStream(file);
            currentChunk++;

            // Converting blockCount to integer
            int blockCount = 0;
            for (byte b : history.get(currentChunk))
                blockCount += b;

            currentChunk += 2;
            int j = 0;
            byte[] completeFile = new byte[512 * 1024 * blockCount];
            for (int i = currentChunk; i < currentChunk + blockCount; i++) {
                for (byte b: history.get(i)) {
                    completeFile[j] = b;
                    j++;
                }
            }
            stream.write(completeFile);
            stream.close();
            currentChunk += blockCount;
        }
    }

    Consumer(Broker b, int topicCode, Socket requestSocket, ObjectOutputStream out, ObjectInputStream in) {
        this.b = b;
        this.topicCode = topicCode;
        this.requestSocketConsumer = requestSocket;
        this.outConsumer = out;
        this.inConsumer = in;
    }
}