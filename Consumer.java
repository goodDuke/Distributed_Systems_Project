import java.io.*;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;

public class Consumer extends Thread{
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
            while (true) {
                newMessage = inConsumer.readBoolean(); // 7C
                if (newMessage)
                    receiveData();
            }
        } catch (IOException | ClassNotFoundException e) {
            e.printStackTrace();
        }
    }

    private void receiveAllData() throws IOException, ClassNotFoundException {
        boolean isEmpty = inConsumer.readBoolean(); // 8C
        if (!isEmpty) {
            System.out.println("Fetching history!");
            int totalChunks = inConsumer.readInt();
            for (int i = 1; i <= totalChunks; i++) {
                byte[] chunk = (byte[]) inConsumer.readObject(); // 9.4C
                history.add(chunk);
            }
        }
        recreateAllData();
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

    // Collecting the chunks for a specific file and adding them to the correct ArrayList
    private void receiveData() throws IOException, ClassNotFoundException {
        boolean isEmpty = inConsumer.readBoolean(); // 8C
        if (!isEmpty) {
            System.out.println("Fetching new message!");
            byte[] fileNameChunk = (byte[]) inConsumer.readObject(); // 9.1C
            history.add(fileNameChunk);
            byte[] blockCountChunk = (byte[]) inConsumer.readObject(); // 9.2C
            history.add(blockCountChunk);
            byte[] publisherId = (byte[]) inConsumer.readObject(); // 9.3C
            history.add(publisherId);

            // Converting blockCount to integer
            int blockCount = 0;
            for (byte b : blockCountChunk)
                blockCount += b;

            // Saving chunks in the corresponding ArrayList
            for (int i = 1; i <= blockCount; i++) {
                byte[] chunk = (byte[]) inConsumer.readObject(); // 9.4C
                history.add(chunk);
            }
            recreateFile(blockCount, fileNameChunk);
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
        pointerChunk = i+1;

        stream.write(completeFile);
        stream.close();
    }

    Consumer(Broker b, int topicCode, Socket requestSocket, ObjectOutputStream out, ObjectInputStream in) {
        this.b = b;
        this.topicCode = topicCode;
        this.requestSocketConsumer = requestSocket;
        this.outConsumer = out;
        this.inConsumer = in;
    }
}

