import java.io.*;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;

public class Consumer extends Thread implements Serializable {
    private String brokerIp;
    private int brokerPort;
    private Socket requestSocketConsumer;
    private ObjectOutputStream outConsumer;
    private ObjectInputStream inConsumer;
    private int topicCode;
    private ArrayList<byte[]> history = new ArrayList<>();
    private int pointerChunk;

    public void run() {
        try {
            receiveAllData();
            while (!Thread.currentThread().isInterrupted()) {
                receiveData();
            }
        } catch (IOException | ClassNotFoundException e) {
            e.printStackTrace();
        }
    }

    // Collecting the chunks for a specific file and adding them to the correct ArrayList
    private void receiveData() throws IOException, ClassNotFoundException {
        pointerChunk = history.size();
        System.out.println("\033[3mWaiting new message!\033[0m");
        byte[] fileNameChunk = (byte[]) inConsumer.readObject(); // 4.1C
        if (fileNameChunk != null)
            history.add(fileNameChunk);
        byte[] blockCountChunk = (byte[]) inConsumer.readObject(); // 4.2C
        if (blockCountChunk != null)
            history.add(blockCountChunk);
        byte[] publisherId = (byte[]) inConsumer.readObject(); // 4.3C
        if (publisherId != null)
            history.add(publisherId);

        int blockCount = 0;
        if (fileNameChunk != null) {
            // Converting blockCount to integer
            for (byte b : blockCountChunk)
                blockCount += b;

            // Saving chunks in the corresponding ArrayList
            for (int i = 1; i <= blockCount; i++) {
                byte[] chunk = (byte[]) inConsumer.readObject(); // 4.4C
                history.add(chunk);
            }
            recreateFile(blockCount, fileNameChunk);
            System.out.println("\033[3mNew message fetched successfully!\033[0m");
        }
    }

    private void recreateFile(int blockCount, byte[] fileNameChunk) throws IOException {
        String fileName = new String(fileNameChunk, StandardCharsets.UTF_8);
        // TODO change path
        String filepath = "D:\\DS\\app\\src\\recreated_files\\" + fileName;
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
        if (!isEmpty) {
            System.out.println("\033[3mFetching history!\033[0m");
            int totalChunks = inConsumer.readInt();  // 2C
            for (int i = 1; i <= totalChunks; i++) {
                byte[] chunk = (byte[]) inConsumer.readObject(); // 3C
                history.add(chunk);
            }
            recreateAllData();
            System.out.println("\033[3mHistory fetched successfully!\033[0m");
        }
    }

    private void recreateAllData() throws IOException {
        int currentChunk = 0;
        while (currentChunk < history.size()) {
            String fileName = new String(history.get(currentChunk), StandardCharsets.UTF_8);
            // TODO change path
            String filepath = "D:\\DS\\app\\src\\recreated_files\\" + fileName;
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

    Consumer(String ip, int port, int topicCode, Socket requestSocket, ObjectOutputStream out, ObjectInputStream in) {
        this.brokerIp = ip;
        this.brokerPort = port;
        this.topicCode = topicCode;
        this.requestSocketConsumer = requestSocket;
        this.outConsumer = out;
        this.inConsumer = in;
    }
}