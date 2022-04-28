import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Scanner;
import java.io.*;
import java.net.*;

public class Publisher extends Thread {
    private Broker b;
    private Socket requestSocket;
    private ObjectOutputStream out;
    private ObjectInputStream in;
    private int topicCode;

    public void run() {
        try {
            push(topicCode);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private void push(int topicCode) throws IOException {
        out.writeInt(topicCode); // 10
        out.flush();
        Scanner s = new Scanner(System.in);
        System.out.println("Enter the path of the file: ");
        //String path = s.nextLine();
        String path = ".\\src\\data\\mnm.txt";
        File file = new File(path);
        byte[] data = fileToByteArray(file);
        ArrayList<byte[]> chunks = createChunks(data);
        createInfoChunks(chunks.size());
        for (byte[] chunk: chunks) {
            out.writeObject(chunk); // 13
            out.flush();
        }
    }

    private void createInfoChunks(int blockCount) throws IOException {
        Scanner s = new Scanner(System.in);
        String fileExtension = null;
        do {
            System.out.println("Press 1 for '.txt' file\n" +
                    "Press 2 for '.jpg' file\n" +
                    "Press 3 for '.mp4' file");
            String fileExtensionNum = s.nextLine();
            switch (fileExtensionNum) {
                case "1":
                    fileExtension = ".txt";
                    break;
                case "2":
                    fileExtension = ".jpg";
                    break;
                case "3":
                    fileExtension = ".mp4";
                    break;
                default:
                    System.out.println("Wrong number entered. Please try again.");
            }
        } while (fileExtension == null);
        byte[] extension = fileExtension.getBytes(StandardCharsets.UTF_8);
        out.writeObject(extension); // 11
        out.flush();
        byte[] chunkCount = ByteBuffer.allocate(Integer.BYTES).putInt(blockCount).array();
        out.writeObject(chunkCount); // 12
        out.flush();
    }

    // Convert file to byte array
    private byte[] fileToByteArray(File file) throws IOException {
        FileInputStream fl = new FileInputStream(file);
        byte[] data = new byte[(int)file.length()];
        fl.read(data);
        fl.close();
        return data;
    }

    // From the byte array create the chunks to be sent to the broker
    private ArrayList<byte[]> createChunks(byte[] data) throws IOException {
        int blockSize = 512 * 1024;
        ArrayList<byte[]> listOfChunks = new ArrayList<>();
        int blockCount = (data.length + blockSize - 1) / blockSize;
        byte[] chunk;
        int start;
        for (int i = 1; i < blockCount; i++) {
            start = (i - 1) * blockSize;
            chunk = Arrays.copyOfRange(data, start, start + blockSize);
            listOfChunks.add(chunk);
        }

        int end;
        if (data.length % blockSize == 0) {
            end = data.length;
        } else {
            end = data.length % blockSize + blockSize * (blockCount - 1);
        }
        chunk = Arrays.copyOfRange(data, (blockCount - 1) * blockSize, end);
        listOfChunks.add(chunk);
        return listOfChunks;
    }

    Publisher(Broker b, int topicCode, Socket requestSocket, ObjectOutputStream out, ObjectInputStream in) {
        this.b = b;
        this.topicCode = topicCode;
        this.requestSocket = requestSocket;
        this.out = out;
        this.in = in;
    }
}