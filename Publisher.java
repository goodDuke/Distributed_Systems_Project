import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.FileSystems;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Scanner;
import java.io.*;
import java.net.*;
import java.util.regex.Pattern;

public class Publisher extends Thread {
    private Broker b;
    private Socket requestSocketPublisher;
    private ObjectOutputStream outPublisher;
    private ObjectInputStream inPublisher;
    private int topicCode;
    private int id;
    private String[] path_split;

    public void run() {
        try {
            push(topicCode);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private void push(int topicCode) throws IOException {
        outPublisher.writeInt(topicCode); // 2P
        outPublisher.flush();
        Scanner s = new Scanner(System.in);
        System.out.println("Enter the path of the file: ");
        String path = s.nextLine();
        path_split = path.split(Pattern.quote(FileSystems.getDefault().getSeparator()));
        File file = new File(path);
        byte[] data = fileToByteArray(file);
        ArrayList<byte[]> chunks = createChunks(data);
        createInfoChunks(chunks.size());
        for (byte[] chunk: chunks) {
            outPublisher.writeObject(chunk); // 6P
            outPublisher.flush();
        }
    }

    private void createInfoChunks(int blockCount) throws IOException {
        byte[] file_name = path_split[path_split.length-1].getBytes(StandardCharsets.UTF_8);
        outPublisher.writeObject(file_name); // 3P
        outPublisher.flush();
        byte[] blockCountChunk = ByteBuffer.allocate(Integer.BYTES).putInt(blockCount).array();
        outPublisher.writeObject(blockCountChunk); // 4P
        outPublisher.flush();
        byte[] publisherId = ByteBuffer.allocate(Integer.BYTES).putInt(id).array();
        outPublisher.writeObject(publisherId); // 5P
        outPublisher.flush();
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

    Publisher(Broker b, int topicCode, Socket requestSocket,
              ObjectOutputStream out, ObjectInputStream in, int id) {
        this.b = b;
        this.topicCode = topicCode;
        this.requestSocketPublisher = requestSocket;
        this.outPublisher = out;
        this.inPublisher = in;
        this.id = id;
    }
}