import java.util.ArrayList;
import java.util.Arrays;
import java.util.Objects;
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
            out = new ObjectOutputStream(requestSocket.getOutputStream());
            in = new ObjectInputStream(requestSocket.getInputStream());
            push(topicCode);
        } catch (UnknownHostException unknownHost) {
            System.err.println("You are trying to connect to an unknown host!");
        } catch (IOException ioException) {
            System.out.println("An error occurred while trying to connect to host: " + b.getIp() + " on port: " +
                    b.getPort() + ". Check the IP address and the port.");
            ioException.printStackTrace();
        } finally {
            try {
                in.close();
                out.close();
                requestSocket.close();
                System.out.println("Connection to broker: " + b.getIp() + " on port: " + b.getPort() + " closed");
            } catch (IOException ioException) {
                ioException.printStackTrace();
            }
        }
    }

    private void push(int topicCode) throws IOException {
        out.writeInt(topicCode); // 5
        out.flush();
        Scanner s = new Scanner(System.in);
        System.out.println("Enter the path of the file: ");
        //String path = s.nextLine();
        String path = ".\\src\\data\\video.mp4";
        File file = new File(path);
        byte[] data = fileToByteArray(file);
        ArrayList<byte[]> chunks = createChunks(data);
        for (byte[] chunk: chunks) {
            out.writeObject(chunk); // 7
            out.flush();
        }
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
        out.writeInt(blockCount); // 6
        out.flush();
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

    Publisher(Broker b, int topicCode, Socket requestSocket) {
        this.b = b;
        this.topicCode = topicCode;
        this.requestSocket = requestSocket;
    }
}
