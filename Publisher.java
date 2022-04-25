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
    private String topicString;
    private boolean firstConnection = true;
    private boolean changedBroker = false;

    public static void main(String args[]) throws InterruptedException {
        Broker b1 = new Broker("127.0.0.1", 1100);
        Broker b2 = new Broker("127.0.0.1", 1200);
        new Publisher(b1).start();
        Thread.sleep(500);
        //new Publisher(b2).start();
    }

    public void run() {
        try {
            requestSocket = new Socket(b.getIp(), b.getPort());
            System.out.println("Connected to broker: " + b.getIp() + " on port: " + b.getPort());
            topicCode = getTopic();
            out = new ObjectOutputStream(requestSocket.getOutputStream());
            in = new ObjectInputStream(requestSocket.getInputStream());

            out.writeBoolean(firstConnection);
            out.flush();

            out.writeInt(topicCode);
            out.flush();
            // Get broker object which contains the requested topic
            Broker matchedBroker = (Broker) in.readObject();

            if (matchedBroker == null)
                System.out.println("The topic \"" + topicString + "\" doesn't exist.");
            else
                connectToMatchedBroker(matchedBroker);

            push(topicCode);

        } catch (UnknownHostException unknownHost) {
            System.err.println("You are trying to connect to an unknown host!");
        } catch (IOException ioException) {
            System.out.println("An error occurred while trying to connect to host: " + b.getIp() + " on port: " +
                    b.getPort() + ". Check the IP address and the port.");
            ioException.printStackTrace();
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
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

    // Create a hash code for the given topic
    private int getTopic() {
        Scanner s = new Scanner(System.in);
        System.out.println("Enter the topic for port " + b.getPort() + ": ");
        topicString = s.nextLine();

        int code = topicString.hashCode();
        return code;
    }

    // Check if the current broker is the correct one
    // Otherwise close the current connection and connect to the right one
    private void connectToMatchedBroker(Broker matchedBroker) throws IOException{
        if (!Objects.equals(b.getIp(), matchedBroker.getIp()) || !Objects.equals(b.getPort(), matchedBroker.getPort())) {
            changedBroker = true;
            out.writeObject(changedBroker);
            out.flush();
            in.close();
            out.close();
            requestSocket.close();
            System.out.println("Connection to broker: " + b.getIp() + " on port: " + b.getPort() + " closed");
            b = matchedBroker;
            requestSocket = new Socket(b.getIp(), b.getPort());
            System.out.println("Connected to broker: " + b.getIp() + " on port: " + b.getPort());
            out = new ObjectOutputStream(requestSocket.getOutputStream());
            in = new ObjectInputStream(requestSocket.getInputStream());
            firstConnection = false;
            out.writeBoolean(firstConnection);
            out.flush();
        } else {
            out.writeObject(changedBroker);
            out.flush();
        }
    }

    private void push(int topicCode) throws IOException {
        out.writeInt(topicCode);
        out.flush();
        Scanner s = new Scanner(System.in);
        System.out.println("Enter the path of the file: ");
        String path = s.nextLine();
        File file = new File(path);
        byte[] data = fileToByteArray(file);
        ArrayList<byte[]> chunks = createChunks(data);
        for (byte[] chunk: chunks) {
            out.writeObject(chunk);
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
        out.writeInt(blockCount);
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

    Publisher(Broker b) {
        this.b = b;
    }
}
