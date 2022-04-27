/*
import java.io.*;
import java.net.Socket;

public class Consumer extends Thread{
    private Broker b;
    private Socket requestSocket;
    private ObjectOutputStream out;
    private ObjectInputStream in;
    private int topicCode;

    public void run() {
        try {
            pull(topicCode);
        } catch (IOException e) {
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

    private void recreateFile(int topicCode, int chunkCount) throws IOException {
        String filepath = ".\\src\\recreated_files\\mnm.txt";
        File file = new File(filepath);
        OutputStream stream = new FileOutputStream(file);
        byte[] completeFile = new byte[512 * 1024 * chunkCount];
        int i = 0;
        // Skip the first to chunks which contain the total chunkCount of the file and its
        int skipChunks = 2;
        for (byte[] chunk: queues.get(topicCode)) {
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
*/
