import java.io.*;
import java.net.*;
import java.util.HashMap;

public class ActionsForUsers extends Thread {
    ObjectInputStream in;
    ObjectOutputStream out;

    public ActionsForUsers(Socket connection, HashMap<Integer, Integer> brokers, int[][] topics) {
        try {
            out = new ObjectOutputStream(connection.getOutputStream());
            in = new ObjectInputStream(connection.getInputStream());
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public void run() {
        try {
            Test body = (Test) in.readObject();
            body.setFlag(true);
            out.writeObject(body);
            out.flush();
        } catch (IOException e) {
            e.printStackTrace();
        } catch (ClassNotFoundException e){
            //TODO auto-generated catch
            e.printStackTrace();
        } finally {
            try {
                in.close();
                out.close();
            } catch (IOException ioException) {
                ioException.printStackTrace();
            }
        }
    }
}
