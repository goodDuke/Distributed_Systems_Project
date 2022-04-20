import java.io.*;
import java.net.*;

public class User extends Thread {
    Test t;
    User(Test t) {
        this.t = t;
    }

    public void run() {
        Socket requestSocket = null;
        ObjectOutputStream out = null;
        ObjectInputStream in = null;
        String ipAddress = "127.0.0.1";
        try {
            requestSocket = new Socket(ipAddress, t.number);
            System.out.println("Connected to host: " + ipAddress + " on port: " + t.number);
            out = new ObjectOutputStream(requestSocket.getOutputStream());
            in = new ObjectInputStream(requestSocket.getInputStream());

            out.writeObject(t);
            out.flush();

            Test res = (Test) in.readObject();
            //System.out.println("Server> " + res.getNumber() + " " + res.isFlag());
        } catch (UnknownHostException unknownHost) {
            System.err.println("You are trying to connect to an unknown host!");
        } catch (IOException ioException) {
            System.out.println("An error occurred while trying to connect to host: " + ipAddress + " on port: " +
                    t.number + ". Check the IP address and the port.");
            ioException.printStackTrace();
        } catch (ClassNotFoundException e) {
            //TODO auto-generated catch
            e.printStackTrace();
        } finally {
            try {
                in.close();
                out.close();
                requestSocket.close();
                System.out.println("Connection closed");
            } catch (IOException ioException) {
                ioException.printStackTrace();
            }
        }
    }

    public static void main(String args[]) {
        Test t = new Test(1100, false);
        Test t1 = new Test(1200, false);
        new User(t).start();
        new User(t1).start();
    }
}
