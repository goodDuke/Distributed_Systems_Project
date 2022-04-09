import java.io.*;
import java.net.*;

public class Client extends Thread {

//    int a, b;
//    Client(int a, int b) {
//        this.a = a;
//        this.b = b;
//    }

    Test t;
    Client(Test t){
        System.out.println("Connection done"+ t.getNumber() + " "+ t.isFlag());
        this.t=t;
    }

    public void run() {
        Socket requestSocket = null;
        ObjectOutputStream out = null;
        ObjectInputStream in = null;
        try {
            requestSocket = new Socket("127.0.0.1", 4321);
            out = new ObjectOutputStream(requestSocket.getOutputStream());
            in = new ObjectInputStream(requestSocket.getInputStream());

//            out.writeInt(a);
//            out.flush();

//            out.writeInt(b);
            //           out.flush();

            out.writeObject(t);
            out.flush();

            Test res = (Test) in.readObject();
            System.out.println("Server>" + res.getNumber() + " "+ res.isFlag());

        } catch (UnknownHostException unknownHost) {
            System.err.println("You are trying to connect to an unknown host!");
        } catch (IOException ioException) {
            ioException.printStackTrace();
        }catch (ClassNotFoundException e){
            //TODO auto-generated catch
            e.printStackTrace();
        }
        finally {
            try {
                in.close();	out.close();
                requestSocket.close();
            } catch (IOException ioException) {
                ioException.printStackTrace();
            }
        }
    }

    public static void main(String args[]) {
        Test t= new Test(3,false);
        new Client(t).start();
//      new Client(10, 5).start();
//      new Client(20, 5).start();
//      new Client(30, 5).start();
//      new Client(40, 5).start();
//      new Client(50, 5).start();
//      new Client(60, 5).start();
//      new Client(70, 5).start();
//      new Client(80, 5).start();
//      new Client(90, 5).start();
//      new Client(100, 5).start();
    }
