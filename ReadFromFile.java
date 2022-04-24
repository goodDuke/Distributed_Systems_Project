import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Scanner;

public class ReadFromFile {
    // ArrayLists containing all the currently available ports, ips and topics
    private ArrayList<Integer> availablePorts = new ArrayList<Integer>();
    private ArrayList<String> availableIps = new ArrayList<String>();
    private ArrayList<String> availableTopics = new ArrayList<String>();

    public ArrayList<Integer> getPorts() {
        try {
            // Read from the given file
            // TODO change the path
            File file = new File("C:\\Users\\elias\\IdeaProjects\\Distributed_Systems\\src\\txts\\ports.txt");
            Scanner scanner = new Scanner(file);

            // Check if file contains any ports
            if (!scanner.hasNextLine()) {
                System.out.println("No ports given.");
                System.exit(-1);
            }

            // Save given ports in an ArrayList
            while (scanner.hasNextLine()) {
                // Check whether the file data is compatible
                try {
                    int port = Integer.parseInt(scanner.nextLine());
                    if (port > 1024 && !availablePorts.contains(port)) {
                        availablePorts.add(port);
                    }
                    else
                        System.out.println("The port " + port + " isn't available for use.");
                } catch (NumberFormatException e) {
                    System.out.println("Invalid input.");
                }
            }
        } catch (IOException ioException) {
            ioException.printStackTrace();
        }
        return availablePorts;
    }

    public ArrayList<String> getIps() {
        try {
            // Read from the given file
            // TODO change the path
            File file = new File("C:\\Users\\elias\\IdeaProjects\\Distributed_Systems\\src\\txts\\ips.txt");
            Scanner scanner = new Scanner(file);

            // Check if file contains any ips
            if (!scanner.hasNextLine()) {
                System.out.println("No ips given.");
                System.exit(-1);
            }

            // Save given ips in an ArrayList
            while (scanner.hasNextLine()) {
                try {
                    String ip = scanner.nextLine();
                    availableIps.add(ip);
                } catch (NumberFormatException e) {
                    System.out.println("Invalid input.");
                }
            }
        } catch (IOException ioException) {
            ioException.printStackTrace();
        }
        return availableIps;
    }

    public ArrayList<String> getTopics() {
        try {
            // Read from the given file
            // TODO change the path~
            File file = new File("C:\\Users\\elias\\IdeaProjects\\Distributed_Systems\\src\\txts\\topics.txt");
            Scanner scanner = new Scanner(file);

            // Check if file contains any topics
            if (!scanner.hasNextLine()) {
                System.out.println("No topics given.");
                System.exit(-1);
            }

            // Save given topics in an ArrayList
            while (scanner.hasNextLine()) {
                String topic = scanner.nextLine();
                availableTopics.add(topic);
            }
        } catch (IOException ioException) {
            ioException.printStackTrace();
        }
        return availableTopics;
    }
}
