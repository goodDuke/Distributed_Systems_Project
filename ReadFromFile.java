import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Scanner;

public class ReadFromFile {
    // ArrayLists containing all the currently available ports, ips,
    // topics and registered users for each topic
    private ArrayList<Integer> availablePorts = new ArrayList<>();
    private ArrayList<String> availableIps = new ArrayList<>();
    private ArrayList<String> availableTopics = new ArrayList<>();
    private ArrayList<String[]> topicsAndUsers = new ArrayList<>();

    public ArrayList<Integer> getPorts() {
        try {
            // Read from the given file
            // TODO change the path
            File file = new File(".\\src\\txts\\ports.txt");
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
            File file = new File(".\\src\\txts\\ips.txt");
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
            // TODO change the path
            File file = new File(".\\src\\txts\\topics.txt");
            Scanner scanner = new Scanner(file);

            // Check if file contains any topics
            if (!scanner.hasNextLine()) {
                System.out.println("No topics given.");
                System.exit(-1);
            }

            // Save given topics in an ArrayList and the topics
            // with the registered users in a different ArrayList
            while (scanner.hasNextLine()) {
                String line = scanner.nextLine();
                topicsAndUsers.add(line.split("-"));
                availableTopics.add(topicsAndUsers.get(topicsAndUsers.size()-1)[0]);
            }
        } catch (IOException ioException) {
            ioException.printStackTrace();
        }
        return availableTopics;
    }

    public ArrayList<String[]> getTopicsAndUsers() {
        return topicsAndUsers;
    }
}
