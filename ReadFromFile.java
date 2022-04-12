import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Scanner;


public class ReadFromFile {
    // ArrayList containing all the currently available ports
    private ArrayList<Integer> availablePorts = new ArrayList<Integer>();

    private ArrayList<Integer> getPortsFromFile() {
        try {
            // Read from the given file
            File file = new File("C:\\Users\\milto\\IdeaProjects\\Distributed_Systems\\txts\\ports.txt");
            Scanner scanner = new Scanner(file);

            // Check if file contains any ports
            if (!scanner.hasNextLine()) {
                System.out.println("No ports given.");
                System.exit(-1);
            }

            // Save given ports in a HashMap
            while (scanner.hasNextLine()) {
                // Check whether the file data is compatible
                try {
                    int port = Integer.parseInt(scanner.nextLine());
                    if (port > 1024 && !availablePorts.contains(port))
                        availablePorts.add(port);
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

    public int getPort() {
        // The first the 'getPort' function is called
        // the .txt file containing the ports is used
        if (availablePorts.isEmpty())
            availablePorts = getPortsFromFile();

        // If all ports have been used the broker closes the connection
        if (!availablePorts.isEmpty()) {
            int port = availablePorts.remove(0);
            return port;
        }
        else
            return -1;
    }

    // Add the released port back to the 'availablePorts' ArrayList
    void releasePort(int port) {
        availablePorts.add(port);
    }
}
