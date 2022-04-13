import java.io.*;

public class ReadFromFile {
    public int getPort() {
        int port = -1;
        int newPort = -1;
        String replacement = "";
        String delete = "";

        try {
            // Read from the given file
            File file = new File("C:\\Users\\milto\\IdeaProjects\\Distributed_Systems\\txts\\ports.txt");
            File tempFile = File.createTempFile("file", ".txt", file.getParentFile());
            BufferedReader reader = new BufferedReader(new InputStreamReader(new FileInputStream(file)));
            PrintWriter pw = new PrintWriter(new OutputStreamWriter(new FileOutputStream(tempFile)));

            for (String line; (line = reader.readLine()) != null; ) {
                // Check whether the file data is compatible
                try {
                    if (port == -1) {
                        port = Integer.parseInt(line);
                        delete = line;
                        newPort = port + 1;
                        replacement = Integer.toString(newPort);
                    }

                    if (port > 1024) {
                        line = line.replace(delete, replacement);
                        pw.println(line);
                    }
                } catch (NumberFormatException e) {
                    System.out.println("Invalid input.");
                }
            }
            reader.close();
            pw.close();
            file.delete();
            tempFile.renameTo(file);
        } catch (IOException ioException) {
            ioException.printStackTrace();
        }
        if (port == -1)
            System.out.println("No available ports.");
        return port;
    }
}
