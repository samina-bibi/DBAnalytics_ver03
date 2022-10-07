import java.io.*;
import java.nio.file.*;
import java.sql.*;
import java.util.*;

/**
 * Initialise Database.
 */
public class InitializeDB {
    public static void main(String[] args) throws SQLException, IOException {

        int[] blobSizes = { 100, 1000, 10000 };
        int[] dbLengths = { 100, 1000, 10000 };
        int[] blobTypes = { 0, 1 };
        //Connection conn = null;

        // For DB of varying amount of rows
        for (int l = 0; l < dbLengths.length; l++) {
            // For DB of varying blob sizes
            for (int s = 0; s < blobSizes.length; s++) {
                // For DB of different blob types
                for (int t = 0; t < blobTypes.length; t++) {

                    int blobSize = blobSizes[s];
                    int dbLength = dbLengths[l];
                    int type = blobTypes[t];

                    String blobType = "Zeros";
                    if (type == 1)
                        blobType = "Random";

                    int runs = 50; // Run 50 Times
                    for (int i = 0; i < runs; i++) {

                        String fileName = "Rows-" + dbLength + "_BlobSize-" + blobSize + "_BlobType-" + blobType + "_"
                                + i + ".db";

                        // // Create Directory if it does not already exist
                        // String newDir =
                        // ".\\src\\main\\java\\org\\apache\\maven\\mavenproject\\databases\\"+dbLength+"_"+blobSize+"_"+blobType+"\\";
                        // Path dirPath = Paths.get(newDir);
                        // if (!Files.exists(dirPath)){
                        // Files.createDirectory(dirPath);
                        // }

                        // Path path = Paths.get(newDir+fileName);
                        Path path = Paths.get(".\\" + fileName);
                        // Delete file if it already exists.
                        if (Files.exists(path)) {
                            Files.delete(path);
                        }
                        //Path file = Files.createFile(path);

                        Scanner sc;
                        Connection connection = null;
                        String txtFile = "src/../DDL.txt";

                        // Access DDL file
                        try {
                            sc = new Scanner(new File(txtFile));
                        } catch (FileNotFoundException e) {
                            System.out.println(
                                    "File not found: " + txtFile.substring(txtFile.lastIndexOf("\\") + 1).trim());
                            return;
                        }

                        try {
                            // Establish connection and create a statement
                            String url = "jdbc:sqlite:" + fileName;
                            connection = DriverManager.getConnection(url);
                            Statement statement = connection.createStatement();

                            // Execute each like of the txtFile as an SQL update
                            while (sc.hasNextLine()) {
                                statement.executeUpdate(sc.nextLine());
                            }

                            // Close Scanner
                            sc.close();
                            statement.close();
                            System.out.println("Database Initialized: " + fileName);

                        } catch (SQLException e) {
                            System.out.println(e.getMessage());

                        } finally {
                            // Close connection
                            if (connection != null)
                                connection.close();
                        }
                    }
                }
            }
        }
    }
}