
/**
 * FileStream class - uses the FileStream class
 */

import java.io.*;
import java.sql.*;
import java.text.*;
import java.util.*;
import java.util.Date;
import java.util.concurrent.ThreadLocalRandom;

class FileStream {

    // /**
    // * Gets ID of the last row updated.
    // * @param r [BufferedReader]
    // * @throws SQLException
    // */
    // public static int rows(BufferedReader r) throws IOException {
    //
    // String lastLine = "";
    // String line = "";
    //
    // while ((line = r.readLine()) != null) {
    // lastLine = line;
    // }
    //
    // System.out.println(lastLine.indexOf(0));
    // return lastLine.indexOf(0);
    // }

    // /**
    // * Calculates write time.
    // * @param then [Date]
    // * @param then [Date]
    // */
    // public static Double getWriteTime(Date then, Date now) {
    //
    // long diffInMillies = now.getTime() - then.getTime();
    // return Double.valueOf(diffInMillies);
    // }

    /**
     * Calculates write time.
     * 
     * @param then [Date]
     * @param then [Date]
     */
    public static Long getWriteTime(Long then, Long now) {

        return now - then;
    }

    public static Connection conn = null;
    private static int fileLength = 0;
    private static int blobSize = 0;
    private static int type = 0;

    /**
     * MAIN METHOD
     */
    public static void main(String[] args) throws IOException {

        int[] blobSizes = { 100, 1000, 10000 };
        int[] fileLengths = { 100, 1000, 10000 };
        int[] blobTypes = { 0, 1 };

        try (PrintWriter writer = new PrintWriter("FileStream.csv")) {
            StringBuilder sb = new StringBuilder();

            // Set CSV File headers
            sb.append("Run , DB Rows , Blob Size (Bytes) , Blob Type , DB Size (Bytes) , Total Write Time\n");
            writer.write(sb.toString());

            sb = new StringBuilder();

            int id = 1;
            // For DB of varying amount of rows
            for (int l = 0; l < fileLengths.length; l++) {
                // For DB of varying blob sizes
                for (int s = 0; s < blobSizes.length; s++) {
                    // For DB of different blob types
                    for (int t = 0; t < blobTypes.length; t++) {

                        fileLength = fileLengths[l];
                        blobSize = blobSizes[s];
                        type = blobTypes[t];

                        String blobType = "Zeros";
                        if (type == 1)
                            blobType = "Random";

                        // CSV File
                        String fileName = "Rows-" + fileLength + "_BlobSize-" + blobSize + "_BlobType-" + blobType;

                        // int runs = 50; // Run 50 Times
                        // for (int i=0; i<runs; i++) {

                        String datFile = fileName + ".dat"; // + "_" + i + ".dat"

                        Date date = new Date();
                        Long start = 0L;
                        Long end = 0L;

                        try (DataOutputStream out = new DataOutputStream(
                                new BufferedOutputStream(new FileOutputStream("src/../" + datFile)))) {
                            out.writeChars("Index_ID , ");
                            out.writeChars("time_stamp , ");
                            out.writeChars("random_int , ");
                            out.writeChars("blob , ");
                            out.writeChars("write_time \n");

                            for (int r = 0; r < fileLength; r++) {

                                date = new Date();
                                SimpleDateFormat dateformat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
                                String timeStamp = dateformat.format(date);
                                int randomNum = ThreadLocalRandom.current().nextInt(0, fileLength + 1);
                                start = System.currentTimeMillis();

                                out.writeInt(id);
                                out.writeChars(" , ");
                                out.writeChars(timeStamp);
                                out.writeChars(" , ");
                                out.writeInt(randomNum);
                                out.writeChars(" , ");

                                for (int b = 0; b < blobSize * 2; b++) {
                                    if (type == 1) {
                                        int rand = ThreadLocalRandom.current().nextInt(0, blobSize * 2 + 1);
                                        out.writeByte(rand);
                                    } else
                                        out.writeByte(0);
                                }

                                out.writeChars(" , ");
                                out.writeLong(getWriteTime(start, System.currentTimeMillis()));
                                out.writeChars(" \n");
                            }

                        } catch (FileNotFoundException ex) {
                            System.out.println("Cannot Open the Output File");
                            return;
                        }

                        long size = 0L;
                        int chunk = 0;
                        end = System.currentTimeMillis();
                        // Reading the data back using DataInputStream
                        try (DataInputStream in = new DataInputStream(new FileInputStream("src/../" + datFile))) {

                            byte[] buffer = new byte[1024];
                            while ((chunk = in.read(buffer)) != -1) {
                                size += chunk;
                            }

                            // for (int o=0; o<1000; o++) {
                            // System.out.print(in.readChar());
                            // }
                            Long totWT = getWriteTime(start, end);

                            sb.append(id);
                            sb.append(" , ");
                            sb.append(fileLength); // Append # of Rows in DB
                            sb.append(" , ");
                            sb.append(blobSize); // Append blob Size
                            sb.append(" , ");
                            sb.append(blobType); // Append binary Blob Type (zeros, random)
                            sb.append(" , ");
                            sb.append(size); // Append Size of DB in Bytes
                            sb.append(" , ");
                            sb.append(totWT); // Total Write Time
                            sb.append('\n');

                            // BufferedReader r = new BufferedReader(in);
                            if (size > 112) {
                                System.out.println("Data File Loaded: " + datFile);
                            } else {
                                System.out.println("Unable to Load Data File.");
                            }

                        } catch (FileNotFoundException e) {
                            System.out.println("Cannot Open the Input File");
                            return;
                        }
                        // }
                        id++;
                    }
                }
            }

            // Write data as a line on the CSV file
            writer.write(sb.toString());
            System.out.println("\nCSV Created:");
            System.out.print(sb.toString());
            System.out.println("Done!\n");

        } catch (FileNotFoundException e) {
            System.out.println(e.getMessage());
        }
    }
}