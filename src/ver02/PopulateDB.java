
/**
 * This class populates the data tables created in InitializeID.java with random data.
 * Successful population of the data tables prints "Database Populated."
 */

import java.io.*;
import java.security.SecureRandom;
import java.sql.*;
import java.util.*;
import java.text.*;
import java.util.concurrent.ThreadLocalRandom;

class PopulateDB {

    /**
     * Method Executes an SQL Query and returns a single string value
     * 
     * @param SQLquery [String] contains query in SQL format which interacts with
     *                 database when executed
     * @return String
     * @throws SQLException
     */
    public static String requestElement(Connection conn, String SQLquery) throws SQLException {

        String element = "";

        try {
            Statement statement = conn.createStatement();
            ResultSet results = statement.executeQuery(SQLquery);
            element = results.getString(1);

            if (statement != null) {
                statement.close();
            }

        } catch (SQLException e) {
            e.getMessage();
        }

        return element;
    }

    /**
     * Method Executes an SQL Query
     * 
     * @param SQLquery [String] contains query in sql format which interacts with
     *                 dtatabase when executed
     * @return [ArrayList<String>]
     * @throws SQLException
     */
    public static ArrayList<String> requestDetails(Connection conn, String SQLquery) throws SQLException {

        ArrayList<String> details = new ArrayList<String>();

        try {

            Statement statement = conn.createStatement();
            ResultSet results = statement.executeQuery(SQLquery);

            do {
                details.add(results.getString(1));
            } while (results.next());

            if (statement != null) {
                statement.close();
            }

        } catch (SQLException e) {
            e.getMessage();
        }

        return details;
    }

    /**
     * Update SQLite database with the write time of each row, since this is not an
     * available feature of the DB.
     * 
     * @param i [int] current record Index_ID
     * @throws SQLException
     * @throws ParseException
     */
    public static void appendWriteTime(Connection conn, int i) throws SQLException, ParseException {

        String previousTimeStamp = requestElement(conn,
                "SELECT time_stamp FROM NormalThings WHERE Index_ID < " + i + " ORDER BY Index_ID DESC LIMIT 1");
        String timeStamp = requestElement(conn,
                "SELECT time_stamp FROM NormalThings WHERE Index_ID < " + (i + 1) + " ORDER BY Index_ID DESC LIMIT 1");

        Double writeTime = getWriteTime(conn, timeStamp, previousTimeStamp);

        PreparedStatement statement = conn
                .prepareStatement("UPDATE NormalThings SET write_time=" + writeTime + " WHERE Index_ID=" + (i - 1));
        statement.executeUpdate();

        if (statement != null) {
            statement.close();
        }

    }

    /**
     * Calculating the time elapsed between writing of rows (in seconds) in the
     * SQLite Database.
     * 
     * @param timeStamp         [String] when record started writing
     * @param previousTimeStamp [String] when next record started writing
     * @return [Double] seconds elapsed since last write
     * @throws SQLException
     * @throws ParseException
     */
    public static Double getWriteTime(Connection conn, String timeStamp, String previousTimeStamp)
            throws SQLException, ParseException {

        String timeDiff = requestElement(conn,
                "SELECT (strftime('%f','" + timeStamp + "') - strftime('%f','" + previousTimeStamp + "'))");
        Double writeTime = Double.parseDouble(timeDiff);
        if (writeTime < 0)
            writeTime += 60.0;
        DecimalFormat df = new DecimalFormat("####0.000");
        writeTime = Double.valueOf(df.format(writeTime));

        return writeTime;

    }

    /**
     * Generating fake data to populate the SQLite DB with.
     * 
     * @throws SQLException
     * @throws ParseException
     */
    public static void appendNormalThings(Connection conn, int i) throws SQLException, ParseException {

        PreparedStatement statement = conn
                .prepareStatement("INSERT INTO NormalThings(write_time, random_int, blob) VALUES (?, ?, ?)");

        int writeTime = 0; // Placeholder
        int randomNum = ThreadLocalRandom.current().nextInt(0, dbLength + 1);

        // Generating Binary Blobs
        byte[] blob = new byte[blobSize]; // all zeros
        if (type == 1)
            new SecureRandom().nextBytes(blob); // random numbers (not crypto-safe)

        statement.setInt(1, writeTime); // Setting time since last write
        statement.setInt(2, randomNum); // Setting random integer
        statement.setBinaryStream(3, new ByteArrayInputStream(blob), blob.length); // Setting binary Blob
        // statement.setBlob(3, [buffer(zlib.compress(new
        // ByteArrayInputStream(blob)))]);

        statement.executeUpdate();

        // Closing statement if not null
        if (statement != null) {
            statement.close();
        }

    }

    /**
     * Method calls sub-methods which populate the database table
     * 
     * @param i [int] current record Index_ID
     * @return [boolean] only true when all databases have been successfully
     *         populated
     * @throws SQLException
     * @throws JSONException
     * @throws ParseException
     */
    public static boolean populateDB(Connection conn, int i) throws SQLException, ParseException {

        try {
            appendNormalThings(conn, i);
            return true;
        } catch (SQLException e) {
            e.getMessage();
            return false;
        }
    }

    /**
     * Iterating through DB rows in order to calculate and update the write time of
     * each row into the table.
     * 
     * @return [Boolean] indicating whether database has been successfully updated
     *         with all write times
     * @throws SQLException
     * @throws ParseException
     */
    public static boolean updateDB(Connection conn) throws SQLException, ParseException {

        try {
            int i = 2;
            for (; i < dbLength + 2; i++) {
                appendWriteTime(conn, i);
            }
            return true;

        } catch (SQLException e) {
            e.getMessage();
            return false;
        }
    }

    /**
     * A temporary additional row is inserted so as to calculate the write time of
     * the previous row.
     * It is then deleted.
     * 
     * @param i [int] current record Index_ID
     * @throws SQLException
     * @throws ParseException
     */
    public static void deleteLastRow(Connection conn, int i) throws SQLException, ParseException {

        PreparedStatement statement = conn.prepareStatement("DELETE FROM NormalThings "
                + "WHERE Index_ID=" + i + " AND write_time=0");
        statement.executeUpdate();

        if (statement != null) {
            statement.close();
        }

    }

    /**
     * Queries DB for its size in Bytes.
     * 
     * @return [int] Database Size in Bytes
     * @throws SQLException
     */
    public static int getDBSize(Connection conn) throws SQLException {

        int DBSize = 0;

        try {

            Statement statement = conn.createStatement();

            ResultSet DBSizeRequest = statement.executeQuery(
                    "SELECT page_count * page_size as size FROM pragma_page_count(), pragma_page_size();");
            DBSize = DBSizeRequest.getInt(1);

            if (statement != null) {
                statement.close();
            }

        } catch (SQLException e) {
            e.getMessage();
        }

        return DBSize;
    }

    /**
     * Queries DB for amount of rows in DB.
     * 
     * @return [int] Database rows #
     * @throws SQLException
     */
    public static int getDBLength(Connection conn) throws SQLException {

        int rows = 0;

        try {
            Statement statement = conn.createStatement();
            ResultSet results = statement
                    .executeQuery("SELECT Index_ID FROM NormalThings ORDER BY Index_ID DESC LIMIT 1");
            rows = results.getInt(1);

            if (statement != null) {
                statement.close();
            }

        } catch (SQLException e) {
            e.getMessage();
        }

        return rows;
    }

    /**
     * Queries DB for total time it took to write the Database.
     * 
     * @return [String] Total Write Time
     * @throws SQLException
     */
    public static String getTotalWriteTime(Connection conn) throws SQLException {

        String start = requestElement(conn, "SELECT time_stamp AS start FROM NormalThings WHERE Index_ID=1");
        String end = requestElement(conn, "SELECT time_stamp AS end FROM NormalThings ORDER BY Index_ID DESC LIMIT 1");

        int H = Integer.parseInt(
                requestElement(conn, "SELECT (strftime('%H', '" + end + "') - strftime('%H', '" + start + "'))"));
        int M = Integer.parseInt(
                requestElement(conn, "SELECT (strftime('%M', '" + end + "') - strftime('%M', '" + start + "'))"));
        double s = Double.valueOf(
                requestElement(conn, "SELECT (strftime('%f', '" + end + "') - strftime('%f', '" + start + "'))"));

        // Avoiding negative seconds
        if (s < 0) {
            s += 60.0;
            if (M > 0)
                M -= 1;
        }

        // Avoiding negative minutes
        if (M < 0) {
            M += 60.0;
            if (H > 0)
                M -= 1;
        }

        DecimalFormat df = new DecimalFormat("####0.000");
        s = Double.valueOf(df.format(s));

        writeTimeSeconds = H * 3600 + M * 60 + s;

        String h = String.valueOf(H);
        String m = String.valueOf(M);
        String S = String.valueOf(s);

        if (H == 0)
            h = "00";
        if (M == 0)
            m = "00";
        if (s == 0)
            S = "00.000";

        return h + ":" + m + ":" + S;// totalWriteTime;
    }

    /**
     * Queries DB for total time it took to write the Database.
     * 
     * @return [String] Total Write Time
     * @throws SQLException
     */
    public static Double minWriteTime(Connection conn) throws SQLException {

        String request = requestElement(conn, "SELECT MIN(write_time) FROM NormalThings");
        Double min = Double.valueOf(request);

        return min;
    }

    /**
     * Queries DB for total time it took to write the Database.
     * 
     * @return [String] Total Write Time
     * @throws SQLException
     */
    public static Double maxWriteTime(Connection conn) throws SQLException {

        String request = requestElement(conn, "SELECT MAX(write_time) FROM NormalThings");
        Double max = Double.valueOf(request);

        return max;
    }

    /**
     * Queries DB for the most common write time in the Database.
     * 
     * @return [Double] Most Common Write Write Time
     * @throws SQLException
     */
    public static Double mostCommonWT(Connection conn) throws SQLException {

        String request = requestElement(conn, "SELECT COUNT(write_time) AS `val_occurrence` FROM NormalThings "
                + "GROUP BY write_time ORDER BY `val_occurrence` DESC LIMIT 1");
        Double writeTime = Double.valueOf(request);

        return writeTime;
    }

    /**
     * Queries DB for number of occurences of the most common write time in the
     * Database.
     * 
     * @param writeTime [Double] write time being filtered by
     * @return [Double] Most Common Write Write Time
     * @throws SQLException
     */
    public static int mostCommonWTOccurences(Connection conn, Double writeTime) throws SQLException {

        String request = requestElement(conn, "SELECT COUNT(write_time) FROM "
                + "NormalThings WHERE write_time=" + writeTime);
        int occurences = Integer.parseInt(request);

        return occurences;
    }

    public static Connection conn = null;
    private static int dbLength = 0;
    private static int blobSize = 0;
    private static int type = 0;
    private static Double writeTimeSeconds = 0D;

    /**
     * MAIN METHOD
     */
    public static void main(String[] args) throws SQLException, IOException {

        int[] blobSizes = { 100, 1000, 10000 };
        int[] dbLengths = { 100, 1000, 10000 };
        int[] blobTypes = { 0, 1 };
        Connection conn = null;

        // For DB of varying amount of rows
        for (int l = 0; l < dbLengths.length; l++) {
            // For DB of varying blob sizes
            for (int s = 0; s < blobSizes.length; s++) {
                // For DB of different blob types
                for (int t = 0; t < blobTypes.length; t++) {

                    dbLength = dbLengths[l];
                    blobSize = blobSizes[s];
                    type = blobTypes[t];

                    String blobType = "Zeros";
                    if (type == 1)
                        blobType = "Random";

                    int runs = 50; // Run 50 Times
                    for (int i = 33; i < runs; i++) {

                        String fileName = "Rows-" + dbLength + "_BlobSize-" + blobSize + "_BlobType-" + blobType + "_"
                                + i + ".db";

                        try {
                            // Establishes online connection
                            String url = "jdbc:sqlite:" + fileName;
                            conn = DriverManager.getConnection(url);

                            // Populate Database
                            int d = 0;
                            for (; d < dbLength + 1; d++) {
                                populateDB(conn, d);
                            }
                            updateDB(conn); // Update Write Times
                            deleteLastRow(conn, getDBLength(conn));

                            String dbSize = new DecimalFormat("#,###").format(getDBSize(conn));

                            String writeTime = getTotalWriteTime(conn);
                            Double minWriteTime = minWriteTime(conn);
                            Double maxWriteTime = maxWriteTime(conn);
                            Double mostCommonWT = mostCommonWT(conn);
                            int mostCommonWTOccurences = mostCommonWTOccurences(conn, mostCommonWT);
                            Double avrgWriteTime = writeTimeSeconds / getDBLength(conn);
                            DecimalFormat df = new DecimalFormat("####0.000");
                            avrgWriteTime = Double.valueOf(df.format(avrgWriteTime));

                            if (d == dbLength + 1) {
                                System.out.println("Database Populated: Run " + (i + 1)
                                        + "\n                    " + dbSize + " Bytes"
                                        + "\n                    " + blobSize + " Bytes / " + blobType + " Binary Blob"
                                        + "\n                    " + getDBLength(conn) + " Lines"
                                        + "\n        Write time: " + writeTime + "  (Average " + avrgWriteTime + " s)"
                                        + "\n                                 (Minimum " + minWriteTime + " s)"
                                        + "\n                                 (Maximum " + maxWriteTime + " s)"
                                        + "\n                                 (Most Common " + mostCommonWT + " s -> "
                                        + mostCommonWTOccurences + "/" + dbLength + " writes)\n");
                            } else {
                                System.out.println("Unable to Populate Database.");
                            }

                        } catch (SQLException e) {
                            e.printStackTrace();
                        } catch (ParseException e) {
                            e.printStackTrace();

                        } finally {
                            if (conn != null)
                                conn.close();
                        }
                    }
                }
            }
        }
    }
}
