import java.io.File;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

public class SQLiteStorageSystem implements IStorageSystem {

    private String databaseName;

    /**
     * Constrctor for making a whole storage system.
     * Only need one SQLite database, and put files into there.
     */
    SQLiteStorageSystem(String dbName) {
        databaseName = dbName;
        prepareStorage();
        store(null);
        closeStorage();
    }

    /**
     * Creates a new SQL database file, with no tables in it.
     * 
     * @param databaseName
     */
    @Override
    public void prepareStorage() {

        // Creating a new database file (.db) with the given name for the whole
        // database, this is not a path (at the moment).
        File database = new File(databaseName + ".db");

        try {
            if (database.createNewFile()) {

            } else if (database.delete()) {
                // This is done to delete any previous instances of the database to create a
                // wholly new one.
                database.createNewFile();
            }
        } catch (IOException e) {
            System.out.println("Database file could not be created.");
        }

        // Creating the SQL database
        try {
            checkIfDBExists();
        } catch (SQLException e) {
            System.out.println("Database cannot be connected to.");
            e.printStackTrace();
        } catch (IOException e) {
            System.out.println("File cannot be created.");
            e.printStackTrace();
        }

    }

    /**
     * This connects to the database and calls the createTables method. It double
     * checks the database file has been created.
     * From CS1003 P3 (My submission)
     * 
     * @param databasePath
     * @throws SQLException
     * @throws IOException
     */
    private void checkIfDBExists() throws SQLException, IOException {

        // Check if a database file exists, the database file will be called DVLA.db
        Connection connection = null;

        try {

            // Connect to the Database Management System
            String dbUrl = "jdbc:sqlite:" + databaseName + ".db";
            connection = DriverManager.getConnection(dbUrl);

            // Create tables in the database
            // createTables(connection);

        } catch (SQLException e) {
            e.printStackTrace();
            System.out.println("Database cannot be connected to");
        } finally {
            // Regardless of whether an exception occurred above or not,
            // make sure we close the connection to the Database Management System
            if (connection != null) {
                connection.close();
            }
        }

    }

    /**
     * This method will store one data file as a table in the SQLDatabase file.
     * 
     * @param data
     */
    @Override
    public void store(byte[] data) {
        /**
         * Get the write times of each row during this time.
         */

        // Make a table with the name associated with the type of data being stored (the
        // size)

        StringBuilder sb = new StringBuilder();

        // TODO Need to come up with a format for table name
        String tableName = "";
        sb.append("DROP TABLE IF EXISTS " + tableName + ";");
        // TODO Table headings.
        sb.append("CREATE TABLE " + tableName + " ();");
    }

    /**
     * This method will read in the different tables to get the read time of
     * different data files, and then delete the table. Finally, the database file
     * itself will be deleted.
     */
    @Override
    public void closeStorage() {
        File database = new File(databaseName + ".db");

        if (database.delete()) {
            System.out.println("Clean up complete.");
        } else {
            System.out.println("Clean up failed.");
        }
    }

    /**
     * This method will give an analysis of the read-write times of different data files.
     */
    @Override
    public void reportAnalysis() {
        // TODO Auto-generated method stub

        Connection conn = null;
        String dbURL = "jdbc:sqlite" + databaseName + ".db";

        try {
            conn = DriverManager.getConnection(dbURL);

            /**
             * Get the read write times of the database
             */

        } catch (SQLException e) {
            System.out.println("Cannot connect to " + databaseName);
            e.printStackTrace();
        }

    }

}