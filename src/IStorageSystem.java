/**
 * This is the interface for either a SQL database structure, or a structure using in built Java reading and writing methods.
 */

public interface IStorageSystem {

    /**
     * Creating database or blank file for FileOutputStream.
     */
    void prepareStorage();

    /**
     * Putting data into the database/blank file created in prepareStorage.
     * This should keep track of the amount of time it takes to write the data passed through as an argument.
     */
    void store(byte[] data);

    /**
     * Cleans up by deleting the huge amounts of data created earlier.
     */
    void closeStorage();

    /**
     * Reports the read and write times of the data.
     */
    void reportAnalysis();

}
