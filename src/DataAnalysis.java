import java.io.*;
import java.math.*;
import java.sql.*;
import java.text.*;

/**
 * Main method class to read and analyse the SQLite database given.
 */
public class DataAnalysis {

	/**
	 * Method Executes an SQL Query and returns a single string value
	 * 
	 * @param conn     [Connection] to establish online connection to database
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
	 * Queries DB for its size in Bytes.
	 * 
	 * @param conn [Connection] to establish online connection to database
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
	 * @param conn [Connection] to establish online connection to database
	 * @return [int] Database rows
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
	 * @param conn [Connection] to establish online connection to database
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
	 * Queries DB for minimumum write time in the Database.
	 * 
	 * @param conn [Connection] to establish online connection to database
	 * @return [Double] Min Write Time
	 * @throws SQLException
	 */
	public static Double getMinWriteTime(Connection conn) throws SQLException {

		String request = requestElement(conn, "SELECT MIN(write_time) FROM NormalThings");
		Double min = Double.valueOf(request);

		return min;
	}

	/**
	 * Queries DB for maximum write time in the Database.
	 * 
	 * @param conn [Connection] to establish online connection to database
	 * @return [Double] Max Write Time
	 * @throws SQLException
	 */
	public static Double getMaxWriteTime(Connection conn) throws SQLException {

		String request = requestElement(conn, "SELECT MAX(write_time) FROM NormalThings");
		Double max = Double.valueOf(request);

		return max;
	}

	/**
	 * Queries DB for the most common write time in the Database.
	 * 
	 * @param conn [Connection] to enable the establishing online connection to
	 *             database
	 * @return [Double] Most Common Write Write Time
	 * @throws SQLException
	 */
	public static Double getMostCommonWT(Connection conn) throws SQLException {

		String request = requestElement(conn, "SELECT write_time, COUNT(write_time) AS `val_occurrence` "
				+ "FROM NormalThings GROUP BY write_time ORDER BY `val_occurrence` DESC LIMIT 1");
		Double writeTime = Double.valueOf(request);

		return writeTime;
	}

	/**
	 * Queries DB for the most common write time occurrence in the Database.
	 * 
	 * @param conn [Connection] to enable the establishing online connection to
	 *             database
	 * @return [int] Most Common Write Write Time occurrence
	 * @throws SQLException
	 */
	public static int getMaxOccurrence(Connection conn, int dbLength) throws SQLException {

		String request = requestElement(conn,
				"SELECT COUNT(write_time) AS `max_occurrence` FROM NormalThings WHERE write_time>0.045"); // 0.015
		int maxOccurrence = Integer.parseInt(request);
		int percent = Integer.valueOf((maxOccurrence / dbLength) * 100);

		return percent;
	}

	/**
	 * Queries DB to see if write time increases with number of writes by
	 * calculating the moving average of the write time.
	 * 
	 * @param conn [Connection] to enable the establishing online connection to
	 *             database
	 * @return [Double] The increase in the moving average of the write time
	 * @throws SQLException
	 */
	public static Double getWTIncrease(Connection conn) throws SQLException {

		String request = requestElement(conn,
				"select sum((x - x_bar) * (y - y_bar)) / sum((x - x_bar) * (x - x_bar)) as Slope "
						+ "FROM (SELECT [ID] AS x, AVG([ID]) AS x_bar, [MovingAverage] AS y, AVG([MovingAverage]) AS y_bar "
						+ "FROM (SELECT [Index_ID] AS ID, [time_stamp], [write_time], AVG([write_time]) "
						+ "OVER (ORDER BY [time_stamp] ROWS BETWEEN 2 PRECEDING AND 2 FOLLOWING) MovingAverage FROM [NormalThings]) "
						+ "MovingAverageTable)");
		Double increase = Double.valueOf(request);

		increase = new BigDecimal(increase).round(new MathContext(3)).doubleValue();

		return increase;
	}

	/**
	 * Queries DB standard deviation from the average write time.
	 * 
	 * @param conn [Connection] to enable the establishing online connection to
	 *             database
	 * @return [Double] The standard deviation in write times
	 * @throws SQLException
	 */
	public static Double getWTDev(Connection conn, int dbLength) throws SQLException {

		Double variance = 0D;

		try {
			Statement statement = conn.createStatement();
			ResultSet results = statement
					.executeQuery("SELECT (SUM([write_time]*[write_time])-(SUM([write_time])*SUM([write_time]))"
							+ "/" + dbLength + ")/(" + dbLength + "-1) FROM NormalThings");
			variance = results.getDouble(1);

			if (statement != null)
				statement.close();

		} catch (SQLException e) {
			e.getMessage();
		}

		Double standard_deviation = Math.sqrt(variance); // Approx

		standard_deviation = new BigDecimal(standard_deviation).round(new MathContext(3)).doubleValue();

		return standard_deviation;
	}

	private static Double writeTimeSeconds = 0D;

	public static void main(String[] args) throws SQLException, IOException {

		int[] blobSizes = { 100, 1000, 10000 };
		int[] blobTypes = { 0, 1 };
		int[] dbLengths = { 100, 1000, 10000 };
		Connection conn = null;

		// For DB of varying amount of rows
		for (int l = 0; l < dbLengths.length; l++) {
			// For DB of varying blob sizes
			for (int s = 0; s < blobSizes.length; s++) {
				// For DB of different blob types
				for (int t = 0; t < blobTypes.length; t++) {

					int blobSize = blobSizes[s];
					int dbLength = dbLengths[l];
					int type = blobTypes[t];
					int maxOcPercent = 0;
					Double avrgWT = 0D;
					Double minWT = 0D;
					Double maxWT = 0D;
					Double mcWT = 0D;
					Double wtDev = 0D;
					Double wtIncrease = 0D;
					String dbSize = "";
					String totWT = "";

					String blobType = "Zeros";
					if (type == 1)
						blobType = "Random";

					String fileName = "Rows-" + dbLength + "_BlobSize-" + blobSize + "_BlobType-" + blobType;
					String filePath = fileName + ".csv";

					// new File(filePath);
					// if (file.mkdirs() == true) {
					// System.out.println("Directory has been created successfully");
					// }
					// else {
					// System.out.println("Directory cannot be created");

					try (PrintWriter writer = new PrintWriter(filePath)) {
						StringBuilder sb = new StringBuilder();

						// Set CSV File headers
						sb.append(
								"Run , DB Rows , Blob Size (Bytes) , Blob Type , DB Size (Bytes) , Total Write Time , "
										+ "Avrg. Write Time (s) , Write Time Deviation , Min Write Time (s) , Max Write Time (s) , "
										+ "Max Write Time Occurrance (%) , Most Common Write Time (s) , Write Time Increase\n");
						writer.write(sb.toString());

						sb = new StringBuilder();

						int runs = 50; // Run 50 Times
						for (int i = 0; i < runs; i++) {

							String file = fileName + "_" + i;
							String url = "jdbc:sqlite:" + file + ".db";
							try {
								// Establishes online connection
								conn = DriverManager.getConnection(url);

								dbSize = new DecimalFormat("#,###").format(getDBSize(conn));
								totWT = getTotalWriteTime(conn);
								avrgWT = writeTimeSeconds / dbLength;
								avrgWT = Double.valueOf(new DecimalFormat("####0.000").format(avrgWT));
								minWT = getMinWriteTime(conn);
								maxWT = getMaxWriteTime(conn);
								mcWT = getMostCommonWT(conn);
								wtIncrease = getWTIncrease(conn);
								wtDev = getWTDev(conn, dbLength);
								maxOcPercent = getMaxOccurrence(conn, dbLength);

							} catch (SQLException e) {
								e.printStackTrace();
							} finally {
								if (conn != null)
									conn.close();
							}

							sb.append(i + 1);
							sb.append(" , ");
							sb.append(dbLength); // Append # of Rows in DB
							sb.append(" , ");
							sb.append(blobSize); // Append blob Size
							sb.append(" , ");
							sb.append(blobType); // Append binary Blob Type (zeros, random)
							sb.append(" , ");
							sb.append(dbSize.replace(",", "")); // Append Size of DB in Bytes
							sb.append(" , ");
							sb.append(totWT); // Total Write Time
							sb.append(" , ");
							sb.append(avrgWT); // Append average Write Time (s)
							sb.append(" , ");
							sb.append(wtDev); // Append write time deviation from the average
							sb.append(" , ");
							sb.append(minWT); // Append minimum Write Time (s)
							sb.append(" , ");
							sb.append(maxWT); // Append maximum Write Time (s)
							sb.append(" , ");
							sb.append(maxOcPercent); // Append likelihood of write times above 15 ms occurring.
							sb.append(" , ");
							sb.append(mcWT); // Append most common Write Time (s)
							sb.append(" , ");
							sb.append(wtIncrease); // Append whether write time increases with # of writings
							sb.append('\n');
						}

						// Write data as a line on the CSV file
						writer.write(sb.toString());
						System.out.print(sb.toString());
						System.out.println("Done with " + filePath + "!\n");

					} catch (FileNotFoundException e) {
						System.out.println(e.getMessage());
					}
				}
			}
		}
	}
}