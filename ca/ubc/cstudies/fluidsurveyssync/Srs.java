package ca.ubc.cstudies.fluidsurveyssync;

import oracle.jdbc.pool.OracleDataSource;

import java.io.FileReader;
import java.io.IOException;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Properties;

public class Srs {
    private static String FLUID_SURVEYS_URL;
    private static String MYSQL_URL;
    private static String SRS_URL;
    static {
        // Load configuration
        Properties config = new Properties();
        try {
            FileReader in = new FileReader("FluidSurveys.properties");
            config.load(in);
            in.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
        FLUID_SURVEYS_URL = "https://" +
                config.getProperty("FS.ApiKey") + ":" +
                config.getProperty("FS.Password") + "@" +
                config.getProperty("FS.ServerName");
        MYSQL_URL = "jdbc:mysql://" +
                config.getProperty("MySQL.Host") + "/" +
                config.getProperty("MySQL.Database") + "?user=" +
                config.getProperty("MySQL.User") + "&password=" +
                config.getProperty("MySQL.Password");
        SRS_URL = "jdbc:oracle:thin:" +
                config.getProperty("SRS.User") + "/" +
                config.getProperty("SRS.Password") + "@(DESCRIPTION=(ADDRESS=(PROTOCOL=tcp)(PORT=1521)(HOST=" +
                config.getProperty("SRS.Host") + "))(CONNECT_DATA=(SERVICE_NAME=" +
                config.getProperty("SRS.ServiceName") + ")))";
    }

    public static void main(String[] args) throws SQLException, IOException {
        // Open database connection
        OracleDataSource ods = new OracleDataSource();
        ods.setURL(SRS_URL);
        Connection conn = ods.getConnection();
        System.out.println(ods.getURL());
        
        // Create a Statement
        Statement stmt = conn.createStatement ();
        // Select the USER column from the dual table
        ResultSet rset = stmt.executeQuery ("select DOW_CODE from DOW");
        // Iterate through the result and print the USER
        while (rset.next ())
          System.out.println ("User name is " + rset.getString (1));
        // Close the RseultSet
        rset.close();
        rset =  null;
        // Close the Statement
        stmt.close();
        stmt = null;
        // Close the connection
        conn.close();
        conn = null;
    }
}
