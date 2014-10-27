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
    public static void main(String[] args) throws SQLException, IOException {
        // Open database connection
        OracleDataSource ods = new OracleDataSource();
        ods.setURL(Config.SRS_URL);
        Connection conn = ods.getConnection();

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
