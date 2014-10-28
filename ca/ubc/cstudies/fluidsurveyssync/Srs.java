package ca.ubc.cstudies.fluidsurveyssync;

import oracle.jdbc.pool.OracleDataSource;

import java.sql.*;

public class Srs {
    public static void main(String[] args) throws SQLException {
        String survey_title = "IL660F14A-9";
        // Open database connection
        OracleDataSource ods = new OracleDataSource();
        ods.setURL(Config.SRS_URL);
        Connection conn = ods.getConnection();

        // Create a Statement
        PreparedStatement stmt = conn.prepareStatement("SELECT " +
                        "SECTION.SECTION_ID, " +
        		        "COURSE.COURSE_CODE || TERM.TERM_CODE || SECTION.SECTION_CODE " +
        	        "FROM SECTION " +
        		        "LEFT JOIN TERM ON SECTION.TERM_ID = TERM.TERM_ID " +
        		        "LEFT JOIN SECTION_COURSE ON SECTION.SECTION_ID = SECTION_COURSE.SECTION_ID " +
        		        "LEFT JOIN COURSE ON SECTION_COURSE.COURSE_ID = COURSE.COURSE_ID " +
                "WHERE ? LIKE COURSE.COURSE_CODE || TERM.TERM_CODE || SECTION.SECTION_CODE || '%'");
        stmt.setString(1, survey_title);
        // Select the USER column from the dual table
        ResultSet rset = stmt.executeQuery();
        // Iterate through the result and print the USER
        while (rset.next ())
          System.out.println ("Section Code: " + rset.getString(2) + ", section id:" + rset.getInt(1));
        // Close the ResultSet
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
