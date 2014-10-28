package ca.ubc.cstudies.fluidsurveyssync;

import oracle.jdbc.pool.OracleDataSource;
import org.apache.http.HttpEntity;
import org.apache.http.HttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.DefaultHttpClient;
import org.apache.http.util.EntityUtils;
import org.json.JSONArray;
import org.json.JSONObject;
import org.json.JSONTokener;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.sql.*;
import java.util.ArrayList;
import java.util.List;

public class Sync {
    private static int SURVEY_NAME_LENGTH = 100;
    private static int QUESTION_HEADER_LENGTH =200;
    private static int ANSWER_VALUE_LENGTH = 200;

    public static void main(String[] args) throws IOException {
        SyncSurveys();
    }

    static void SyncSurveys() throws IOException {

        DefaultHttpClient httpclient = new DefaultHttpClient();
        HttpGet httpGet = new HttpGet(Config.FLUID_SURVEYS_URL + "/api/v2/surveys/");
        HttpResponse response1 = httpclient.execute(httpGet);
// 1. Get a list of surveys
        JSONArray surveys;
        try {
            HttpEntity entity1 = response1.getEntity();
            surveys = (new JSONObject(new JSONTokener(entity1.getContent()))).getJSONArray("surveys");
            EntityUtils.consume(entity1);
        } finally {
            httpGet.releaseConnection();
        }

// 2. For each survey get CSV responses
        JSONObject survey;
        for (int i = 0; i < surveys.length(); i++) {
            survey = surveys.getJSONObject(i);
            System.out.println("ID: " + survey.get("id") + ", Name: " + survey.get("name"));
            AddUpdateSurvey(survey);

            httpGet = new HttpGet(Config.FLUID_SURVEYS_URL + "/api/v2/surveys/" + survey.get("id") + "/csv/");
            response1 = httpclient.execute(httpGet);
            try {
                HttpEntity entity1 = response1.getEntity();
                BufferedReader isr = new BufferedReader(new InputStreamReader(entity1.getContent(), "UTF-16LE"));
                isr.skip(1); // skip unicode marker
                AddUpdateResponses(isr, survey.getInt("id"));
                EntityUtils.consume(entity1);
            } finally {
                httpGet.releaseConnection();
            }
        }
    }

    public static void AddUpdateSurvey(JSONObject s) {
/*
        try {
// The newInstance() call is a work around for some
// broken Java implementations
            Class.forName("com.mysql.jdbc.Driver").newInstance();
        } catch (Exception ex) {
// handle the error
            System.out.println("SQLException: " + ex.getMessage());
        }
*/
        Connection conn = null;
        PreparedStatement stmt = null;
        ResultSet rs = null;
        try {
            conn = DriverManager.getConnection(Config.MYSQL_URL);
// Find survey
            stmt = conn.prepareStatement("SELECT id FROM surveys WHERE id = ?");
            stmt.setInt(1, s.getInt("id"));
            rs = stmt.executeQuery();
            if (rs.next()) {
// Survey found
                stmt = conn.prepareStatement("UPDATE surveys SET name = ?, section_id = ? WHERE id = ?");
                stmt.setString(1, left(s.getString("name"), SURVEY_NAME_LENGTH));
                stmt.setObject(2, resolveSectionId(s.getString("name")), Types.INTEGER);
                stmt.setInt(3, s.getInt("id"));
                stmt.executeUpdate();
            } else {
// New survey
                stmt = conn.prepareStatement("INSERT INTO surveys SET id = ?, name = ?, section_id = ?");
                stmt.setInt(1, s.getInt("id"));
                stmt.setString(2, left(s.getString("name"), SURVEY_NAME_LENGTH));
                stmt.setObject(3, resolveSectionId(s.getString("name")), Types.INTEGER);
                stmt.executeUpdate();
            }

        } catch (SQLException ex) {// handle any errors
            System.out.println("SQLException: " + ex.getMessage());
            System.out.println("SQLState: " + ex.getSQLState());
            System.out.println("VendorError: " + ex.getErrorCode());
        } finally {
// it is a good idea to release
// resources in a finally{} block
// in reverse-order of their creation
// if they are no-longer needed
            if (rs != null) {
                try {
                    rs.close();
                } catch (SQLException sqlEx) {
                } // ignore
                rs = null;
            }
            if (stmt != null) {
                try {
                    stmt.close();
                } catch (SQLException sqlEx) {
                } // ignore
                stmt = null;
            }
            if (conn != null) {
                try {
                    conn.close();
                } catch (SQLException sqlEx) {
                } // ignore
                conn = null;
            }
        }
    }

    private static int STANDARD_HEADERS = 17;

    private static void AddUpdateResponses(BufferedReader r, int survey_id) throws IOException {
        List<String> l;
        // get headers
        l = ParseLine(r);
        if (l.size() < STANDARD_HEADERS)
            throw new IOException("CSV Format error - expected at least " + STANDARD_HEADERS + " columns, found " + l.size());
/*
        try {
// The newInstance() call is a work around for some
// broken Java implementations
            Class.forName("com.mysql.jdbc.Driver").newInstance();
        } catch (Exception ex) {
// handle the error
            System.out.println("SQLException: " + ex.getMessage());
        }
*/
        Connection conn = null;
        PreparedStatement stmt = null;
        ResultSet rs = null;
        try {
            int[] question_id = new int[l.size()];
            int meta_question_id;
            int response_id;
            conn = DriverManager.getConnection(Config.MYSQL_URL);
            for (int i = STANDARD_HEADERS; i < l.size(); i++) {
                System.out.println(l.get(i));
// Find question
                stmt = conn.prepareStatement("SELECT id FROM questions WHERE survey_id = ? AND header = ?");
                stmt.setInt(1, survey_id);
                stmt.setString(2, left(l.get(i), QUESTION_HEADER_LENGTH));
                rs = stmt.executeQuery();
                if (rs.next()) {
                    question_id[i] = rs.getInt("id");
// Question found
                } else {
// Find meta-question
                    stmt = conn.prepareStatement("SELECT id FROM meta_questions WHERE header = ?");
                    stmt.setString(1, left(l.get(i), QUESTION_HEADER_LENGTH));
                    rs = stmt.executeQuery();
                    if (rs.next()) {
// Meta-question found
                        meta_question_id = rs.getInt("id");
                    } else {
// New meta-question
                        stmt = conn.prepareStatement("INSERT INTO meta_questions SET header = ?", Statement.RETURN_GENERATED_KEYS);
                        stmt.setString(1, left(l.get(i), QUESTION_HEADER_LENGTH));
                        stmt.executeUpdate();
                        rs = stmt.getGeneratedKeys();
                        if (rs.next()) {
                            meta_question_id = rs.getInt(1);
                        } else {
                            throw new IOException("Couldn't create a new meta-question");
                        }
                    }
// New question
                    stmt = conn.prepareStatement("INSERT INTO questions SET survey_id = ?, meta_question_id = ?, header = ?", Statement.RETURN_GENERATED_KEYS);
                    stmt.setInt(1, survey_id);
                    stmt.setInt(2, meta_question_id);
                    stmt.setString(3, left(l.get(i), QUESTION_HEADER_LENGTH));
                    stmt.executeUpdate();
                    rs = stmt.getGeneratedKeys();
                    if (rs.next()) {
                        question_id[i] = rs.getInt(1);
                    } else {
                        throw new IOException("Couldn't create a new question");
                    }
                }
            }
            // cycle through lines until EOF
            while ((l = ParseLine(r)) != null) {
                if (l.size() > question_id.length)
                    throw new IOException("More columns than headers");
                else if (l.size() < question_id.length)
                    throw new IOException("Less columns than headers");
// Find response
                response_id = Integer.parseInt(l.get(1));
                stmt = conn.prepareStatement("SELECT id FROM responses WHERE id = ?");
                stmt.setInt(1, response_id);
                rs = stmt.executeQuery();
                if (rs.next()) {
// Response found
// TODO Update the response
                } else {
// New response
                    stmt = conn.prepareStatement("INSERT INTO responses SET id = ?, survey_id = ?");
                    stmt.setInt(1, response_id);
                    stmt.setInt(2, survey_id);
// TODO Insert other fields
                    stmt.executeUpdate();
                }
                for (int i = STANDARD_HEADERS; i < l.size(); i++) {
// Find answer
                    stmt = conn.prepareStatement("SELECT id FROM answers WHERE response_id = ? AND question_id = ?");
                    stmt.setInt(1, response_id);
                    stmt.setInt(2, question_id[i]);
                    rs = stmt.executeQuery();
                    if (rs.next()) {
// Answer found
                        stmt = conn.prepareStatement("UPDATE answers SET value = ? WHERE response_id = ? AND question_id = ?");
                        stmt.setString(1, left(l.get(i), ANSWER_VALUE_LENGTH));
                        stmt.setInt(2, response_id);
                        stmt.setInt(3, question_id[i]);
                        stmt.executeUpdate();
                    } else {
// New answer
                        stmt = conn.prepareStatement("INSERT INTO answers SET response_id = ?, question_id = ?, value = ?");
                        stmt.setInt(1, response_id);
                        stmt.setInt(2, question_id[i]);
                        stmt.setString(3, left(l.get(i), ANSWER_VALUE_LENGTH));
                        stmt.executeUpdate();
                    }
                    System.out.print(l.get(i) + '\t');
                }
                System.out.println("");
            }
        } catch (SQLException ex) {// handle any errors
            System.out.println("SQLException: " + ex.getMessage());
            System.out.println("SQLState: " + ex.getSQLState());
            System.out.println("VendorError: " + ex.getErrorCode());
        } finally {
// it is a good idea to release
// resources in a finally{} block
// in reverse-order of their creation
// if they are no-longer needed
            if (rs != null) {
                try {
                    rs.close();
                } catch (SQLException sqlEx) {
                } // ignore
                rs = null;
            }
            if (stmt != null) {
                try {
                    stmt.close();
                } catch (SQLException sqlEx) {
                } // ignore
                stmt = null;
            }
            if (conn != null) {
                try {
                    conn.close();
                } catch (SQLException sqlEx) {
                } // ignore
                conn = null;
            }
        }
    }

    private static List<String> ParseLine(BufferedReader r) throws IOException {
        List<String> l = new ArrayList<String>();
        int i;
        Boolean tf = false;
        StringBuilder x = null;

        // read characters until EOF or EOL
        while ((i = r.read()) != -1) {
            if (!tf) {
                if (i != '"')
                    // throw an exception - tokens should always start with '"'
                    throw new IOException("Expected opening '\"', encountered " + (char) i);
                // found an opening '"' for the next token
                tf = true;
                x = new StringBuilder();
            } else {
                if (i != '"') {
                    // add another character to the current token
                    x.append((char) i);
                } else {
                    // found '"' in the stream, we need the next
                    // character to interpret that '"' correctly
                    i = r.read();
                    if (i == '"') {
                        // found '""' - escaped '"', un-escape,
                        // add to the current token
                        x.append('"');
                    } else if (i == '\t' || i == '\r') {
                        // found a closing '"' for the current token
                        l.add(x.toString());
                        tf = false;
                        if (i == '\r') {
                            r.skip(1); // Discard the following \n
                            break; // EOL
                        }
                    } else
                        // throw an exception - formatting error
                        throw new IOException("Expected '\"', TAB, or EOL after '\"', encountered " + (char) i);
                }
            }
        }
        return l.size() == 0 ? null : l;
    }

    public static Integer resolveSectionId(String code) throws SQLException {
        Integer sectionId;
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
        stmt.setString(1, code);
        ResultSet rset = stmt.executeQuery();
        if (rset.next()) {
            System.out.println ("Found! Section id: " + rset.getInt(1) + ", section Code:" + rset.getString(2));
            sectionId = rset.getInt(1);
        } else {
            System.out.println ("Not found! Section Code: " + code);
            sectionId = null;
        }
        // Close the ResultSet
        rset.close();
        rset =  null;
        // Close the Statement
        stmt.close();
        stmt = null;
        // Close the connection
        conn.close();
        conn = null;

        return sectionId;
    }
    private static String left(String s, int l) {
        return s.substring(0, Math.min(l, s.length()));
    }
}
