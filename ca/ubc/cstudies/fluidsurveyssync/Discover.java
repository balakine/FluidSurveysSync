package ca.ubc.cstudies.fluidsurveyssync;

import oracle.jdbc.pool.OracleDataSource;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.HttpResponse;
import org.apache.http.HttpEntity;
import org.apache.http.conn.ssl.SSLConnectionSocketFactory;
import org.apache.http.conn.ssl.SSLContexts;
import org.apache.http.util.EntityUtils;
import org.json.JSONArray;
import org.json.JSONObject;
import org.json.JSONTokener;

import javax.net.ssl.SSLContext;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.security.NoSuchAlgorithmException;
import java.security.KeyManagementException;
import java.sql.*;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class Discover {
    private static int SURVEY_NAME_LENGTH = 100;
    private static int QUESTION_HEADER_LENGTH =200;
    private static int ANSWER_VALUE_LENGTH = 200;

    public static void main(String[] args) throws IOException, KeyManagementException, NoSuchAlgorithmException {

// Workaround for disabled SSL3 on fluidsurveys.com
        SSLContext sslContext = SSLContexts.custom()
            .useTLS() // Only this turned out to be not enough
            .build();
        SSLConnectionSocketFactory sf = new SSLConnectionSocketFactory(
            sslContext,
            new String[] {"TLSv1", "TLSv1.1", "TLSv1.2"},
            null,
            SSLConnectionSocketFactory.BROWSER_COMPATIBLE_HOSTNAME_VERIFIER);
        CloseableHttpClient httpClient = HttpClients.custom()
        .setSSLSocketFactory(sf)
        .build();

// 1. Get a list of surveys
        HttpGet httpGet = new HttpGet(Config.FLUID_SURVEYS_URL + "/api/v2/surveys/");
        HttpResponse response = httpClient.execute(httpGet);
        JSONArray surveys, pages, questions;
        try {
            HttpEntity entity = response.getEntity();
            surveys = (new JSONObject(new JSONTokener(entity.getContent()))).getJSONArray("surveys");
            EntityUtils.consume(entity);
        } finally {
            httpGet.releaseConnection();
        }

// 2. Process each survey
        JSONObject survey, question;
        for (int i = 0; i < surveys.length(); i++) {
            survey = surveys.getJSONObject(i);
//            System.out.println("ID: " + survey.get("id") + ", Name: " + survey.get("name"));
            Sync.AddUpdateSurvey(survey);

            httpGet = new HttpGet(Config.FLUID_SURVEYS_URL + "/api/v2/surveys/" + survey.get("id") + "/?structure");
            response = httpClient.execute(httpGet);
            try {
                HttpEntity entity = response.getEntity();
                pages = (new JSONObject(new JSONTokener(entity.getContent()))).getJSONArray("form");
                EntityUtils.consume(entity);
            } finally {
                httpGet.releaseConnection();
            }

// 3. For each page get a list of questions
            Map<String, String> idnames = new HashMap<>();
            for (int j = 0; j < pages.length(); j++) {
                questions = pages.getJSONObject(j).getJSONArray("children");

                // 4. For each question add to a Map
                for (int k = 0; k < questions.length(); k++) {
                    question = questions.getJSONObject(k);
                    switch (question.getString("idname")) {
                        case "multiple-choice":
                            // add children and "/text" for "Other" option, if needed
                            break;
                        case "ranking":
                        case "single-choice-grid":
                        case "text-response-grid":
                            // add children
                            break;
                        case "single-choice":
                            idnames.put("{" + question.getString("id") + "}", question.getJSONObject("title").getString("en"));
                            // add "/other" if needed
                            String other = Sync.hasOther(question);
                            if (other != null)
                                idnames.put("{" + question.getString("id") + "\\other}",
                                        question.getJSONObject("title").getString("en") + " | " + other);
                            break;
                        case "boolean-choice":
                        case "dropdown-choice":
                        case "text-response":
                        default:
                            idnames.put("{" + question.getString("id") + "}", question.getJSONObject("title").getString("en"));
                            break;
                    }
                }
            }

            httpGet = new HttpGet(Config.FLUID_SURVEYS_URL + "/api/v2/surveys/" + survey.get("id") + "/csv/?include_id=1&show_titles=0");
            response = httpClient.execute(httpGet);
            try {
                HttpEntity entity = response.getEntity();
                BufferedReader isr = new BufferedReader(new InputStreamReader(entity.getContent(), "UTF-16LE"));
                isr.skip(1); // skip unicode marker
                AddUpdateResponses(isr, survey.getInt("id"), idnames);
                EntityUtils.consume(entity);
            } finally {
                httpGet.releaseConnection();
            }
        }
    }

    private static int STANDARD_HEADERS = 17;

    public static void AddUpdateResponses(BufferedReader r, int survey_id, Map<String, String> idnames) throws IOException {
        List<String> l;
        // get headers
        l = Sync.ParseLine(r);
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
//                System.out.println(l.get(i));
// Find question
                stmt = conn.prepareStatement("SELECT id FROM questions WHERE survey_id = ? AND header = ?");
                stmt.setInt(1, survey_id);
                stmt.setString(2, Sync.left(l.get(i), QUESTION_HEADER_LENGTH));
                rs = stmt.executeQuery();
                if (rs.next()) {
                    question_id[i] = rs.getInt("id");
// Question found
                } else {
// Find meta-question
                    stmt = conn.prepareStatement("SELECT id FROM meta_questions WHERE header = ?");
                    stmt.setString(1, Sync.left(l.get(i), QUESTION_HEADER_LENGTH));
                    rs = stmt.executeQuery();
                    if (rs.next()) {
// Meta-question found
                        meta_question_id = rs.getInt("id");
                    } else {
// New meta-question
                        stmt = conn.prepareStatement("INSERT INTO meta_questions SET header = ?", Statement.RETURN_GENERATED_KEYS);
                        stmt.setString(1, Sync.left(l.get(i), QUESTION_HEADER_LENGTH));
                        stmt.executeUpdate();
                        rs = stmt.getGeneratedKeys();
                        if (rs.next()) {
                            meta_question_id = rs.getInt(1);
                        } else {
                            throw new IOException("Couldn't create a new meta-question");
                        }
                    }
                    String fs_id = l.get(i).substring(1, 11); // get FS question id - TODO
                    String fs_idname = idnames.get(fs_id); // match header to question idname
// New question
                    stmt = conn.prepareStatement("INSERT INTO questions SET survey_id = ?, meta_question_id = ?, header = ?, fs_id = ?, fs_idname = ?", Statement.RETURN_GENERATED_KEYS);
                    stmt.setInt(1, survey_id);
                    stmt.setInt(2, meta_question_id);
                    stmt.setString(3, Sync.left(l.get(i), QUESTION_HEADER_LENGTH));
                    stmt.setObject(4, fs_id, Types.VARCHAR);
                    stmt.setObject(5, fs_idname, Types.VARCHAR);
                    stmt.executeUpdate();
                    rs = stmt.getGeneratedKeys();
                    if (rs.next()) {
                        question_id[i] = rs.getInt(1);
                    } else {
                        throw new IOException("Couldn't create a new question");
                    }
                }
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
}
