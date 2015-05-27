package ca.ubc.cstudies.fluidsurveyssync;

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
    private static int QUESTION_HEADER_LENGTH = 400;
    private static int ANSWER_VALUE_LENGTH = 200;

    private static int maxQuestionLength;

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
        JSONArray surveys;
        HttpGet httpGet = new HttpGet(Config.FLUID_SURVEYS_URL + "/api/v2/surveys/");
        HttpResponse response = httpClient.execute(httpGet);
        try {
            HttpEntity entity = response.getEntity();
            surveys = (new JSONObject(new JSONTokener(entity.getContent()))).getJSONArray("surveys");
            EntityUtils.consume(entity);
        } finally {
            httpGet.releaseConnection();
        }

// 2. Process each survey
        JSONObject survey;
        Map<String, Integer> idnames = new HashMap<>();
        for (int i = 0; i < surveys.length(); i++) {
            survey = surveys.getJSONObject(i);
//            System.out.println("ID: " + survey.get("id") + ", Name: " + survey.get("name"));
//            Sync.AddUpdateSurvey(survey);

// 3. Get full questions (titles) from the survey structure
            JSONArray pages;
            httpGet = new HttpGet(Config.FLUID_SURVEYS_URL + "/api/v2/surveys/" + survey.get("id") + "/?structure");
            response = httpClient.execute(httpGet);
            try {
                HttpEntity entity = response.getEntity();
                pages = (new JSONObject(new JSONTokener(entity.getContent()))).getJSONArray("form");
                EntityUtils.consume(entity);
            } finally {
                httpGet.releaseConnection();
            }
            JSONArray questions;
            JSONObject question;
            for (int j = 0; j < pages.length(); j++) {
                questions = pages.getJSONObject(j).getJSONArray("children");

                JSONArray choices;
                // 4. For each question add to a Map
                for (int k = 0; k < questions.length(); k++) {
                    question = questions.getJSONObject(k);
                    Integer l = idnames.get(question.getString("idname"));
                    idnames.put(question.getString("idname"), l == null ? 1 : ++l);
                }
            }
//            Map<String, String> titles = Sync.findTitles(pages);

// 4. Get responses with question ids in the headers
/*
            httpGet = new HttpGet(Config.FLUID_SURVEYS_URL + "/api/v2/surveys/" + survey.get("id") + "/csv/?include_id=1&show_titles=0");
            response = httpClient.execute(httpGet);
            try {
                HttpEntity entity = response.getEntity();
                BufferedReader isr = new BufferedReader(new InputStreamReader(entity.getContent(), "UTF-16LE"));
                isr.skip(1); // skip unicode marker
                AddUpdateResponses(isr, survey.getInt("id"), titles);
                EntityUtils.consume(entity);
            } finally {
                httpGet.releaseConnection();
            }
*/
        }
        System.out.println(idnames);
    }

    private static int STANDARD_HEADERS = 17;

    public static void AddUpdateResponses(BufferedReader r, int survey_id, Map<String, String> titles) throws IOException {
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
                String fs_id = l.get(i).substring(0, l.get(i).indexOf("}") + 1);
                String title = titles.get(fs_id);
                if (title.length() > maxQuestionLength) {
                    maxQuestionLength =  title.length();
                    System.out.println(maxQuestionLength + " - " + survey_id + " - " + title);
                }
//                System.out.println(l.get(i));
// Find question
                stmt = conn.prepareStatement("SELECT id FROM questions WHERE survey_id = ? AND fs_id = ?");
                stmt.setInt(1, survey_id);
                stmt.setString(2, fs_id);
                rs = stmt.executeQuery();
                if (rs.next()) {
                    question_id[i] = rs.getInt("id");
// Question found
                } else {
// Find meta-question
                    stmt = conn.prepareStatement("SELECT id FROM meta_questions WHERE header = ?");
                    stmt.setString(1, Sync.left(title, QUESTION_HEADER_LENGTH));
                    rs = stmt.executeQuery();
                    if (rs.next()) {
// Meta-question found
                        meta_question_id = rs.getInt("id");
                    } else {
// New meta-question
                        stmt = conn.prepareStatement("INSERT INTO meta_questions SET header = ?", Statement.RETURN_GENERATED_KEYS);
                        stmt.setString(1, Sync.left(title, QUESTION_HEADER_LENGTH));
                        stmt.executeUpdate();
                        rs = stmt.getGeneratedKeys();
                        if (rs.next()) {
                            meta_question_id = rs.getInt(1);
                        } else {
                            throw new IOException("Couldn't create a new meta-question");
                        }
                    }
// New question
                    stmt = conn.prepareStatement("INSERT INTO questions SET survey_id = ?, meta_question_id = ?, header = ?, fs_id = ?", Statement.RETURN_GENERATED_KEYS);
                    stmt.setInt(1, survey_id);
                    stmt.setInt(2, meta_question_id);
                    stmt.setString(3, Sync.left(title, QUESTION_HEADER_LENGTH));
                    stmt.setString(4, fs_id);
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
