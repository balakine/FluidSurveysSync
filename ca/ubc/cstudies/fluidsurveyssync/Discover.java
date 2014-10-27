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
import java.io.IOException;
import java.security.NoSuchAlgorithmException;
import java.security.KeyManagementException;
import java.sql.SQLException;

public class Discover {
    private static int SURVEY_NAME_LENGTH = 100;
    private static int QUESTION_HEADER_LENGTH =200;
    private static int ANSWER_VALUE_LENGTH = 200;

    public static void main(String[] args) throws IOException, NoSuchAlgorithmException, KeyManagementException, SQLException {
        SyncSurveys();
    }

    static void SyncSurveys() throws IOException, NoSuchAlgorithmException, KeyManagementException, SQLException {
        SSLContext sslContext = SSLContexts.custom()
            .useTLS() // Only this turned out to be not enough
            .build();
        SSLConnectionSocketFactory sf = new SSLConnectionSocketFactory(
            sslContext,
            new String[] {"TLSv1", "TLSv1.1", "TLSv1.2"},
            null,
            SSLConnectionSocketFactory.BROWSER_COMPATIBLE_HOSTNAME_VERIFIER);
        CloseableHttpClient httpclient = HttpClients.custom()
        .setSSLSocketFactory(sf)
        .build();
//        DefaultHttpClient httpclient = new DefaultHttpClient();
        HttpGet httpGet = new HttpGet(Config.FLUID_SURVEYS_URL + "/api/v2/surveys/");
        HttpResponse response1 = httpclient.execute(httpGet);
// 1. Get a list of surveys
        JSONArray surveys, pages, questions;
        JSONObject survey, page, question;
        try {
            HttpEntity entity1 = response1.getEntity();
            surveys = (new JSONObject(new JSONTokener(entity1.getContent()))).getJSONArray("surveys");
            EntityUtils.consume(entity1);
        } finally {
            httpGet.releaseConnection();
        }

        for (int i = 0; i < surveys.length(); i++) {
            survey = surveys.getJSONObject(i);
            System.out.println(survey.getLong("id") + ";" + survey.getString("name"));
        }
/*
        Map <String, Integer> idnames = new HashMap<String, Integer>();
// 2. For each survey get a list of pages
        for (int i = 0; i < surveys.length(); i++) {
            survey = surveys.getJSONObject(i);
            httpGet = new HttpGet(Config.FLUID_SURVEYS_URL + "/api/v2/surveys/" + survey.get("id") + "/?structure");
            response1 = httpclient.execute(httpGet);
            try {
                HttpEntity entity1 = response1.getEntity();
                pages = (new JSONObject(new JSONTokener(entity1.getContent()))).getJSONArray("form");
                EntityUtils.consume(entity1);
            } finally {
                httpGet.releaseConnection();
            }

// 3. For each page get a list of questions
            for (int j = 0; j < pages.length(); j++) {
                questions = pages.getJSONObject(j).getJSONArray("children");

// 4. For each question add to a Map
                for (int k = 0; k < questions.length(); k++) {
                    question = questions.getJSONObject(k);
                    Integer c = idnames.get(question.getString("idname"));
                    idnames.put(question.getString("idname"), (c == null) ? 1 : c + 1);
                }
            }
        }
        System.out.println("ID name: " + idnames);
        */
    }
}
