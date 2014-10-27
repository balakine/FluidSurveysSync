package ca.ubc.cstudies.fluidsurveyssync;

import java.io.FileReader;
import java.io.IOException;
import java.util.Properties;

public class Config {
    public static String FLUID_SURVEYS_URL;
    public static String MYSQL_URL;
    public static String SRS_URL;
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
        System.out.println("Config read OK.");
    }
}
