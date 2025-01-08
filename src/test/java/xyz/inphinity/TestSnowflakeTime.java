package xyz.inphinity;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.sql.*;
import java.time.LocalTime;
import java.util.Properties;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.fail;

class TestSnowflakeTime {
    private static Properties config;
    private Connection conn = null;

    @BeforeAll
    static void loadConfig() throws IOException {
        try (var is = ClassLoader.getSystemClassLoader().getResourceAsStream("connection.properties")) {
            config = new Properties();
            config.load(is);
        }
    }

    private Connection newConnection() throws SQLException {
        String url = String.format("jdbc:snowflake://%s?client_config_file=sf_client_config.json", config.getProperty("host"));
        Connection conn = DriverManager.getConnection(url, config.getProperty("username"), config.getProperty("password"));
        try (Statement stmt = conn.createStatement()) {
            stmt.execute("USE WAREHOUSE " + config.getProperty("warehouse"));
            stmt.execute("USE DATABASE " + config.getProperty("database"));
            stmt.execute("USE SCHEMA " + config.getProperty("schema"));
            stmt.execute("CREATE OR REPLACE TABLE TIME_TEST (ID INT, TEST_TIME TIME)");
        }
        return conn;
    }

    @BeforeEach
    void openConnection() throws SQLException {
        conn = newConnection();
    }

    @AfterEach
    void closeConnection() throws SQLException {
        if (conn != null) {
            conn.close();
            conn = null;
        }
    }

    @Test
    void test() throws SQLException {
        LocalTime expectedTime = LocalTime.of(12, 34, 56);

        try (Statement stmt = conn.createStatement()) {
            stmt.execute("CREATE OR REPLACE TABLE TIME_TEST (ID INT, TEST_TIME TIME)");

            String insertSql = "INSERT INTO TIME_TEST (ID, TEST_TIME) VALUES (?, ?)";
            try (PreparedStatement pstmt = conn.prepareStatement(insertSql)) {
                pstmt.setInt(1, 1);
                pstmt.setTime(2, Time.valueOf(expectedTime));
                pstmt.executeUpdate();
            }
        }

        String selectSql = "SELECT TEST_TIME FROM TIME_TEST WHERE ID = 1";
        try (Statement stmt = conn.createStatement(); ResultSet rs = stmt.executeQuery(selectSql)) {
            if (rs.next()) {
                Time retrievedSqlTime = rs.getTime("TEST_TIME");
                LocalTime actualTime = retrievedSqlTime.toLocalTime();

                // Compare
                assertEquals(expectedTime, actualTime, "Retrieved time does not match the inserted time");
            } else {
                fail("No row found for ID = 1");
            }
        }
    }
}
