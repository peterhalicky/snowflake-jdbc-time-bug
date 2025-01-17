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

            // uncommenting this fixes testTime()
            //stmt.execute("ALTER SESSION SET JDBC_USE_SESSION_TIMEZONE = FALSE");

            // this has no effect
            //stmt.execute("ALTER SESSION SET JDBC_FORMAT_DATE_WITH_TIMEZONE = FALSE");
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
    void testTime() throws SQLException {
        Time expectedTime = Time.valueOf(LocalTime.of(12, 34, 56));

        try (Statement stmt = conn.createStatement()) {
            stmt.execute("CREATE OR REPLACE TABLE TIME_TEST (ID INT, TEST_TIME TIME)");

            String insertSql = "INSERT INTO TIME_TEST (ID, TEST_TIME) VALUES (?, ?)";
            try (PreparedStatement pstmt = conn.prepareStatement(insertSql)) {
                pstmt.setInt(1, 1);
                pstmt.setTime(2, expectedTime);
                pstmt.executeUpdate();
            }
        }

        String selectSql = "SELECT TEST_TIME FROM TIME_TEST WHERE ID = 1";
        try (Statement stmt = conn.createStatement(); ResultSet rs = stmt.executeQuery(selectSql)) {
            if (rs.next()) {
                Time retrievedSqlTime = rs.getTime("TEST_TIME");

                // Compare
                assertEquals(expectedTime, retrievedSqlTime, "Retrieved time does not match the inserted time");
            } else {
                fail("No row found for ID = 1");
            }
        }
    }

    @Test
    void testDateTime() throws SQLException {
        Timestamp expectedTime = new Timestamp(System.currentTimeMillis());

        try (Statement stmt = conn.createStatement()) {
            stmt.execute("CREATE OR REPLACE TABLE DATETIME_TEST (ID INT, TEST_DATETIME TIMESTAMP)");

            String insertSql = "INSERT INTO DATETIME_TEST (ID, TEST_DATETIME) VALUES (?, ?)";
            try (PreparedStatement pstmt = conn.prepareStatement(insertSql)) {
                pstmt.setInt(1, 1);
                pstmt.setTimestamp(2, expectedTime);
                pstmt.executeUpdate();
            }
        }

        String selectSql = "SELECT TEST_DATETIME FROM DATETIME_TEST WHERE ID = 1";
        try (Statement stmt = conn.createStatement(); ResultSet rs = stmt.executeQuery(selectSql)) {
            if (rs.next()) {
                Timestamp retrievedSqlTime = rs.getTimestamp("TEST_DATETIME");

                // Compare
                assertEquals(expectedTime, retrievedSqlTime, "Retrieved time does not match the inserted time");
            } else {
                fail("No row found for ID = 1");
            }
        }
    }
}
