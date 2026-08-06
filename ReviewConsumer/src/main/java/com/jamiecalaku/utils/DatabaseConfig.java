package com.jamiecalaku.utils;

import org.apache.flink.connector.jdbc.JdbcConnectionOptions;

public class DatabaseConfig {
    private static final String URL = System.getenv("POSTGRES_URL");
    private static final String USER = System.getenv("POSTGRES_USER");
    private static final String PASSWORD = System.getenv("POSTGRES_PASSWORD");

    public static JdbcConnectionOptions get() {
        return new JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
                .withUrl(URL)
                .withDriverName("org.postgresql.Driver")
                .withUsername(USER)
                .withPassword(PASSWORD)
                .build();
    }
}