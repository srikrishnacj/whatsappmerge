package xyz.cjcj.whatsappmerge.jdbc;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.*;

public class SqliteConnection {
    public static final Logger log = LoggerFactory.getLogger(MainProgram.class);

    private final String FILE;
    private Connection connection;

    public SqliteConnection(String FILE) {
        this.FILE = FILE;
        this.open();
    }

    public String getFILE() {
        return FILE;
    }

    private void loadDriver() {
        try {
            Class.forName("org.sqlite.JDBC");
        } catch (java.lang.ClassNotFoundException e) {
            throw new RuntimeException("Unable to load SQLite jdbc drivers");
        }
    }

    public void open() {
        this.loadDriver();
        try {
            connection = DriverManager.getConnection("jdbc:sqlite:" + FILE);
            log.info("connection opened: {}", FILE);
        } catch (SQLException throwables) {
            throw new RuntimeException("Unable to open connection to Sqlite database");
        }
    }

    public void close() {
        try {
            connection.close();
            log.info("connection closed: {}", FILE);
        } catch (SQLException e) {
            throw new RuntimeException("Unable to open connection to Sqlite database");
        }
    }

    public void attach(String file) {
        String query = "attach '" + file + "' as TOMERGE";
        try {
            connection.prepareStatement(query).execute();
        } catch (SQLException throwables) {
            throwables.printStackTrace();
            throw new RuntimeException("Unable to attach database");
        }
    }

    public void detach() {
        String query = "detach toMerge";
        try {
            connection.prepareStatement(query).execute();
        } catch (SQLException throwables) {
            throwables.printStackTrace();
            throw new RuntimeException("Unable to detach database");
        }
    }

    public ResultSet executeQuery(String query) {
        try {
            Statement s = connection.createStatement();
            return s.executeQuery(query);
        } catch (SQLException throwables) {
            throwables.printStackTrace();
            throw new RuntimeException("Unable to execute query");
        }
    }

    public void executeUpdate(String query) {
        try {
            Statement s = connection.createStatement();
            s.execute(query);
            s.close();
        } catch (SQLException e) {
            e.printStackTrace();
            throw new RuntimeException("Unable to execute update");

        }
    }
}