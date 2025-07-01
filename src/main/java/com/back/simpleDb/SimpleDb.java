package com.back.simpleDb;

import lombok.extern.slf4j.Slf4j;

import java.sql.*;

@Slf4j
public class SimpleDb {
    private final String URL, USERNAME, PASSWORD, DB_NAME;
    private boolean isDev; // 개발 모드 여부
    private static final ThreadLocal<Connection> connectionThreadLocal = new ThreadLocal<>();

    public SimpleDb(String URL, String USERNAME, String PASSWORD, String DB_NAME) {
        this.URL = URL;
        this.USERNAME = USERNAME;
        this.PASSWORD = PASSWORD;
        this.DB_NAME = DB_NAME;
        this.isDev = false; // 기본값은 false로 설정
    }

    public void setDevMode(boolean isDev) {
        this.isDev = isDev;
    }

    public Connection getConnection() {
        String connectionURL = "jdbc:mysql://" + URL + "/" + DB_NAME;
        Connection con = connectionThreadLocal.get();// 현재 스레드의 커넥션을 반환
        try {
            if (con == null || con.isClosed()) {
                con = DriverManager.getConnection(connectionURL, USERNAME, PASSWORD);
                connectionThreadLocal.set(con);
            }
        } catch (SQLException e) {
            log.error("method=getConnection, Connection error: {}", e.getMessage());
        }
        return con;
    }

    public void run(String query) {
//        Connection con = null;
//        Statement stmt = null;
        try (Statement stmt = getConnection().createStatement()) {
//            con = getConnection();
//            Statement stmt = con.createStatement();
            stmt.execute(query);
        } catch (SQLException e) {
            log.error("method=run, SQL error: {}", e.getMessage());
//            throw new RuntimeException(e);
        } finally {
//            close();
        }
    }

    public void run(String query, String title, String body, boolean isBlind) {
//        PreparedStatement pstmt = null;
        try (PreparedStatement pstmt = getConnection().prepareStatement(query)) {
//            pstmt = getConnection().prepareStatement(query);
            pstmt.setString(1, title);
            pstmt.setString(2, body);
            pstmt.setBoolean(3, isBlind);
            pstmt.executeUpdate();
        } catch (SQLException e) {
            log.error("method=run, SQL error: {}", e.getMessage());
        }finally{
//            close();
        }
    }

    public void close() {
        try {
            //현재 스레드의 커넥션을 가져와서 닫기
            getConnection().close();
        } catch (SQLException e) {
            log.error("method=close, Connection close error: {}", e.getMessage());
//            throw new RuntimeException(e);
        } finally {
            connectionThreadLocal.remove(); // ThreadLocal에서 연결 제거
            log.info("Connection closed and removed from ThreadLocal.");
        }
    }

    public Sql genSql() {
        return new Sql(getConnection());
    }

    public void startTransaction() {
        try {
            // 현재 스레드의 커넥션을 가져와서 트랜잭션 시작
            getConnection().setAutoCommit(false);
        } catch (SQLException e) {
            log.error("method=startTransaction, Transaction start error: {}", e.getMessage());
        }
    }

    public void rollback() {
        try {
            // 현재 스레드의 커넥션을 가져와서 롤백
            getConnection().rollback();
            log.info("Transaction rolled back.");
            getConnection().setAutoCommit(true);
        } catch (SQLException e) {
            log.error("method=rollback, Transaction rollback error: {}", e.getMessage());
        }
    }

    public void commit() {
        try {
            getConnection().commit();
        } catch (SQLException e) {
            log.error("method=commit, Transaction commit error: {}", e.getMessage());
            throw new RuntimeException(e);
        }
    }
}