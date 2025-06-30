package com.back.simpleDb;

import lombok.extern.slf4j.Slf4j;

import java.lang.reflect.Field;
import java.sql.*;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

@Slf4j
public class Sql {
    private final Connection con;
    private final StringBuilder query = new StringBuilder();
    private final List<Object> params = new ArrayList<>();

    public Sql(Connection con) {
        this.con = con;
    }

    public Sql append(String s) {
        query.append(s).append(" ");
        return this;
    }

    public Sql append(String s, Object... params) {
        query.append(s).append(" ");
        if(params != null) {
            this.params.addAll(Arrays.asList(params));//변수는 따로 저장
        }
        return this;
    }

    public Sql appendIn(String s, Object... params) {
        StringBuilder sb = new StringBuilder();
        sb.append("?");
        for (int i = 1; i < params.length; i++) {
            sb.append(", ?");
        }
        String altered = s.replaceAll("\\?", sb.toString());
        query.append(altered).append(" ");
        this.params.addAll(Arrays.asList(params));
        return this;
    }

    public long insert() {
//        PreparedStatement pstmt = null;
//        ResultSet rs = null;
        try(PreparedStatement pstmt = con.prepareStatement(query.toString(), Statement.RETURN_GENERATED_KEYS)

        ) {
//            pstmt = con.prepareStatement(query.toString(), Statement.RETURN_GENERATED_KEYS);
            for (int i = 0; i < params.size(); i++) {
                pstmt.setObject(i + 1, params.get(i));
            }
            pstmt.executeUpdate();
            ResultSet rs = pstmt.getGeneratedKeys();
            if (rs.next()) return rs.getLong(1);
        } catch (SQLException e) {
            log.error("method=insert, SQL error: {}", e.getMessage());
        }
        return -1;
    }

    public int update() {
//        PreparedStatement pstmt = null;
        try(PreparedStatement pstmt = con.prepareStatement(query.toString())) {
//            pstmt = con.prepareStatement(query.toString());
            for (int i = 0; i < params.size(); i++) {
                pstmt.setObject(i + 1, params.get(i));
            }
            return pstmt.executeUpdate();
        }catch (SQLException e){
            log.error("method=update, SQL error: {}", e.getMessage());
        }

        return -1;
    }

    public int delete() {
//        PreparedStatement pstmt = null;
        try(PreparedStatement pstmt = con.prepareStatement(query.toString())) {
//            pstmt = con.prepareStatement(query.toString());
            for (int i = 0; i < params.size(); i++) {
                pstmt.setObject(i + 1, params.get(i));
            }
            return pstmt.executeUpdate();
        } catch (SQLException e) {
            log.error("method=delete, SQL error: {}", e.getMessage());
        }
        return -1;
    }

    public List<Map<String, Object>> selectRows() {
//        PreparedStatement pstmt = null;
//        ResultSet rs = null;
        try(PreparedStatement pstmt = con.prepareStatement(query.toString())) {
//            pstmt = con.prepareStatement(query.toString());
            ResultSet rs = pstmt.executeQuery();
            if(rs.next()) {
                ResultSetMetaData rsmd = rs.getMetaData();
                int columnCount = rsmd.getColumnCount();
                List<Map<String, Object>> rows = new ArrayList<>();
                do {
                    Map<String, Object> row = new java.util.HashMap<>();
                    for (int i = 1; i <= columnCount; i++) {
                        String columnName = rsmd.getColumnName(i);
                        Object columnValue = rs.getObject(i);
                        row.put(columnName, columnValue);
                    }
                    rows.add(row);
                } while (rs.next());
                return rows;
            }
        }catch (SQLException e){
            log.error("method=selectRows, SQL error: {}", e.getMessage());
            return null;
        }
        return null;
    }

    public Map<String, Object> selectRow() {
//        PreparedStatement pstmt = null;
//        ResultSet rs = null;
        try(PreparedStatement pstmt = con.prepareStatement(query.toString())) {
//            pstmt = con.prepareStatement(query.toString());
            ResultSet rs = pstmt.executeQuery();
            if(rs.next()) {
                ResultSetMetaData rsmd = rs.getMetaData();
                int columnCount = rsmd.getColumnCount();
                Map<String, Object> row = new java.util.HashMap<>();
                for (int i = 1; i <= columnCount; i++) {
                    String columnName = rsmd.getColumnName(i);
                    Object columnValue = rs.getObject(i);
                    row.put(columnName, columnValue);
                }
                return row;
            }
        }catch (SQLException e) {
            log.error("method=selectRow, SQL error: {}", e.getMessage());
            return null;
        }
        return null;
    }

    public LocalDateTime selectDatetime() {
//        PreparedStatement pstmt = null;
//        ResultSet rs = null;
        try(PreparedStatement pstmt = con.prepareStatement(query.toString())) {
//            pstmt = con.prepareStatement(query.toString());
            ResultSet rs = pstmt.executeQuery();
            if(rs.next()) {
                return rs.getTimestamp(1).toLocalDateTime();
            }
        } catch (SQLException e) {
            log.error("method=selectDatetime, SQL error: {}", e.getMessage());
            throw new RuntimeException(e);
        }

        return null;
    }

    public long selectLong() {
//        PreparedStatement pstmt = null;
//        ResultSet rs = null;
        try(PreparedStatement pstmt = con.prepareStatement(query.toString())) {
//            pstmt = con.prepareStatement(query.toString());
            for (int i = 0; i < params.size(); i++) {
                pstmt.setObject(i + 1, params.get(i));//쿼리문에 파라미터를 설정
            }
            ResultSet rs = pstmt.executeQuery();
            if(rs.next()) {
                return rs.getLong(1);//쿼리 결과의 첫 번째 컬럼 값을 long 타입으로 가져오는 메서드
            }
        } catch (SQLException e) {
            log.error("method=selectLong, SQL error: {}", e.getMessage());
        }
        return -1L;
    }

    public String selectString() {
//        PreparedStatement pstmt = null;
//        ResultSet rs = null;
        try (PreparedStatement pstmt = con.prepareStatement(query.toString())) {
//            pstmt = con.prepareStatement(query.toString());
            ResultSet rs = pstmt.executeQuery();
            if(rs.next()) {
                return rs.getString(1);//쿼리 결과의 첫 번째 컬럼 값을 String 타입으로 가져오는 메서드
            }
        } catch (SQLException e) {
            log.error("method=selectString, SQL error: {}", e.getMessage());
        }
        return null;
    }

    public Boolean selectBoolean() {
//        PreparedStatement pstmt = null;
//        ResultSet rs = null;
        try(PreparedStatement pstmt = con.prepareStatement(query.toString())) {
//            pstmt = con.prepareStatement(query.toString());
            ResultSet rs = pstmt.executeQuery();
            if(rs.next()) {
                return rs.getBoolean(1);//쿼리 결과의 첫 번째 컬럼 값을 Boolean 타입으로 가져오는 메서드
            }
        } catch (SQLException e) {
            log.error("method=selectBoolean, SQL error: {}", e.getMessage());
        }
        return null;
    }

    public List<Long> selectLongs(){
//        PreparedStatement pstmt = null;
//        ResultSet rs = null;
        List<Long> longList = new ArrayList<>();
        try(PreparedStatement pstmt = con.prepareStatement(query.toString())) {
//            pstmt = con.prepareStatement(query.toString());
            for (int i = 0; i < params.size(); i++) {
                pstmt.setObject(i + 1, params.get(i));//쿼리문에 파라미터를 설정
            }
            ResultSet rs = pstmt.executeQuery();
            while(rs.next()) {
                longList.add(rs.getLong(1));
            }
            return longList;
        } catch (SQLException e) {
            log.error("method=selectLongs, SQL error: {}", e.getMessage());
        }
        return null;
    }

    //<T>는 이 메서드가 타입 매개변수 T를 사용한다는 선언
    public <T> List<T> selectRows(Class<T> tClass){
        List<T> tList = new ArrayList<>();
        try (PreparedStatement preparedStatement = con.prepareStatement(query.toString())) {
            ResultSet resultSet = preparedStatement.executeQuery();
            //ResultSetMetaData는 SQL 쿼리 결과(ResultSet)에 대한 "메타데이터", 즉 **"결과셋의 구조"**를 알려주는 객체
            //resultSet이 가진 열(column) 들에 대한 정보를 resultSetMetaData 객체로 가져온다.
            ResultSetMetaData resultSetMetaData = resultSet.getMetaData();
            int columnCount = resultSetMetaData.getColumnCount();

            while (resultSet.next()) {
                try {
                    //T 타입의 클래스 객체를 생성.
                    T instance = tClass.getDeclaredConstructor().newInstance();
                    for (int i = 1; i <= columnCount; i++) {
                        String columnName = resultSetMetaData.getColumnName(i);
                        Object columnValue = resultSet.getObject(i);

                        //Field class : Java 리플렉션 API의 클래스. 필드(멤버 변수)를 다룰 수 있게 해줌.
                        //클래스(tClass)로부터 이름이 columnName인 필드를 찾음.
                        Field field = tClass.getDeclaredField(columnName);
                        field.setAccessible(true);//private 필드라도 접근을 허용하도록 설정
                        field.set(instance, columnValue);//필드에 값을 할당
                    }
                    tList.add(instance);
                } catch (Exception e) {
                    log.error("Error creating instance of {}: {}", tClass.getName(), e.getMessage());
                }
            }
            return tList;
        } catch (SQLException e) {
            log.error("method=selectRows, SQL error: {}", e.getMessage());
        }
        return null;
    }

    //T 타입의 객체를 반환하는 메서드
    //selectRows 메서드를 호출하여 결과를 가져오고, 첫 번째 결과를 반환
    public <T> T selectRow(Class<T> tClass){
        try{
            List<T> results = selectRows(tClass);
            if (results != null && !results.isEmpty()) {
                return results.get(0); // 첫 번째 결과를 반환
            }
        } catch (Exception e) {
            log.error("method=selectRow, Error: {}", e.getMessage());
        }
        return null; // 결과가 없거나 오류가 발생한 경우 null 반환
    }
}

