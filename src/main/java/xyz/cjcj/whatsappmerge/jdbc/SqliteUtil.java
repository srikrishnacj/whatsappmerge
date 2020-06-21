package xyz.cjcj.whatsappmerge.jdbc;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import xyz.cjcj.whatsappmerge.model.RecordDE;

import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.*;
import java.util.stream.Collectors;

public class SqliteUtil {
    private static final Logger log = LoggerFactory.getLogger(SqliteUtil.class);
    private static Map<String, Object> cache = new HashMap<>();

    public static List<String> getAllColumnsNamesForTable(SqliteConnection connection, String tableName) {
        String cacheKey = "ALL_COLUMNS:" + tableName;
        if (cache.containsKey(cacheKey)) {
            return (List<String>) cache.get(cacheKey);
        }

        String query = "SELECT * FROM :table LIMIT 1";
        query = query.replaceAll(":table", tableName);


        List<String> columns = new LinkedList<>();
        try {
            ResultSet rs = connection.executeQuery(query);
            ResultSetMetaData oldMetaData = rs.getMetaData();
            for (int i = 1; i < oldMetaData.getColumnCount() + 1; i++)
                columns.add(oldMetaData.getColumnName(i));
            rs.close();
            cache.put(cacheKey, columns);
            return columns;
        } catch (Exception e) {
            throw new RuntimeException("Unable to read column names form table: " + tableName, e);
        }
    }

    public static List<String> getCommonColumnsFor(SqliteConnection connection, String tableName) {
        String cacheKey = "COMMON_COLUMNS:" + tableName;
        if (cache.containsKey(cacheKey)) {
            return (List<String>) cache.get(cacheKey);
        }

        String oldTableName = "TOMERGE." + tableName;

        List<String> columns1 = getAllColumnsNamesForTable(connection, oldTableName);
        log.info("old table: {} has {} columns", tableName, columns1.size());

        List<String> columns2 = getAllColumnsNamesForTable(connection, tableName);
        log.info("new table: {} has {} columns", tableName, columns2.size());

        columns1.retainAll(columns2);
        log.info("Found {} common columns for table: {} between latest and old database", columns1.size(), tableName);

        cache.put(cacheKey, columns1);
        return columns1;
    }

    public static List<String> readSingleColumnAsList(SqliteConnection connection, String query, String columnName) {
        List<String> data = new LinkedList<>();
        try {
            ResultSet rs = connection.executeQuery(query);
            while (rs.next()) {
                data.add(rs.getString(columnName));
            }
            rs.close();
        } catch (Exception e) {
            throw new RuntimeException("Unable to read single column data for query: " + query, e);
        }
        return data;
    }

    public static String readSingleValue(SqliteConnection connection, String query, String columnName) {
        ResultSet rs = connection.executeQuery(query);
        String data = null;
        try {
            data = rs.getString(columnName);
            rs.close();
        } catch (SQLException throwables) {
            throw new RuntimeException("Unable to read single Value for query: " + query, throwables);
        }
        return data;
    }

    public static String totalNoOfRecordsInTable(SqliteConnection connection, String tableName, String idColumn) {
        String query = "select COUNT(:idColumn) from :table";
        query = query.replaceAll(":table", tableName);
        query = query.replaceAll(":idColumn", idColumn);
        String count = readSingleValue(connection, query, "COUNT(" + idColumn + ")");
        return count;
    }

    public static void copyRecords(SqliteConnection destinationConnection, String tableName, String idColumn, String idColumnType, String diffColumnName, PostOffsetIdCallback callback) {
        String oldTableName = "TOMERGE." + tableName;
        log.info("============================= Copying Table: {} ======================================", tableName);

        String sourceTableIdCount = totalNoOfRecordsInTable(destinationConnection, oldTableName, idColumn);
        log.info("{} records found in source table: {}", sourceTableIdCount, tableName);

        String destinationTableIdsCount = totalNoOfRecordsInTable(destinationConnection, tableName, idColumn);
        log.info("{} records found in destination table: {}", destinationTableIdsCount, tableName);

        String query = "select :idColumn from TOMERGE.:table where :column not in (select :column from :table)";
        query = query.replaceAll(":table", tableName);
        query = query.replaceAll(":column", diffColumnName);
        query = query.replaceAll(":idColumn", idColumn);
        List<String> idsToCopy = SqliteUtil.readSingleColumnAsList(destinationConnection, query, idColumn);
        log.info("{} records need to be copied from source to destination table: {}", idsToCopy.size(), tableName);

        if (idColumn.equals("_id")) {
            String ids = joinIds(idsToCopy, idColumnType);
            String.join(", ", idsToCopy);
            query = "select :idColumn from :table where _id in (:ids)";
            query = query.replaceAll(":table", tableName);
            query = query.replaceAll(":idColumn", idColumn);
            query = query.replaceAll(":ids", ids);
            List<String> duplicateIds = SqliteUtil.readSingleColumnAsList(destinationConnection, query, "_id");
            log.info("{} duplicate ids found in destination table: {} ids: {}", duplicateIds.size(), tableName, duplicateIds);

            if (duplicateIds.isEmpty() == false) {
                log.info("Trying to offset duplicate ids in source tables");

                query = "select MAX(_id) from :table";
                query = query.replaceAll(":table", tableName);
                int newTableMaxId = Integer.parseInt(readSingleValue(destinationConnection, query, "MAX(_id)")) + 1;

                query = "select MAX(_id) from TOMERGE.:table";
                query = query.replaceAll(":table", tableName);
                int oldTableMaxId = Integer.parseInt(readSingleValue(destinationConnection, query, "MAX(_id)")) + 1;

                int offset = Math.max(newTableMaxId, oldTableMaxId);

                for (String id : duplicateIds) {
                    offset++;
                    replaceColumnValue(destinationConnection, oldTableName, "_id", id, offset + "");
                    callback.postOffset(id, offset + "");
                }
            }
        }


        if (idsToCopy.isEmpty()) return;

        List<String> columns = SqliteUtil.getCommonColumnsFor(destinationConnection, tableName);
        String columnStr = String.join(", ", columns);
        log.info("Copying records from source to destination table: {}", tableName);


        Set<List<String>> idChunks = StreamUtil.chunked(idsToCopy, 5000);
        idChunks.forEach(idChunck -> {
            try {
                String tempQuery = "INSERT INTO :table (:columns) SELECT :columns FROM TOMERGE.:table where :idColumn IN (:ids)";
                tempQuery = tempQuery.replaceAll(":table", tableName);
                tempQuery = tempQuery.replaceAll(":columns", columnStr);
                tempQuery = tempQuery.replaceAll(":idColumn", idColumn);
                tempQuery = tempQuery.replaceAll(":ids", joinIds(idChunck, idColumnType));
                destinationConnection.executeUpdate(tempQuery);
            } catch (RuntimeException e) {
                log.info("Error while coping for table: {} ids: {}", tableName, idChunck);
                throw e;
            }
        });


        destinationTableIdsCount = totalNoOfRecordsInTable(destinationConnection, tableName, idColumn);
        log.info("{} records found in destination table: {} after copy", destinationTableIdsCount, tableName);
        log.info("Copy records complete for table: {}", tableName);
    }

    public static void searchColumnWithPostFixAndReplaceColumnValueInTables(SqliteConnection connection, List<String> tableNames, String columnPostFix, String oldValue, String newValue) {
        for (String tableName : tableNames) {
            List<String> columns = getAllColumnsNamesForTable(connection, tableName);
            for (String column : columns) {
                if (column.endsWith(columnPostFix)) {
                    replaceColumnValue(connection, tableName, column, oldValue, newValue);
                }
            }
        }
    }

    public static void replaceColumnValue(SqliteConnection connection, String tableName, String column, String oldValue, String newValue) {
        String query = "UPDATE :table SET :column = :newValue where :column = :oldValue";
        query = query.replaceAll(":table", tableName);
        query = query.replaceAll(":column", column);
        query = query.replaceAll(":newValue", newValue);
        query = query.replaceAll(":oldValue", oldValue);
        connection.executeUpdate(query);
    }

    private static String joinIds(List<String> list, String idType) {
        if (idType.equals("INT")) {
            return String.join(", ", list);
        } else {
            list = list.stream().map(n -> "'" + n + "'").collect(Collectors.toList());
            return String.join(", ", list);
        }
    }
}
