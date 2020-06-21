package xyz.cjcj.whatsappmerge.jdbc;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.util.StringUtils;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;
import java.util.stream.Collectors;

public class MainProgram {
    private static final Logger log = LoggerFactory.getLogger(MainProgram.class);
    List<Table> tablesToCopy = new LinkedList<>();

    {
        tablesToCopy.add(new Table("messages", "_id", "INT", "message_row_id", "key_id"));
        tablesToCopy.add(new Table("chat", "_id", "INT", "chat_row_id", "jid_row_id"));
        tablesToCopy.add(new Table("chat_list", "_id", "INT", "", "key_remote_jid"));
        tablesToCopy.add(new Table("jid", "_id", "INT", "jid_row_id", "raw_string"));

        tablesToCopy.add(new Table("message_thumbnails", "key_id", "STRING", "", "key_id"));
        tablesToCopy.add(new Table("message_media", "message_row_id", "INT", "", "message_row_id"));
        tablesToCopy.add(new Table("media_refs", "_id", "INT", "", "_id"));
    }

    public static void main(String args[]) throws IOException {
        Path workingDir = Paths.get("C:\\Users\\Cj\\Desktop\\WhatsApp\\App Data Msgstore backups");

//        List<String> mergeDataBases = Arrays.asList("v1.db", "v2.db", "v3.db", "v4.db");
        List<String> mergeDataBases = Arrays.asList("v1.db", "v2.db");

        Path sourceDb = null;
        Path destinationDb = null;

        Iterator<String> dbIt = mergeDataBases.iterator();
        destinationDb = FileUtil.clone(workingDir, dbIt.next());

        do {
            sourceDb = destinationDb;

            destinationDb = FileUtil.clone(workingDir, dbIt.next());

            MainProgram mainProgram = new MainProgram();
            mainProgram.merge(sourceDb, destinationDb);

            FileUtil.delete(sourceDb);
        } while (dbIt.hasNext());

        Path finalDestination = Paths.get(workingDir.toString(), "msgstore.db");
        Files.deleteIfExists(finalDestination);
        Files.copy(destinationDb, finalDestination);
        Files.deleteIfExists(destinationDb);
    }

    public void merge(Path sourceDatabase, Path destinationDatabase) {
        log.info("*******************************************************************");
        log.info("Trying to merge databases {} > {}", sourceDatabase.getFileName(), destinationDatabase.getFileName());
        log.info("*******************************************************************");
        SqliteConnection connection = new SqliteConnection(destinationDatabase.toString());
        connection.attach(sourceDatabase.toString());

        for (Table table : tablesToCopy) {
            copyTable(connection, table.getName(), table.getIdColumn(), table.getIdColumnType(), table.getIdPostFix(), table.getDiffColumn());
        }

        connection.detach();
        connection.close();
        log.info("*******************************************************************");
        log.info("Merge Complete Databases {} > {}", sourceDatabase.getFileName(), destinationDatabase.getFileName());
        log.info("*******************************************************************");
    }

    public void copyTable(SqliteConnection connection, String tableName, String idColumn, String idColumnType, String foriginIdPostFix, String diffColumn) {
        PostOffsetIdCallback callback = (oldId, newId) -> {
            if (StringUtils.isEmpty(foriginIdPostFix)) return;

            List<String> tablesToUpdate = tablesToCopy
                    .stream()
                    .map(table -> table.getName())
                    .filter(tn -> tn.equals(tableName) == false)
                    .map(tn -> "TOMERGE." + tn)
                    .collect(Collectors.toList());

            SqliteUtil.searchColumnWithPostFixAndReplaceColumnValueInTables(connection, tablesToUpdate, foriginIdPostFix, oldId, newId);
        };

        SqliteUtil.copyRecords(connection, tableName, idColumn, idColumnType, diffColumn, callback);
    }
}
