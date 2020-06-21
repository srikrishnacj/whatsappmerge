package xyz.cjcj.whatsappmerge.jdbc;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.UUID;

public class FileUtil {
    public static Path clone(Path workingDir, String fileName) throws IOException {
        Path source = Paths.get(workingDir.toString(), fileName);
        Path destination = Paths.get(workingDir.toString(), UUID.randomUUID().toString() + ".db");
        Files.copy(source, destination);
        return destination;
    }

    public static void delete(Path path) throws IOException {
        if (path != null) Files.deleteIfExists(path);
    }
}
