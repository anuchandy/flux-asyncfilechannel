package flux.asyncfilechannel;

import org.junit.BeforeClass;

import java.io.IOException;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.attribute.BasicFileAttributes;

public class TestBase {
    protected static Path TEMP_FOLDER_PATH;
    protected static final int NUM_FILES = 100;
    protected static final int FILE_SIZE = 1024 * 1024 * 100;
    protected static final int CHUNK_SIZE = 8192;
    protected static final int CHUNKS_PER_FILE = FILE_SIZE / CHUNK_SIZE;

    @BeforeClass
    public static void beforeClass() {
        String tempFolderPath = System.getenv("JAVA_STRESS_TEST_TEMP_PATH");
        if (tempFolderPath == null || tempFolderPath.isEmpty()) {
            tempFolderPath = "temp";
        }

        TEMP_FOLDER_PATH = Paths.get(tempFolderPath);
    }

    protected static void deleteRecursive(Path tempFolderPath) throws IOException {
        try {
            Files.walkFileTree(tempFolderPath, new SimpleFileVisitor<Path>() {
                @Override
                public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) throws IOException {
                    Files.delete(file);
                    return FileVisitResult.CONTINUE;
                }

                @Override
                public FileVisitResult postVisitDirectory(Path dir, IOException exc) throws IOException {
                    if (exc != null) {
                        throw exc;
                    }

                    Files.delete(dir);
                    return FileVisitResult.CONTINUE;
                }
            });
        } catch (NoSuchFileException ignored) {
        }
    }

}
