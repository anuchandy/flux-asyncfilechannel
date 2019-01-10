package flux.asyncfilechannel;

import org.junit.Test;
import org.slf4j.LoggerFactory;
import reactor.core.Exceptions;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.AsynchronousFileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Random;
import java.util.concurrent.atomic.AtomicInteger;

public class FluxUtilTest extends TestBase {
    @Test
    public void create100MFiles() throws IOException {
        AtomicInteger fileCreatedCount = new AtomicInteger(0);
        deleteRecursive(TEMP_FOLDER_PATH);

        if (Files.exists(TEMP_FOLDER_PATH)) {
            LoggerFactory.getLogger(FluxUtilTest.class).info("Temp files directory already exists: " + TEMP_FOLDER_PATH.toAbsolutePath());
        } else {
            LoggerFactory.getLogger(FluxUtilTest.class).info("Generating temp files in directory: " + TEMP_FOLDER_PATH.toAbsolutePath());
            Files.createDirectory(TEMP_FOLDER_PATH);
            //
            final Flux<ByteBuffer> contentGenerator = Flux.generate(Random::new, (random, synchronousSink) -> {
                ByteBuffer buf = ByteBuffer.allocate(CHUNK_SIZE);
                random.nextBytes(buf.array());
                synchronousSink.next(buf);
                return random;
            });
            //
            Flux.range(0, 10).flatMap(integer -> {
                final int i = integer;
                final Path filePath = TEMP_FOLDER_PATH.resolve("100m-" + i + ".dat");
                //
                AsynchronousFileChannel file;
                try {
                    Files.deleteIfExists(filePath);
                    Files.createFile(filePath);
                    file = AsynchronousFileChannel.open(filePath, StandardOpenOption.READ, StandardOpenOption.WRITE);
                } catch (IOException ioe) {
                    throw Exceptions.propagate(ioe);
                }
                //
                MessageDigest messageDigest;
                try {
                    messageDigest = MessageDigest.getInstance("MD5");
                } catch (NoSuchAlgorithmException nsae) {
                    throw Exceptions.propagate(nsae);
                }
                //
                Flux<ByteBuffer> fileContent = contentGenerator
                        .take(CHUNKS_PER_FILE)
                        .doOnNext(buf -> messageDigest.update(buf.array()));
                //
                return FluxUtil.writeFile(fileContent, file, 0).then(Mono.defer(() -> {
                    try {
                        file.close();
                        Files.write(TEMP_FOLDER_PATH.resolve("100m-" + i + "-md5.dat"), messageDigest.digest());
                    } catch (IOException ioe) {
                        throw Exceptions.propagate(ioe);
                    }
                    LoggerFactory.getLogger(getClass()).info("Finished writing file " + i);
                    return Mono.empty();
                }));

            }).blockLast();
        }
    }
}
