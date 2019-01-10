package flux.asyncfilechannel;

import io.reactivex.Completable;
import io.reactivex.CompletableSource;
import io.reactivex.Flowable;
import org.junit.Test;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.AsynchronousFileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.security.MessageDigest;
import java.util.Random;
import java.util.concurrent.Callable;
import io.reactivex.functions.Function;
import org.slf4j.LoggerFactory;

public class FlowableUtilTest extends TestBase {
    @Test
    public void create100MFiles() throws IOException {
        final Flowable<ByteBuffer> contentGenerator = Flowable.generate(Random::new, (random, emitter) -> {
            ByteBuffer buf = ByteBuffer.allocate(CHUNK_SIZE);
            random.nextBytes(buf.array());
            emitter.onNext(buf);
        });

        deleteRecursive(TEMP_FOLDER_PATH);

        if (Files.exists(TEMP_FOLDER_PATH)) {
            LoggerFactory.getLogger(FlowableUtilTest.class).info("Temp files directory already exists: " + TEMP_FOLDER_PATH.toAbsolutePath());
        } else {
            LoggerFactory.getLogger(FlowableUtilTest.class).info("Generating temp files in directory: " + TEMP_FOLDER_PATH.toAbsolutePath());
            Files.createDirectory(TEMP_FOLDER_PATH);
            Flowable.range(0, NUM_FILES).flatMapCompletable(new Function<Integer, Completable>() {
                @Override
                public Completable apply(Integer integer) throws Exception {
                    final int i = integer;
                    final Path filePath = TEMP_FOLDER_PATH.resolve("100m-" + i + ".dat");

                    Files.deleteIfExists(filePath);
                    Files.createFile(filePath);
                    final AsynchronousFileChannel file = AsynchronousFileChannel.open(filePath, StandardOpenOption.READ, StandardOpenOption.WRITE);
                    final MessageDigest messageDigest = MessageDigest.getInstance("MD5");

                    Flowable<ByteBuffer> fileContent = contentGenerator
                            .take(CHUNKS_PER_FILE)
                            .doOnNext(buf -> messageDigest.update(buf.array()));

                    return FlowableUtil.writeFile(fileContent, file, 0).andThen(Completable.defer(new Callable<CompletableSource>() {
                        @Override
                        public CompletableSource call() throws Exception {
                            file.close();
                            Files.write(TEMP_FOLDER_PATH.resolve("100m-" + i + "-md5.dat"), messageDigest.digest());
                            LoggerFactory.getLogger(getClass()).info("Finished writing file " + i);
                            return Completable.complete();
                        }
                    }));
                }
            }).blockingAwait();
        }
    }
}
