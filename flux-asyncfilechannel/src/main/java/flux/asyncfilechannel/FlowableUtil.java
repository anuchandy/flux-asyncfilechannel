package flux.asyncfilechannel;

import io.reactivex.Completable;
import io.reactivex.Flowable;
import io.reactivex.FlowableSubscriber;
import org.reactivestreams.Subscription;

import java.nio.ByteBuffer;
import java.nio.channels.AsynchronousFileChannel;
import java.nio.channels.CompletionHandler;

public final class FlowableUtil {
    /**
     * Writes the bytes emitted by a Flowable to an AsynchronousFileChannel
     * starting at the given position in the file.
     *
     * @param content the Flowable content
     * @param outFile the file channel
     * @param position the position in the file to begin writing
     * @return a Completable which performs the write operation when subscribed
     */
    public static Completable writeFile(Flowable<ByteBuffer> content, AsynchronousFileChannel outFile, long position) {
        return Completable.create(emitter -> content.subscribe(new FlowableSubscriber<ByteBuffer>() {
            // volatile ensures that writes to these fields by one thread will be immediately visible to other threads.
            // An I/O pool thread will write to isWriting and read isCompleted,
            // while another thread may read isWriting and write to isCompleted.
            volatile boolean isWriting = false;
            volatile boolean isCompleted = false;
            volatile Subscription subscription;
            volatile long pos = position;

            @Override
            public void onSubscribe(Subscription s) {
                subscription = s;
                s.request(1);
            }

            @Override
            public void onNext(ByteBuffer bytes) {
                isWriting = true;
                outFile.write(bytes, pos, null, onWriteCompleted);
            }


            CompletionHandler<Integer, Object> onWriteCompleted = new CompletionHandler<Integer, Object>() {
                @Override
                public void completed(Integer bytesWritten, Object attachment) {
                    isWriting = false;
                    if (isCompleted) {
                        emitter.onComplete();
                    }
                    //noinspection NonAtomicOperationOnVolatileField
                    pos += bytesWritten;
                    subscription.request(1);
                }

                @Override
                public void failed(Throwable exc, Object attachment) {
                    subscription.cancel();
                    emitter.onError(exc);
                }
            };

            @Override
            public void onError(Throwable throwable) {
                subscription.cancel();
                emitter.onError(throwable);
            }

            @Override
            public void onComplete() {
                isCompleted = true;
                if (!isWriting) {
                    emitter.onComplete();
                }
            }
        }));
    }

}
