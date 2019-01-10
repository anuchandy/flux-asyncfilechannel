package flux.asyncfilechannel;

import org.reactivestreams.Subscription;
import reactor.core.CoreSubscriber;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.nio.ByteBuffer;
import java.nio.channels.AsynchronousFileChannel;
import java.nio.channels.CompletionHandler;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;

public final class FluxUtil {
    /**
     * Writes the bytes emitted by a Flux to an AsynchronousFileChannel
     * starting at the given position in the file.
     *
     * @param content the Flux content
     * @param outFile the file channel
     * @param position the position in the file to begin writing
     * @return a Void Mono which performs the write operation when subscribed
     */
    public static Mono<Void> writeFile(Flux<ByteBuffer> content, AsynchronousFileChannel outFile, long position) {
        // TODO: Get this reviewed by smaldini, specifically MonoSink::success & MonoSink::error invocation from call-back handler.
        //
        Mono<Void> voidMono = Mono.create(monoSink -> content.subscribe(new CoreSubscriber<ByteBuffer>() {
            // volatile ensures that writes to these fields by one thread will be immediately visible to other threads.
            // An I/O pool thread will write to isWriting and read isCompleted,
            // while another thread may read isWriting and write to isCompleted.
            volatile boolean isWriting = false;
            volatile boolean isCompleted = false;
            volatile Subscription subscription;
            volatile long pos = position;
            AtomicInteger reqCount = new AtomicInteger(0);

            private Consumer<Void> requestDelegate = new Consumer<Void>() {
                @Override
                public void accept(Void aVoid) {
                    reqCount.incrementAndGet();
                    subscription.request(1);
                }
            };

            @Override
            public void onSubscribe(Subscription s) {
                subscription = s;
                requestDelegate.accept(null);
            }

            @Override
            public void onNext(ByteBuffer bytes) {
                isWriting = true;
                outFile.write(bytes, pos, null, onWriteCompleted);
            }

            @Override
            public void onError(Throwable throwable) {
                subscription.cancel();
                monoSink.error(throwable);
            }

            @Override
            public void onComplete() {
                isCompleted = true;
                if (!isWriting) {
                    // Passing null to MonoSink<T>::success is accepted by standard implementations.
                    monoSink.success(null);
                }
            }

            CompletionHandler<Integer, Object> onWriteCompleted = new CompletionHandler<Integer, Object>() {
                @Override
                public void completed(Integer bytesWritten, Object attachment) {
                    isWriting = false;
                    if (isCompleted) {
                        // Passing null to MonoSink<T>::success is accepted by standard implementations.
                        monoSink.success(null);
                    } else {
                        pos += bytesWritten;
                        requestDelegate.accept(null);
                    }
                }

                @Override
                public void failed(Throwable exc, Object attachment) {
                    subscription.cancel();
                    monoSink.error(exc);
                }
            };
        }));
        return voidMono;
    }
}
