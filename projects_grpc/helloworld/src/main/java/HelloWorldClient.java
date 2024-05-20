import io.grpc.Channel;
import io.grpc.Grpc;
import io.grpc.InsecureChannelCredentials;
import io.grpc.ManagedChannel;
import io.grpc.StatusRuntimeException;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;

import helloworld.*;

/**
 * A simple client that requests a greeting from the {@link HelloWorldServer}.
 */
public class HelloWorldClient {
    private static final Logger logger = Logger.getLogger(HelloWorldClient.class.getName());

    private static GreeterGrpc.GreeterBlockingStub blockingStub;

    /**
     * Greet server. If provided, the first element of {@code args} is the name to use in the
     * greeting. The second argument is the target server.
     */
    public static void main(String[] args) throws Exception {
        String user = "world";
        // Access a service running on the local machine on port 50051
        String target = "localhost:50051";
        
        // Create a communication channel to the server, known as a Channel. Channels are thread-safe
        // and reusable. It is common to create channels at the beginning of your application and reuse
        // them until the application shuts down.
        //
        // For the example we use plaintext insecure credentials to avoid needing TLS certificates. To
        // use TLS, use TlsChannelCredentials instead.
        ManagedChannel channel = Grpc.newChannelBuilder(target, InsecureChannelCredentials.create())
            .build();
        try {
            blockingStub = GreeterGrpc.newBlockingStub(channel);

            /** Say hello to server. */
            logger.info("Will try to greet " + user + " ...");
            HelloRequest request = HelloRequest.newBuilder().setName(user).build();
            HelloReply response;
            try {
                response = blockingStub.sayHello(request); // 서버로 메소드 호출
            } catch (StatusRuntimeException e) { // 네트워크 관련 Exception 처리 필요
                logger.log(Level.WARNING, "RPC failed: {0}", e.getStatus());
                return;
            }
            logger.info("Greeting: " + response.getMessage());
        } finally {
            // ManagedChannels use resources like threads and TCP connections. To prevent leaking these
            // resources the channel should be shut down when it will no longer be used. If it may be used
            // again leave it running.
            channel.shutdownNow().awaitTermination(5, TimeUnit.SECONDS);
        }
    }
}
