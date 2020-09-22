package com.mattring.flink.nats;


import com.google.common.base.Splitter;
import com.google.common.base.Strings;
import java.util.Properties;
import java.util.concurrent.LinkedBlockingQueue;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.nats.Connection;
import org.nats.MsgHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class NatsSource implements RichSourceFunction<String> {
    private volatile boolean isRunning = true;
    private transient Connection connection;
    private final NatsConfig config;

    public NatsSource(NatsConfig conf) throws Exception {
        this.config = conf;
        this.connection = Nats.connect(String.format("nats://%s:%s", this.host, this.port));
    }

    @Override
    public void run(SourceFunction.SourceContext<String> ctx) throws Exception {
        
        isRunning = true;

        if (isRunning) {
            
            final Properties natsProps = this.config.getAsJava_NatsProperties();

            try (Connection natsConn = Connection.connect(natsProps)) {

                final LinkedBlockingQueue<String> inbox = new LinkedBlockingQueue<>();
                final MsgHandler msgHandler = new MsgHandler() {
                    @Override
                    public void execute(String msg, String reply, String subject) {
                        final boolean enqueued = inbox.offer(msg);
                    }
                };
                final int natsSubId = natsConn.subscribe(natsConfig.getTopic(), msgHandler);

                while (isRunning) {
                    final String msg = inbox.poll();
                    if (msg != null) {
                        if (delimiter != null) {
                            final Iterable<String> msgPartIterable = 
                                    Splitter.on(delimiter)
                                            .trimResults()
                                            .omitEmptyStrings()
                                            .split(msg);
                            for (String msgPart : msgPartIterable) {
                                ctx.collect(msg);
                            }
                        } else {
                            ctx.collect(msg);
                        }
                    }
                }
                
                natsConn.unsubscribe(natsSubId);
            }
        }
    }

    @Override
    public void cancel() {
        isRunning = false;
    }
}
