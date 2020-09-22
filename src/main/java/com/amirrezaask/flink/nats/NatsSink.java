package com.snapp.qazi.consumer;

import io.nats.client.Connection;
import io.nats.client.Nats;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

public class NatsSink<T> extends RichSinkFunction<T> {
  private volatile boolean isRunning = true;
  private final NatsConfig config;
  private transient Connection connection;
  
  public NatsSink(NatsConfig conf) throws Exception {
    this.config = conf;
  }

    @Override
    public void open(Configuration parameters) throws Exception {
        connection = Connection.connect(this.config.getAsJava_NatsProperties());
    }

    @Override
    public void close() throws Exception {
        connection.close(true);
        connection = null;
    }
  @Override
  public void invoke(T value, Context context) throws Exception {
    this.connection.publish(this.subject, value.toString().getBytes());
  }
}
