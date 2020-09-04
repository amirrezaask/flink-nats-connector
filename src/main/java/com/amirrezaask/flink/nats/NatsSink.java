package com.amirrezaask.flink.nats;

import io.nats.client.Connection;
import io.nats.client.Nats;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

public class NatsSink<T> extends RichSinkFunction<T> {
  private volatile boolean isRunning = true;
  private String host;
  private String port;
  private String subject;
  private Connection connection;
  
  public NatsSink(String host, String port, String topic) throws Exception {
    this.host = host;
    this.port = port;
    this.subject = topic;
    this.connection = Nats.connect(String.format("nats://%s:%s", this.host, this.port));
  }

  @Override
  public void invoke(T value, Context context) throws Exception {
    this.connection.publish(this.subject, value.toString().getBytes());
  }
}
