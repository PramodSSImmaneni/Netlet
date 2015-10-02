package com.datatorrent.netlet.benchmark.netlet;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;
import java.nio.channels.SocketChannel;

import com.datatorrent.netlet.AbstractClient;
import com.datatorrent.netlet.DefaultEventLoop;
import com.datatorrent.netlet.EventLoop;
import com.datatorrent.netlet.Listener.ClientListener;

/**
 * Created by pramod on 10/1/15.
 */
public class TestClient
{

  public void execute() throws IOException, InterruptedException
  {
    byte[] b = new byte[1400];
    DefaultEventLoop eventLoop = DefaultEventLoop.createEventLoop("Client");
    eventLoop.start();
    TestListener client = new TestListener();
    //TestListenerDirect client = new TestListenerDirect();
    eventLoop.connect(new InetSocketAddress("node16.morado.com", 9045), client);
    long startTime = System.currentTimeMillis();
    for (int i = 0; i < 6000000; ++i) {
      while(!client.send(b)) {
        Thread.sleep(0);
      }
    }
    while (!client.isDone()) {
      Thread.sleep(1);
    }
    eventLoop.disconnect(client);
    long duration = System.currentTimeMillis() - startTime;
    System.out.println("Duration " + duration);
    eventLoop.stop();
  }

  private static class TestListener extends AbstractClient
  {
    private ByteBuffer buffer = ByteBuffer.allocateDirect(1400);

    @Override
    public ByteBuffer buffer()
    {
      return buffer;
    }

    @Override
    public void read(int len)
    {
    }

    public boolean isDone() {
      return sendBuffer4Polls.isEmpty() && (writeBuffer.position() == 0);
    }
  }

  private static class TestListenerDirect implements ClientListener
  {

    private SelectionKey key;
    ByteBuffer buffer = ByteBuffer.allocateDirect(1400);
    long nwrite;
    long check = 1000000000;
    long startTime;

    @Override
    public void read() throws IOException
    {

    }

    @Override
    public void write() throws IOException
    {
      SocketChannel channel = (SocketChannel)key.channel();
      nwrite += channel.write(buffer);
      if (buffer.remaining() == 0) {
        buffer.clear();
      }
      if (nwrite >= check) {
        System.out.println("Write " + nwrite + " duration " + (System.currentTimeMillis() - startTime));
        check += 1000000000;
      }
    }

    @Override
    public void connected()
    {
      startTime = System.currentTimeMillis();
    }

    @Override
    public void disconnected()
    {

    }

    @Override
    public void handleException(Exception exception, EventLoop eventloop)
    {

    }

    @Override
    public void registered(SelectionKey key)
    {
      this.key = key;
    }

    @Override
    public void unregistered(SelectionKey key)
    {

    }

    public boolean isDone() {
      return false;
    }
  }

  public static void main(String[] args) throws IOException, InterruptedException
  {
    new TestClient().execute();
  }
}
