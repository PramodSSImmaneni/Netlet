package com.datatorrent.netlet.benchmark.netlet;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;

import com.datatorrent.netlet.AbstractClient;
import com.datatorrent.netlet.DefaultEventLoop;
import com.datatorrent.netlet.EventLoop;
import com.datatorrent.netlet.Listener.ClientListener;
import com.datatorrent.netlet.Listener.ServerListener;

/**
 * Created by pramod on 10/1/15.
 */
public class TestServer
{
  public void execute() throws IOException, InterruptedException
  {
    DefaultEventLoop eventLoop = DefaultEventLoop.createEventLoop("Server");
    Thread eventTh = eventLoop.start();
    eventLoop.start("0.0.0.0", 9045, new TestServerListener());
    eventTh.join();
    eventLoop.stop();
  }

  private static class TestServerListener implements ServerListener {

    @Override
    public ClientListener getClientConnection(SocketChannel client, ServerSocketChannel server)
    {
      //return new TestListener();
      return new TestListenerDirect();
    }

    @Override
    public void handleException(Exception exception, EventLoop eventloop)
    {

    }

    @Override
    public void registered(SelectionKey key)
    {

    }

    @Override
    public void unregistered(SelectionKey key)
    {

    }
  }

  private static class TestListener extends AbstractClient
  {
    private ByteBuffer buffer = ByteBuffer.allocateDirect(1400);
    private long nread;
    private long check = 1000000000;
    private long startTime = System.currentTimeMillis();

    @Override
    public ByteBuffer buffer()
    {
      return buffer;
    }

    @Override
    public void read(int len)
    {
      nread += len;
      if (nread >= check) {
        System.out.println("Number " + nread + " duration " + (System.currentTimeMillis() - startTime));
        check += 1000000000;
      }
      buffer.clear();
    }
  }

  private static class TestListenerDirect implements ClientListener {

    private SelectionKey key;
    ByteBuffer buffer = ByteBuffer.allocateDirect(1400);

    private long nread;
    private long check = 1000000000;
    private long startTime;

    @Override
    public void read() throws IOException
    {
      SocketChannel channel = (SocketChannel)key.channel();
      nread += channel.read(buffer);
      if (buffer.remaining() == 0) {
        buffer.clear();
      }
      if (nread >= check) {
        System.out.println("Number " + nread + " duration " + (System.currentTimeMillis() - startTime));
        check += 1000000000;
      }
    }

    @Override
    public void write() throws IOException
    {

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
  }

  public static void main(String[] args) throws IOException, InterruptedException
  {
    new TestServer().execute();
  }

}
