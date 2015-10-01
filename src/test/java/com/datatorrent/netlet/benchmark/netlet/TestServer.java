package com.datatorrent.netlet.benchmark.netlet;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;

import com.datatorrent.netlet.AbstractClient;
import com.datatorrent.netlet.DefaultEventLoop;
import com.datatorrent.netlet.EventLoop;
import com.datatorrent.netlet.Listener.ServerListener;

/**
 * Created by pramod on 10/1/15.
 */
public class TestServer
{
  public void execute() throws IOException, InterruptedException
  {
    DefaultEventLoop eventLoop = DefaultEventLoop.createEventLoop("Client");
    Thread eventTh = eventLoop.start();
    eventLoop.start("0.0.0.0", 9045, new TestServerListener());
    eventTh.join();
    eventLoop.stop();
  }

  private static class TestServerListener implements ServerListener {

    @Override
    public ClientListener getClientConnection(SocketChannel client, ServerSocketChannel server)
    {
      return new TestListener();
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
    private ByteBuffer buffer = ByteBuffer.allocateDirect(8192);
    private long nread;
    private int numBatch = 1;
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
      if (nread >= (numBatch * 1000000)) {
        System.out.println("Number " + nread + " duration " + (System.currentTimeMillis() - startTime));
        ++numBatch;
      }
    }
  }


}
