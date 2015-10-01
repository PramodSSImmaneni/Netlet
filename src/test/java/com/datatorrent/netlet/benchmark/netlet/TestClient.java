package com.datatorrent.netlet.benchmark.netlet;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;

import com.datatorrent.netlet.AbstractClient;
import com.datatorrent.netlet.DefaultEventLoop;

/**
 * Created by pramod on 10/1/15.
 */
public class TestClient
{

  public void execute() throws IOException, InterruptedException
  {
    byte[] b = new byte[8192];
    DefaultEventLoop eventLoop = DefaultEventLoop.createEventLoop("Client");
    eventLoop.start();
    TestListener client = new TestListener();
    eventLoop.connect(new InetSocketAddress("node16.morado.com", 9045), client);
    long startTime = System.currentTimeMillis();
    for (int i = 0; i < 1000000; ++i) {
      while(!client.send(b)) {
        Thread.sleep(0);
      }
    }
    while (!client.isDone()) {
      Thread.sleep(1);
    }
    eventLoop.disconnect(client);
    long duration = System.currentTimeMillis() - startTime;
    eventLoop.stop();
    System.out.println("Duration " + duration);
  }

  private static class TestListener extends AbstractClient
  {
    private ByteBuffer buffer = ByteBuffer.allocateDirect(8192);

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

  public static void main(String[] args) throws IOException, InterruptedException
  {
    new TestClient().execute();
  }
}
