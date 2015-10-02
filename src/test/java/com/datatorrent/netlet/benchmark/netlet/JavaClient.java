package com.datatorrent.netlet.benchmark.netlet;

import java.io.IOException;
import java.io.OutputStream;
import java.net.Socket;

/**
 * Created by pramod on 10/2/15.
 */
public class JavaClient
{
  public void execute() throws IOException
  {
    byte[] b = new byte[1400];
    Socket socket = new Socket("node16.morado.com", 9056);
    OutputStream out = null;
    long startTime = System.currentTimeMillis();
    try {
      out = socket.getOutputStream();
      for (int i = 0; i < 6000000; ++i) {
        out.write(b);
      }
    } finally {
      if (out != null) {
        out.close();
      }
      System.out.println("Duration " + (System.currentTimeMillis() - startTime));
    }
  }

  public static void main(String[] args) throws IOException
  {
    new JavaClient().execute();
  }
}
