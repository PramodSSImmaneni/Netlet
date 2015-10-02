package com.datatorrent.netlet.benchmark.netlet;

import java.io.IOException;
import java.io.InputStream;
import java.net.ServerSocket;
import java.net.Socket;

/**
 * Created by pramod on 10/2/15.
 */
public class JavaServer
{
  public void execute() throws IOException
  {
    byte[] b = new byte[1400];
    ServerSocket serverSocket = new ServerSocket(9056);
    while(true) {
      Socket socket = serverSocket.accept();
      InputStream in = null;
      long startTime = System.currentTimeMillis();
      long nread = 0;
      long check = 1000000000;
      int rb;
      try {
        in = socket.getInputStream();
        while ((rb = in.read(b)) != -1) {
          nread += rb;
          if (nread >= check) {
            System.out.println("Read data " + nread + " duration " + (System.currentTimeMillis() - startTime));
            check += 1000000000;
          }
        }
      } finally {
        if (in != null) {
          in.close();
        }
      }
      socket.close();
    }
  }

  public static void main(String[] args) throws IOException
  {
    new JavaServer().execute();
  }

}
