/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package malhar.netlet;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;
import java.nio.channels.SocketChannel;
import malhar.netlet.Listener.ClientListener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * @author Chetan Narsude <chetan@malhar-inc.com>
 */
public abstract class Client implements ClientListener
{
  protected final ByteBuffer writeBuffer;
  protected final CircularBuffer<Fragment> freeBuffer;
  protected CircularBuffer<Fragment> sendBuffer;
  protected boolean write = true;
  protected SelectionKey key;

  public SelectionKey getKey()
  {
    return key;
  }

  public Client(int writeBufferSize, int sendBufferSize)
  {
    this(ByteBuffer.allocateDirect(writeBufferSize), sendBufferSize);
  }

  public Client()
  {
    this(8 * 1 * 1024, 1024 * 1024);
  }

  public Client(ByteBuffer writeBuffer, int sendBufferSize)
  {
    this.writeBuffer = writeBuffer;
    sendBuffer = new CircularBuffer<Fragment>(sendBufferSize, 10);
    freeBuffer = new CircularBuffer<Fragment>(sendBufferSize, 10);
  }

  @Override
  public void registered(SelectionKey key)
  {
    this.key = key;
    logger.debug("listener = {} and interestOps = {}", key.attachment(), Integer.toBinaryString(key.interestOps()));
  }

  @Override
  public final void read() throws IOException
  {
    SocketChannel channel = (SocketChannel)key.channel();
    int read;
    if ((read = channel.read(buffer())) > 0) {
      this.read(read);
    }
    else if (read == -1) {
      unregistered(key);
      channel.close();
    }
  }

  @Override
  public final void write() throws IOException
  {
    /*
     * at first when we enter this function, our buffer is in fill mode.
     */
    int remaining, size;
    if ((size = sendBuffer.size()) > 0 && (remaining = writeBuffer.remaining()) > 0) {
      do {
        Fragment f = sendBuffer.peekUnsafe();
        if (remaining <= f.len) {
          writeBuffer.put(f.array, f.offset, remaining);
          f.offset += remaining;
          f.len -= remaining;
          break;
        }
        else {
          writeBuffer.put(f.array, f.offset, f.len);
          remaining -= f.len;
          freeBuffer.offer(sendBuffer.pollUnsafe());
        }
      }
      while (--size > 0);
    }

    /*
     * switch to the read mode!
     */
    writeBuffer.flip();

    SocketChannel channel = (SocketChannel)key.channel();
    while ((remaining = writeBuffer.remaining()) > 0) {
      remaining -= channel.write(writeBuffer);
      if (remaining > 0) {
        /*
         * switch back to the fill mode.
         */
        writeBuffer.compact();
        return;
      }
      else if ((size = sendBuffer.size()) > 0) {
        /*
         * switch back to the write mode.
         */
        writeBuffer.clear();

        remaining = writeBuffer.capacity();
        do {
          Fragment f = sendBuffer.peekUnsafe();
          if (remaining <= f.len) {
            writeBuffer.put(f.array, f.offset, remaining);
            f.offset += remaining;
            f.len -= remaining;
            break;
          }
          else {
            writeBuffer.put(f.array, f.offset, f.len);
            remaining -= f.len;
            freeBuffer.offer(sendBuffer.pollUnsafe());
          }
        }
        while (--size > 0);

        /*
         * switch to the read mode.
         */
        writeBuffer.flip();
      }
    }

    /*
     * switch back to fill mode.
     */
    writeBuffer.clear();
    synchronized (this) {
      if (sendBuffer.isEmpty()) {
        key.interestOps(key.interestOps() & ~SelectionKey.OP_WRITE);
        write = false;
      }
    }
  }

  public void send(byte[] array) throws InterruptedException
  {
    send(array, 0, array.length);
  }

  public void send(byte[] array, int offset, int len) throws InterruptedException
  {
    //logger.debug("sending {}", Arrays.toString(Arrays.copyOfRange(array, offset, offset+len)));
    Fragment f = freeBuffer.isEmpty() ? new Fragment() : freeBuffer.pollUnsafe();
    f.array = array;
    f.offset = offset;
    f.len = len;
    sendBuffer.put(f);

    synchronized (this) {
      if (!write) {
        key.interestOps(key.interestOps() | SelectionKey.OP_WRITE);
        write = true;
      }
    }
  }

  @Override
  public void handleException(Exception cce, DefaultEventLoop el)
  {
    logger.debug("", cce);
  }

  public abstract ByteBuffer buffer();

  public abstract void read(int len);

  @Override
  public void unregistered(SelectionKey key)
  {
//    Client.this.sendBuffer = sendBuffer.getWhitehole("Client already disconnected!");
//    key.attach(new ClientListener()
//    {
//      @Override
//      public void handleException(Exception cce, DefaultEventLoop el)
//      {
//        Client.this.handleException(cce, el);
//      }
//
//      @Override
//      public void registered(SelectionKey key)
//      {
//      }
//
//      @Override
//      public void unregistered(SelectionKey key)
//      {
//      }
//
//      @Override
//      public void read() throws IOException
//      {
//      }
//
//      @Override
//      public void write() throws IOException
//      {
//        Client.this.write();
//        if (sendBuffer.isEmpty() && writeBuffer.position() == 0) {
//          Client.this.key.cancel();
//        }
//      }
//
//    });
//    key.interestOps(SelectionKey.OP_WRITE);
  }

  protected static class Fragment
  {
    byte[] array;
    int offset;
    int len;
  }

  private static final Logger logger = LoggerFactory.getLogger(Client.class);
}
