package com.datatorrent.netlet.datagram;

import java.nio.channels.SelectableChannel;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.spi.AbstractSelectionKey;

/**
 * Created by pramod on 6/9/15.
 */
public class SelectionKeyChannel extends AbstractSelectionKey
{
  @Override
  public SelectableChannel channel()
  {
    return null;
  }

  @Override
  public Selector selector()
  {
    return null;
  }

  @Override
  public int interestOps()
  {
    return 0;
  }

  @Override
  public SelectionKey interestOps(int ops)
  {
    return null;
  }

  @Override
  public int readyOps()
  {
    return 0;
  }
}
