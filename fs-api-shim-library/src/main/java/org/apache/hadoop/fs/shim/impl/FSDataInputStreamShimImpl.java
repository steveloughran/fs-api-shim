/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.fs.shim.impl;

import java.io.Closeable;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.concurrent.atomic.AtomicBoolean;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.fs.ByteBufferReadable;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.shim.FSDataInputStreamShim;
import org.apache.hadoop.fs.shim.StandardStreamCapabilities;

import static org.apache.hadoop.fs.shim.impl.Invocation.unavailable;
import static org.apache.hadoop.fs.shim.impl.ShimUtils.getInvocation;

/**
 * invoke or emulate ByteBufferPositionedReadable.
 */
public class FSDataInputStreamShimImpl
    extends AbstractAPIShim<FSDataInputStream>
    implements FSDataInputStreamShim {

  private static final Logger LOG = LoggerFactory.getLogger(FSDataInputStreamShimImpl.class);


  /**
   * {@code ByteBufferPositionedRead.readFully()}.
   */
  private final Invocation byteBufferPositionedRead;

  /**
   * {@code ByteBufferPositionedRead.readFully()}.
   */
  private final Invocation byteBufferPositionedReadFully;
  private final AtomicBoolean byteBufferPositionedReadFunctional;

  /**
   * Constructor.
   * @param instalnce Instance being shimmed.
   */
  public FSDataInputStreamShimImpl(
      final FSDataInputStream instance) {
    super(FSDataInputStream.class, instance);
    byteBufferPositionedRead = getInvocation(getClazz(), "read",
        Long.class, ByteBuffer.class);

    byteBufferPositionedReadFully =
        byteBufferPositionedRead.available()
            ? getInvocation(getClazz(), "readFully",
            Long.class, ByteBuffer.class)
            : unavailable("readFully");
    byteBufferPositionedReadFunctional = new AtomicBoolean(
        byteBufferPositionedRead.available()
            && getInstance().hasCapability(StandardStreamCapabilities.PREADBYTEBUFFER));
  }

  @Override public final boolean byteBufferPositionedReadFunctional() {
    return byteBufferPositionedReadFunctional.get();
  }

  @Override public int read(long position, ByteBuffer buf) throws IOException {
    if (byteBufferPositionedReadFunctional()) {
      try {
        return (int) byteBufferPositionedRead.invoke(getInstance(), position, buf);
      } catch (UnsupportedOperationException e) {
        LOG.debug("Failure to invoke read() on {}", getInstance(), e);
        // note the api isn't actually available,
        // before falling back.
        byteBufferPositionedReadFunctional.set(false);
      }
    }
    fallbackRead(position, buf);
    return (int) byteBufferPositionedRead.invoke(getInstance(), position, buf);
  }

  @Override public void readFully(long position, ByteBuffer buf) throws IOException {
    if (byteBufferPositionedReadFunctional()) {
      try {
        byteBufferPositionedReadFully.invoke(getInstance(), position, buf);
        return;
      } catch (UnsupportedOperationException e) {
        LOG.debug("Failure to invoke readFully() on {}", getInstance(), e);
        // note the api isn't actually available,
        // before falling back.
        byteBufferPositionedReadFunctional.set(false);
      }
    }
    fallbackReadFully(position, buf);
  }

  /**
   * Fallback implementation of PositionedReadable: read into a buffer
   * Based on some of the hdfs code.
   * {@code DFSInputStream.actualGetFromOneDataNode()}.
   * @param position position within file
   * @param buf the ByteBuffer to receive the results of the read operation.
   * @throws IOException failure
   */
  public synchronized void fallbackReadFully(long position, ByteBuffer buf) throws IOException {
    FSDataInputStream in = getInstance();
    // position to return to.
    if (buf.hasArray()) {
      LOG.debug("reading directly into bytebuffer array via positionedRead");
      int len = buf.remaining();
      ByteBuffer tmp = buf.duplicate();
      tmp.limit(tmp.position() + len);
      tmp = tmp.slice();
      in.readFully(position, tmp.array(), tmp.position(), len);
      buf.position(buf.position() + len);
      return;
    }
    // no array.
    // is the inner stream ByteBufferReadable? if so, read
    // through that then seek back.
    if (in.getWrappedStream() instanceof ByteBufferReadable) {
      LOG.debug("reading bytebuffer through seek and read(ByteBuffer)");
      try (SeekBack back = new SeekBack(in)) {
        in.seek(position);
        // but what if
        in.read(buf);
      }
      return;
    }

    // final strategy, loop through with positioned read
    // TODO
    throw new IOException("todo");


  }

  /**
   * Fallback implementation of PositionedReadable: read into a buffer
   * Based on some of the hdfs code.
   * {@code DFSInputStream.actualGetFromOneDataNode()}.
   * @param position position within file
   * @param buf the ByteBuffer to receive the results of the read operation.
   * @return bytes read
   * @throws IOException failure
   */
  public synchronized int fallbackRead(long position, ByteBuffer buf) throws IOException {
    FSDataInputStream in = getInstance();
    // position to return to.
    try (SeekBack back = new SeekBack(in)) {
      if (buf.hasArray()) {
        int len = buf.remaining();
        ByteBuffer tmp = buf.duplicate();
        tmp.limit(tmp.position() + len);
        tmp = tmp.slice();
        int read = in.read(position, tmp.array(), tmp.position(), len);
        buf.position(buf.position() + read);
        return read;
      }
    }
    return 0;
  }

  /**
   * class to seek back to the original position after a read;
   * intended for use in try with closeable.
   */
  public static final class SeekBack implements Closeable {
    private final FSDataInputStream in;
    private final long pos;

    public SeekBack(FSDataInputStream in) throws IOException {
      this.in = in;
      this.pos = in.getPos();
    }

    /**
     * Seek back to the original position if needed.
     * @throws IOException failure
     */
    @Override public void close() throws IOException {
      if (in.getPos() != pos) {
        in.seek(pos);
      }
    }

  }
}
