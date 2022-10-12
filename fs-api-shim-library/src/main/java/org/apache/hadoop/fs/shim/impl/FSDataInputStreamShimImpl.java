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
import java.io.EOFException;
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
 * There's an expectation that the stream implements readFully() efficienty, and
 * is has a lazy seek() call, or the cost of a seek() is so low as to not matter.
 */
public class FSDataInputStreamShimImpl
    extends AbstractAPIShim<FSDataInputStream>
    implements FSDataInputStreamShim {

  private static final Logger LOG = LoggerFactory.getLogger(FSDataInputStreamShimImpl.class);

  /**
   * buffer size for fallbacks when reading into ByteBuffers which are not also
   * arrays.
   * TODO: make configurable?
   */
  public static final int TEMPORARY_BUFFER = 1024 * 128;

  /**
   * {@code ByteBufferPositionedRead.readFully()}.
   */
  private final Invocation byteBufferPositionedRead;

  /**
   * {@code ByteBufferPositionedRead.readFully()}.
   */
  private final Invocation byteBufferPositionedReadFully;
  private final AtomicBoolean byteBufferPositionedReadFunctional;

  private final AtomicBoolean byteBufferReadableAvailable;

  /**
   * Constructor.
   *
   * @param instance Instance being shimmed.
   */
  public FSDataInputStreamShimImpl(
      final FSDataInputStream instance) {
    super(FSDataInputStream.class, instance);
    byteBufferPositionedRead = getInvocation(getClazz(), "read",
        Long.class, ByteBuffer.class);

    byteBufferPositionedReadFully =
        byteBufferPositionedRead.available()
            ? getInvocation(getClazz(), "readFully", Long.class, ByteBuffer.class)
            : unavailable("readFully");
    byteBufferPositionedReadFunctional = new AtomicBoolean(
        byteBufferPositionedRead.available()
            && getInstance().hasCapability(StandardStreamCapabilities.PREADBYTEBUFFER));
    // declare ByteBufferReadable available if the inner stream supports it.
    // if an attempt to use it fails, it will downgrade
    byteBufferReadableAvailable = new AtomicBoolean(
        instance.getWrappedStream() instanceof ByteBufferReadable);
  }

  @Override
  public final boolean byteBufferPositionedReadFunctional() {
    return byteBufferPositionedReadFunctional.get();
  }

  @Override
  public int read(long position, ByteBuffer buf) throws IOException {
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

  @Override
  public void readFully(long position, ByteBuffer buf) throws IOException {
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
   *
   * @param position position within file
   * @param buf the ByteBuffer to receive the results of the read operation.
   *
   * @throws IOException failure
   */
  public synchronized void fallbackReadFully(long position, ByteBuffer buf) throws IOException {
    FSDataInputStream in = getInstance();
    int len = buf.remaining();
    LOG.debug("read @{} {} bytes", position, len);
    // position to return to.
    if (buf.hasArray()) {
      readIntoArrayByteBufferThroughReadFully(position, buf, len);
      return;
    }
    // no array.
    // is the inner stream ByteBufferReadable? if so, read
    // through that then seek back.
    if (byteBufferReadableAvailable.get()) {
      LOG.debug("reading bytebuffer through seek and read(ByteBuffer)");
      try (SeekToThenBack back = new SeekToThenBack(position)) {
        while (buf.remaining() > 0) {
          int bytesRead = in.read(buf);
          if (bytesRead < 0) {
            throw new EOFException("No more data in stream; needed "
                + buf.remaining() + " to complete the read");
          }
        }
      } catch (UnsupportedOperationException ex) {
        LOG.debug("stream does not support ByteBufferReadable", ex);
        // don't try using this again
        byteBufferReadableAvailable.set(false);
      }
      return;
    }

    // final strategy.
    // buffer isn't an array, so need to create a smaller one then read via a series of readFully
    // calls.
    int bufferSize = Math.min(len, TEMPORARY_BUFFER);
    byte[] byteArray = new byte[bufferSize];
    long nextReadPosition = position;
    while (buf.remaining() > 0) {
      int bytesToRead = Math.min(bufferSize, buf.remaining());
      getInstance().readFully(nextReadPosition, byteArray, 0,
          bytesToRead);
      buf.put(byteArray, 0, bytesToRead);
      // move forward in the file
      nextReadPosition += bytesToRead;
    }

  }

  /**
   * Read directly into bytebuffer array via PositionedReadable.readFully()");
   *
   * @param position
   * @param buf
   * @param in
   * @param len
   *
   * @throws IOException
   */
  private void readIntoArrayByteBufferThroughReadFully(
      final long position,
      final ByteBuffer buf,
      final int len) throws IOException {
    LOG.debug("reading directly into bytebuffer array via PositionedReadable.readFully()");
    ByteBuffer tmp = buf.duplicate();
    tmp.limit(tmp.position() + len);
    tmp = tmp.slice();
    getInstance().readFully(position, tmp.array(), tmp.position(), len);
    buf.position(buf.position() + len);
  }

  /**
   * Fallback implementation of PositionedReadable: read into a buffer
   * Based on some of the hdfs code.
   * {@code DFSInputStream.actualGetFromOneDataNode()}.
   *
   * @param position position within file
   * @param buf the ByteBuffer to receive the results of the read operation.
   *
   * @return bytes read
   *
   * @throws IOException failure
   */
  public synchronized int fallbackRead(long position, ByteBuffer buf)
      throws IOException {
    FSDataInputStream in = getInstance();
    int len = buf.remaining();
    // position to return to.
    if (buf.hasArray()) {
      ByteBuffer tmp = buf.duplicate();
      tmp.limit(tmp.position() + len);
      tmp = tmp.slice();
      int read = in.read(position, tmp.array(), tmp.position(), len);
      buf.position(buf.position() + read);
      return read;
    } else {
      // only read up to the temp buffer; caller gets to
      // ask for more if it they want it
      int bufferSize = Math.min(len, TEMPORARY_BUFFER);
      byte[] byteArray = new byte[bufferSize];
      int read = getInstance().read(position, byteArray, 0,
          bufferSize);
      buf.put(byteArray, 0, bufferSize);
      return read;
    }

  }

  /**
   * class to seek back to the original position after a read;
   * intended for use in try with closeable.
   */
  public final class SeekToThenBack implements Closeable {

    private final long pos;

    public SeekToThenBack(long newPos) throws IOException {
      this.pos = getInstance().getPos();
      seekTo(newPos);
    }

    /**
     * On demand seek.
     *
     * @param newPos new position
     *
     * @throws IOException failure.
     */
    private void seekTo(long newPos) throws IOException {
      if (getInstance().getPos() != pos) {
        getInstance().seek(pos);
      }
    }

    /**
     * Seek back to the original position if needed.
     *
     * @throws IOException failure
     */
    @Override
    public void close() throws IOException {
      seekTo(pos);
    }

  }
}
