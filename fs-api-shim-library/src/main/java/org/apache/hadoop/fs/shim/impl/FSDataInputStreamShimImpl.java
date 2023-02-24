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
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.IntFunction;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.fs.ByteBufferReadable;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.shim.FSDataInputStreamShim;
import org.apache.hadoop.fs.shim.StandardStreamCapabilities;
import org.apache.hadoop.fs.shim.VectorFileRange;

import static org.apache.hadoop.fs.shim.impl.Invocation.unavailable;
import static org.apache.hadoop.fs.shim.impl.ShimUtils.getInvocation;
import static org.apache.hadoop.fs.shim.impl.VectoredRangeReadUtils.readInDirectBuffer;
import static org.apache.hadoop.util.StringUtils.toLowerCase;

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
  private final Invocation<Integer> byteBufferPositionedRead;

  /**
   * {@code ByteBufferPositionedRead.readFully()}.
   */
  private final Invocation<Void> byteBufferPositionedReadFully;
  private final AtomicBoolean isByteBufferPositionedReadAvailable;

  private final AtomicBoolean isByteBufferReadableAvailable;

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
    isByteBufferPositionedReadAvailable = new AtomicBoolean(
        byteBufferPositionedRead.available()
            && instance.hasCapability(StandardStreamCapabilities.PREADBYTEBUFFER));
    // declare ByteBufferReadable available if the inner stream supports it.
    // if an attempt to use it fails, it will downgrade
    isByteBufferReadableAvailable = new AtomicBoolean(
        instance.getWrappedStream() instanceof ByteBufferReadable);
  }

  @Override
  public boolean hasCapability(final String capability) {
    switch (toLowerCase(capability)) {
    case "in:preadbytebuffer":
      // positioned read is always available
      return true;
    case "in:readvectored":
      // because of the inability to reference FileRange objects,
      // there's no way to do passthrough of PositionedReadable.readVectored()
      return false;
    default:
      return getInstance().hasCapability(capability);
    }
  }

  @Override
  public int read(final long position, final byte[] buffer, final int offset, final int length)
      throws IOException {
    return getInstance().read(position, buffer, offset, length);
  }

  @Override
  public void readFully(final long position,
      final byte[] buffer,
      final int offset,
      final int length) throws IOException {
    getInstance().readFully(position, buffer, offset, length);
  }

  @Override
  public void readFully(final long position, final byte[] buffer) throws IOException {
    getInstance().readFully(position, buffer);
  }

  @Override
  public final boolean isByteBufferPositionedReadAvailable() {
    return isByteBufferPositionedReadAvailable.get();
  }

  @Override
  public int read(long position, ByteBuffer buf) throws IOException {
    if (isByteBufferPositionedReadAvailable()) {
      try {
        return byteBufferPositionedRead.invoke(getInstance(), position, buf);
      } catch (UnsupportedOperationException e) {
        LOG.debug("Failure to invoke read() on {}", getInstance(), e);
        // note the api isn't actually available,
        // before falling back.
        isByteBufferPositionedReadAvailable.set(false);
      }
    }
    fallbackRead(position, buf);
    return byteBufferPositionedRead.invoke(getInstance(), position, buf);
  }

  @Override
  public void readFully(long position, ByteBuffer buf) throws IOException {
    if (isByteBufferPositionedReadAvailable()) {
      try {
        byteBufferPositionedReadFully.invoke(getInstance(), position, buf);
        return;
      } catch (UnsupportedOperationException e) {
        LOG.debug("Failure to invoke readFully() on {}", getInstance(), e);
        // note the api isn't actually available,
        // before falling back.
        isByteBufferPositionedReadAvailable.set(false);
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
    if (isByteBufferReadableAvailable.get()) {
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
        isByteBufferReadableAvailable.set(false);
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
    readFully(position, tmp.array(), tmp.position(), len);
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
    int len = buf.remaining();
    // position to return to.
    if (buf.hasArray()) {
      ByteBuffer tmp = buf.duplicate();
      tmp.limit(tmp.position() + len);
      tmp = tmp.slice();
      int read = read(position, tmp.array(), tmp.position(), len);
      buf.position(buf.position() + read);
      return read;
    } else {
      // only read up to the temp buffer; caller gets to
      // ask for more if it they want it
      int bufferSize = Math.min(len, TEMPORARY_BUFFER);
      byte[] byteArray = new byte[bufferSize];
      int read = read(position, byteArray, 0, bufferSize);
      buf.put(byteArray, 0, bufferSize);
      return read;
    }

  }

  /**
   * class to seek back to the original position after a read;
   * intended for use in try with closeable.
   */
  private final class SeekToThenBack implements Closeable {

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

  @Override
  public void readVectoredRanges(List<VectorFileRange> ranges,
      IntFunction<ByteBuffer> allocate) throws IOException {
    // fallback code
    readVectoredLegacy(ranges, allocate);
  }

  /**
   * This is the default implementation which iterates through the ranges
   * to read each synchronously, but the intent is that subclasses
   * can make more efficient readers.
   * The data or exceptions are pushed into {@link VectorFileRange#getData()}.
   *
   * @param ranges the byte ranges to read
   * @param allocate the byte buffer allocation
   */
  private void readVectoredLegacy(
      List<? extends VectorFileRange> ranges,
      IntFunction<ByteBuffer> allocate) {
    for (VectorFileRange range : ranges) {
      range.setData(readRangeFrom(range, allocate));
    }
  }

  /**
   * Synchronously reads a range from the stream dealing with the combinations
   * of ByteBuffers buffers and PositionedReadable streams.
   *
   * @param range the range to read
   * @param allocate the function to allocate ByteBuffers
   *
   * @return the CompletableFuture that contains the read data
   */
  private CompletableFuture<ByteBuffer> readRangeFrom(VectorFileRange range,
      IntFunction<ByteBuffer> allocate) {
    CompletableFuture<ByteBuffer> result = new CompletableFuture<>();
    try {
      ByteBuffer buffer = allocate.apply(range.getLength());
      if (isByteBufferPositionedReadAvailable()) {
        // use readFully if present
        readFully(range.getOffset(), buffer);
        buffer.flip();
      } else {
        readNonByteBufferPositionedReadable(range, buffer);
      }
      result.complete(buffer);
    } catch (IOException ioe) {
      result.completeExceptionally(ioe);
    }
    return result;
  }

  /**
   * Fall back to classic.
   *
   * @param range
   * @param buffer
   *
   * @throws IOException
   */
  private void readNonByteBufferPositionedReadable(
      VectorFileRange range,
      ByteBuffer buffer) throws IOException {
    final FSDataInputStream stream = getInstance();

    if (buffer.isDirect()) {
      readInDirectBuffer(range.getLength(),
          buffer,
          (position, buffer1, offset, length) -> {
            readFully(position, buffer1, offset, length);
            return null;
          });
      buffer.flip();
    } else {
      readFully(range.getOffset(), buffer.array(),
          buffer.arrayOffset(), range.getLength());
    }
  }

}
