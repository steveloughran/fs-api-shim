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

package org.apache.hadoop.fs.shim;

import java.io.EOFException;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.function.IntFunction;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.PositionedReadable;
import org.apache.hadoop.fs.StreamCapabilities;

/**
 * FSDataInputStream Shim.
 * This implements all of PositionedReadable, with the non vector IO methods
 * being passed through to the caller.
 * StreamCapabilities calls may be processed here before being passed back,
 * filtering out disabled options.
 *
 */
public interface FSDataInputStreamShim extends APIShim<FSDataInputStream>,
    PositionedReadable, StreamCapabilities {

  /**
   * Is ByteBufferPositionedRead in the wrapped stream functional?
   * That is: the API is in the stream *and the stream capabilities
   * declares that it is available*
   * If this is not true the fallback implementation will be used.
   *
   *
   * @return true if the ByteBufferPositionedRead methods can be used.
   */
  boolean isByteBufferPositionedReadAvailable();

  /**
   * ByteBufferPositionedReadable.read().
   * All implementations of this are required to return true for the probe
   * {@code hasCapability("in:preadbytebuffer")}.
   *
   * @param position position within file
   * @param buf the ByteBuffer to receive the results of the read operation.
   * @param position position within file
   * @param buf the ByteBuffer to receive the results of the read operation.
   *
   * @return the number of bytes read, possibly zero, or -1 if reached
   * end-of-stream
   *
   * Reads up to {@code buf.remaining()} bytes into buf from a given position
   * in the file and returns the number of bytes read. Callers should use
   * {@code buf.limit(...)} to control the size of the desired read and
   * {@code buf.position(...)} to control the offset into the buffer the data
   * should be written to.
   * <p>
   * After a successful call, {@code buf.position()} will be advanced by the
   * number of bytes read and {@code buf.limit()} will be unchanged.
   * <p>
   * In the case of an exception, the state of the buffer (the contents of the
   * buffer, the {@code buf.position()}, the {@code buf.limit()}, etc.) is
   * undefined, and callers should be prepared to recover from this
   * eventuality.
   * <p>
   * Callers should use {@link StreamCapabilities#hasCapability(String)} with
   * {@link StreamCapabilities#PREADBYTEBUFFER} to check if the underlying
   * stream supports this interface, otherwise they might get a
   * {@link UnsupportedOperationException}.
   * <p>
   * Implementations should treat 0-length requests as legitimate, and must not
   * signal an error upon their receipt.
   * <p>
   * This does not change the current offset of a file, and is thread-safe.
   *
   * @return the number of bytes read, possibly zero, or -1 if reached
   * end-of-stream
   *
   * @throws IOException if there is some error performing the read
   */
  int read(long position, ByteBuffer buf) throws IOException;

  /**
   * ByteBufferPositionedReadable.readFully().
   * All implementations of this are required to return true for the probe
   * {@code hasCapability("in:preadbytebuffer")}.
   *
   * @param position position within file
   * @param buf the ByteBuffer to receive the results of the read operation.
   *
   * @throws IOException if there is some error performing the read
   * @throws EOFException the end of the data was reached before
   * the read operation completed
   * @throws UnsupportedOperationException operation isn't available
   * @see #read(long, ByteBuffer)
   */
  void readFully(long position, ByteBuffer buf) throws IOException;

  /**
   * Read fully a list of file ranges asynchronously from this file.
   * The default iterates through the ranges to read each synchronously, but
   * the intent is that FSDataInputStream subclasses can make more efficient
   * readers.
   * As a result of the call, each range will have FileRange.setData(CompletableFuture)
   * called with a future that when complete will have a ByteBuffer with the
   * data from the file's range.
   * <p>
   * The position returned by getPos() after readVectoredRanges() is undefined.
   * </p>
   * <p>
   * If a file is changed while the vectored read operation is in progress, the output is
   * undefined. Some ranges may have old data, some may have new and some may have both.
   * </p>
   * <p>
   * While a readVectoredRanges() operation is in progress, normal read api calls may block.
   * </p>
   * Shim integration notes
   * <ol>
   *   <li>hands off to {@code PositionedReadable.readVectored()} if present.</li>
   *   <li>if not: uses the same sequence of buffer reads as the default implementation
   *        of that API does. </li>
   *   <li>including using ByteBufferPositionedReadable where implemeted.</li>
   * </ol>
   * @param ranges the byte ranges to read
   * @param allocate the function to allocate ByteBuffer
   *
   * @throws IOException any IOE.
   */
  void readVectoredRanges(List<VectorFileRange> ranges,
      IntFunction<ByteBuffer> allocate) throws IOException;
}
