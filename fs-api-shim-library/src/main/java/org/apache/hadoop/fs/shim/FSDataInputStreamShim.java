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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.shim.impl.Invocation;

import static org.apache.hadoop.fs.shim.impl.Invocation.unavailable;
import static org.apache.hadoop.fs.shim.impl.ShimUtils.getInvocation;

/**
 * Shim methods for FSDataInputStream.
 */
public class FSDataInputStreamShim extends AbstractAPIShim<FSDataInputStream> {

  private static final Logger LOG = LoggerFactory.getLogger(FSDataInputStreamShim.class);

  /**
   * {@code ByteBufferPositionedRead.readFully()}.
   */
  private final Invocation byteBufferPositionedRead;

  /**
   * {@code ByteBufferPositionedRead.readFully()}.
   */
  private final Invocation byteBufferPositionedReadFully;

  /**
   * Constructor.
   * @param instalnce Instance being shimmed.
   */
  public FSDataInputStreamShim(
      final FSDataInputStream instance) {
    super(FSDataInputStream.class, instance);
    byteBufferPositionedRead = getInvocation(getClazz(), "read",
        Long.class, ByteBuffer.class);

    byteBufferPositionedReadFully =
        byteBufferPositionedRead.available()
            ? getInvocation(getClazz(), "readFully",
            Long.class, ByteBuffer.class)
            : unavailable("readFully");
  }

  /**
   * Is {@code ByteBufferPositionedRead} API available to invoke
   * If not, calling the methods will raise UnsupportedOperationException
   * @return true if the methods were found.
   */
  public final boolean byteBufferPositionedReadFound() {
    return byteBufferPositionedRead.available();
  }

  /**
   * Is the API functional?
   * That is: the API is in the stream *andK the stream capabilities
   * declares that it is available.
   * @return true if the ByteBufferPositionedRead methods can be used.
   */
  public final boolean byteBufferPositionedReadFunctional() {
    return byteBufferPositionedReadFound()
        && getInstance().hasCapability(StandardStreamCapabilities.PREADBYTEBUFFER);
  }

  /**
   * ByteBufferPositionedReadable.read().
   * All implementations of this are required to return true for the probe
   * {@code hasCapability("in:preadbytebuffer")}.
   * @param position position within file
   * @param buf the ByteBuffer to receive the results of the read operation.
   * @return the number of bytes read, possibly zero, or -1 if reached
   *         end-of-stream
   * @throws IOException if there is some error performing the read
   * @throws UnsupportedOperationException operation isn't available
   */
  public int read(long position, ByteBuffer buf) throws IOException {
    return (int) byteBufferPositionedRead.invoke(getInstance(), position, buf);
  }

  /**
   * ByteBufferPositionedReadable.readFully().
   * All implementations of this are required to return true for the probe
   * {@code hasCapability("in:preadbytebuffer")}.
   * @param position position within file
   * @param buf the ByteBuffer to receive the results of the read operation.
   * @throws IOException if there is some error performing the read
   * @throws EOFException the end of the data was reached before
   * the read operation completed
   * @throws UnsupportedOperationException operation isn't available
   * @see #read(long, ByteBuffer)
   */
  public void readFully(long position, ByteBuffer buf) throws IOException {
    byteBufferPositionedReadFully.invoke(getInstance(), position, buf);
  }

}
