/**
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

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.fs.ByteBufferReadable;
import org.apache.hadoop.fs.CanSetDropBehind;
import org.apache.hadoop.fs.CanSetReadahead;
import org.apache.hadoop.fs.CanUnbuffer;
import org.apache.hadoop.fs.Syncable;

/**
 *
 *  The Standard StreamCapabilities.
 */
@InterfaceAudience.Public
@InterfaceStability.Evolving
public interface StandardStreamCapabilities {
  /**
   * Stream hflush capability implemented by {@code Syncable#hflush()}.
   *
   * Use the {@code #HSYNC} probe to check for the support of Syncable;
   * it's that presence of {@code hsync()} which matters.
   */
  @Deprecated
  String HFLUSH = "hflush";

  /**
   * Stream hsync capability implemented by {@code Syncable#hsync()}.
   */
  String HSYNC = "hsync";

  /**
   * Stream setReadahead capability implemented by
   * {@code CanSetReadahead#setReadahead(Long)}.
   */
  String READAHEAD = "in:readahead";

  /**
   * Stream setDropBehind capability implemented by
   * {@code CanSetDropBehind#setDropBehind(Boolean)}.
   */
  String DROPBEHIND = "dropbehind";

  /**
   * Stream unbuffer capability implemented by {@code CanUnbuffer#unbuffer()}.
   */
  String UNBUFFER = "in:unbuffer";

  /**
   * Stream read(ByteBuffer) capability implemented by
   * {@code ByteBufferReadable#read(java.nio.ByteBuffer)}.
   */
  String READBYTEBUFFER = "in:readbytebuffer";

  /**
   * Stream read(long, ByteBuffer) capability implemented by
   * {@code ByteBufferPositionedReadable#read(long, java.nio.ByteBuffer)}.
   */
  String PREADBYTEBUFFER = "in:preadbytebuffer";

  /**
   * IOStatisticsSource API.
   */
  String IOSTATISTICS = "iostatistics";

}

