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

package org.apache.hadoop.fs.shim.test.binding;

import org.apache.hadoop.fs.shim.api.StandardStreamCapabilities;

public class FeatureKeys {

  /**
   * Is the PathCapabilities API available?
   * Value: {@value}.
   */
  public static final String PATH_CAPABILITIES = "path.capabilities";

  /**
   * Is the msync call available?
   * Value: {@value}.
   */
  public static final String MSYNC = "msync";
  public static final String OPENFILE = "openfile";

  public static final String IOSTATISTICS = StandardStreamCapabilities.IOSTATISTICS;

  /**
   * Is the ByteBufferPositionedRead API available?
   * Value: {@value}.
   */
  public static final String BYTEBUFFER_POSITIONED_READ =
      "bytebuffer.positionedread";

  /**
   * Is the ByteBufferPositionedRead API actually
   * implemented by the underlying stream?
   * Value: {@value}.
   */
  public static final String BYTEBUFFER_POSITIONED_READ_IMPLEMENTED =
      "bytebuffer.positionedread.implemented";

  /**
   * Is the vector IO API available?
   * Value: {@value}.
   */
  public static final String VECTOR_IO = "vector.io";
}
