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

import java.nio.ByteBuffer;
import java.util.List;
import java.util.function.IntFunction;

import org.apache.hadoop.fs.PositionedReadable;

/**
 * Support for vectored IO.
 */
public class VectorIOShim {




/**
 * This is the default implementation which iterates through the ranges
 * to read each synchronously, but the intent is that subclasses
 * can make more efficient readers.
 * The data or exceptions are pushed into {@link VectorFileRange#getData()}.
 * @param stream the stream to read the data from
 * @param ranges the byte ranges to read
 * @param allocate the byte buffer allocation
 */
public static void readVectored(PositionedReadable stream,
                                List<? extends VectorFileRange> ranges,
                                IntFunction<ByteBuffer> allocate) {

}


}
