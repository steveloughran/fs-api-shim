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

import java.lang.reflect.Constructor;
import java.nio.ByteBuffer;
import java.util.concurrent.CompletableFuture;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.fs.shim.VectorFileRange;

import static org.apache.hadoop.fs.shim.impl.ShimReflectionSupport.ctor;
import static org.apache.hadoop.fs.shim.impl.ShimReflectionSupport.loadInvocation;

/**
 * Class to bridge to a FileRange implementation class through reflection.
 */
public final class FileRangeBridge {
  private static final Logger LOG = LoggerFactory.getLogger(FileRangeBridge.class);

  public static final String CLASSNAME = "org.apache.hadoop.fs.impl.FileRangeImpl";

  private final Class<?> fileRangeClass;
  private final Invocation<Long> _getOffset;
  private final Invocation<Integer> _getLength;
  private final Invocation<CompletableFuture<ByteBuffer>> _getData;
  private final Invocation<Void> _setData;
  private final Invocation<Object> _getReference;
  private final Constructor<?> newFileRange;
  // this is a special one as the type is not yet known

  public FileRangeBridge() {

    // try to load the class
    Class<?> cl;
    try {
      cl = this.getClass().getClassLoader().loadClass(CLASSNAME);
    } catch (ClassNotFoundException e) {
      LOG.debug("No {}", CLASSNAME);
      cl = null;
    }
    fileRangeClass = cl;
    // class found, so load the methods
    _getOffset = loadInvocation(fileRangeClass, "getOffset", Long.class);
    _getLength= loadInvocation(fileRangeClass, "getLength", Integer.class);
    _getData = loadInvocation(fileRangeClass, "getData", null);
    _setData = loadInvocation(fileRangeClass, "setData", Void.class, CompletableFuture.class);
    _getReference = loadInvocation(fileRangeClass, "getReference", Object.class);

    newFileRange = ctor(fileRangeClass, Long.class, Integer.class, Object.class);
  }

  public boolean bridgeAvailable() {
    return fileRangeClass != null;
  }

  /**
   * This creates an implementation of {@link VectorFileRange} which
   * actually forwards to the inner FileRange class through reflection.
   * This allows the rest of the shim library to use the VectorFileRange
   * API to interact with these.
   */
  private class WrappedFileRange implements VectorFileRange {
    final Object instance;

    public WrappedFileRange(final Object instance) {
      this.instance = instance;
    }

    @Override
    public long getOffset() {
      return _getOffset.invokeUnchecked(instance);
    }

    @Override
    public int getLength() {
      return _getLength.invokeUnchecked(instance);
    }

    @Override
    public CompletableFuture<ByteBuffer> getData() {
      return _getData.invokeUnchecked(instance);

    }

    @Override
    public void setData(final CompletableFuture<ByteBuffer> data) {
      _setData.invokeUnchecked(instance, data);
    }

    @Override
    public Object getReference() {
      return _getReference.invokeUnchecked(instance);
    }
  }

}
