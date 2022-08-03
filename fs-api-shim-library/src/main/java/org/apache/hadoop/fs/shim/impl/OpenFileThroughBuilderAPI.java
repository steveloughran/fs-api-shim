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

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicInteger;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.shim.AbstractAPIShim;
import org.apache.hadoop.fs.shim.functional.FutureIO;

import static org.apache.hadoop.fs.shim.impl.ShimUtils.getMethod;

/**
 * Open a file through the builder API.
 * The only builder methods looked up and invoked are
 * withFileStatus(), opt(String, String), and must(String, String).
 * withFileStatus() can be disabled.
 */
public final class OpenFileThroughBuilderAPI
    extends AbstractAPIShim<FileSystem> implements ExecuteOpenFile {
  private static final Logger LOG = LoggerFactory.getLogger(OpenFileThroughBuilderAPI.class);

  private final Method openFileMethod;

  private final AtomicInteger openFileFailures = new AtomicInteger();

  /**
   * Last exception swallowed in openFile();
   */
  private volatile Exception lastOpenFileException;

  /**
   * Should the .withFileStatus() method be called?
   */
  private final boolean enableWithFileStatus;

  /**
   * Constructor.
   * @param instance FS instance to shim.
   * @param enableWithFileStatus should the .withFileStatus() method be called?
   */
  public OpenFileThroughBuilderAPI(
      final FileSystem instance,
      final boolean enableWithFileStatus) {
    super(FileSystem.class, instance);
    openFileMethod = getMethod(instance.getClass(), "openFile", Path.class);

    this.enableWithFileStatus = enableWithFileStatus;
  }

  /**
   * Record an exception during openFile.
   * Increments the coutner and sets the
   * {@link #lastOpenFileException} to the value
   *
   * @param ex caught exception.
   */
  private void openFileException(Path path, Exception ex) {
    LOG.info("Failed to use openFile({})", path, ex);
    openFileFailures.incrementAndGet();
    if (ex != null) {
      lastOpenFileException = ex;
    }

  }

  public boolean openFileFound() {
    return openFileMethod != null;
  }

  /**
   * Count of openfile failures.
   *
   * @return counter
   */
  public int getOpenFileFailures() {
    return openFileFailures.get();
  }

  /**
   * Use the openFile method to open a file from the builder API.
   *
   * @param source builder to read.
   *
   * @return the input stream
   *
   * @throws ClassCastException classcast problems
   * @throws UnsupportedOperationException if the reflection calls failed.
   */
  @SuppressWarnings("unchecked")
  @Override
  public CompletableFuture<FSDataInputStream> executeOpenFile(final OpenFileBuilder source)
      throws IllegalArgumentException, UnsupportedOperationException, IOException {

    FileStatus status = source.getStatus();
    FileSystem fs = getInstance();
    Path path = source.getPath();

    try {
      Object builder = openFileMethod.invoke(fs, path);
      Class<?> builderClass = builder.getClass();
      // optional paraketers
      Method opt = builderClass.getMethod("opt", String.class, String.class);
      Configuration options = source.getOptions();
      for (Map.Entry<String, String> option : options) {
        opt.invoke(builder, option.getKey(), option.getValue());
      }
      // mandatory parameters
      Method must = builderClass.getMethod("must", String.class, String.class);
      for (String k : source.getMandatoryKeys()) {
        must.invoke(builder, k, options.get(k));
      }

      // a bit unsure about this, because s3a 3.3.0 raises an exception if
      // the status is of the wrong type.
      // hence the ability to disable it.

      if (status != null && enableWithFileStatus) {
        // filestatus
        Method withFileStatus =
            builderClass.getMethod("withFileStatus", FileStatus.class);
        withFileStatus.invoke(builder, status);
      }
      Method build = builderClass.getMethod("build");
      build.setAccessible(true);
      Object result = build.invoke(builder);
      // cast and return. may raise ClassCastException which will
      // be thrown.
      CompletableFuture<FSDataInputStream> future = (CompletableFuture<FSDataInputStream>) result;

      return future;

    } catch (ClassCastException e) {
      // this a bug in the code, rethrow
      openFileException(path, e);
      throw e;
    } catch (InvocationTargetException e) {
      // an exception was reaised by the method, so examine it
      throw FutureIO.unwrapInnerException(e);

    } catch (IllegalAccessException | NoSuchMethodException e) {
      // downgrade on all failures, even classic IOEs...let the fallback handle them.
      openFileException(path, e);
      throw new UnsupportedOperationException(e);
    }

  }

}
