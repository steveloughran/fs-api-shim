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
import org.apache.hadoop.fs.shim.functional.FutureDataInputStreamBuilder;
import org.apache.hadoop.fs.shim.impl.Invocation;
import org.apache.hadoop.fs.shim.impl.OpenFileBuilder;
import org.apache.hadoop.fs.shim.impl.OpenFileBuilder.ExecuteOpenFile;

import static org.apache.hadoop.fs.shim.functional.FutureIO.eval;
import static org.apache.hadoop.fs.shim.impl.ShimUtils.convertToIOException;
import static org.apache.hadoop.fs.shim.impl.ShimUtils.getInvocation;
import static org.apache.hadoop.fs.shim.impl.ShimUtils.getMethod;

/**
 * Shim for the Hadoop {@code FileSystem} class.
 */
public class FileSystemShim extends AbstractAPIShim<FileSystem> {
  private static final Logger LOG = LoggerFactory.getLogger(FileSystemShim.class);

  private final Method openFileMethod;
  private final Invocation hasPathCapabilityMethod;
  private final Invocation msyncMethod;

  private final AtomicInteger openFileFailures = new AtomicInteger();
  private final ExecuteOpenFile classicOpenFile;
  private final ExecuteOpenFile openFileToInvoke;

  /**
   * Last exception swallowed in openFile();
   */
  private volatile Exception lastOpenFileException;

  private final boolean raiseExceptionsOnOpenFileFailures;

  /**
   * Constructor
   *
   * @param instance instance to bind to.
   */
  public FileSystemShim(final FileSystem instance) {
    this(instance, false);
  }

  /**
   * Costructor, primarily for testing.
   *
   * @param instance instance to bind to.
   * @param raiseExceptionsOnOpenFileFailures raise exceptions rather than downgrade on openFile builder failures.
   */
  public FileSystemShim(
      final FileSystem instance,
      final boolean raiseExceptionsOnOpenFileFailures) {
    super(FileSystem.class, instance);
    this.raiseExceptionsOnOpenFileFailures = raiseExceptionsOnOpenFileFailures;

    // this is always present and used as the fallback
    this.classicOpenFile = new OpenFileThroughClassicAPI();

    Class<FileSystem> clazz = getClazz();
    openFileMethod = getMethod(clazz, "openFile", Path.class);
    if (openFileMethod != null) {
      // the method is present, so bind to it.
      openFileToInvoke = new OpenFileThroughBuilder();
    } else {
      openFileToInvoke = classicOpenFile;
    }

    hasPathCapabilityMethod = getInvocation(clazz, "hasPathCapability",
        Path.class, String.class);

    msyncMethod = getInvocation(clazz, "msync");

  }

  private void setLongOpt(Object builder, Method opt, String key, long len)
      throws InvocationTargetException, IllegalAccessException {
    if (len >= 0) {
      opt.invoke(builder, key, Long.toString(len));
    }
  }

  public FutureDataInputStreamBuilder openFile(Path path)
      throws IOException, UnsupportedOperationException {
    return new OpenFileBuilder(openFileToInvoke, path);
  }

  /**
   * Record an exception during openFile.
   * Increments the coutner and sets the
   * {@link #lastOpenFileException} to the value
   *
   * @param ex caught exception.
   */
  protected void openFileException(Path path, Exception ex) throws IOException {
    LOG.info("Failed to use openFile({})", path, ex);
    openFileFailures.incrementAndGet();
    if (ex != null) {
      lastOpenFileException = ex;
    }
    if (raiseExceptionsOnOpenFileFailures) {
      throw convertToIOException(ex);
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

  public boolean pathCapabilitiesFound() {
    return hasPathCapabilityMethod.available();
  }

  /**
   * Has path capability. HADOOP-15691/3.2.2+
   *
   * @param path path to query the capability of.
   * @param capability non-null, non-empty string to query the path for support.
   *
   * @return true if the capability is supported under that part of the FS.
   *
   * @throws IOException this should not be raised, except on problems
   * resolving paths or relaying the call.
   * @throws IllegalArgumentException invalid arguments
   */
  public boolean hasPathCapability(final Path path, final String capability)
      throws IOException {
    if (!pathCapabilitiesFound()) {
      return false;
    }
    try {
      return (Boolean) hasPathCapabilityMethod.invoke(getInstance(), path, capability);
    } catch (ClassCastException | IllegalArgumentException e) {
      LOG.debug("Failure of hasPathCapability({}, {})", path, capability, e);
      return false;
    }
  }

  public boolean msyncFound() {
    return msyncMethod.available();
  }

  /**
   * Synchronize client metadata state.
   * <p>
   * In many versions of hadoop, but not cloudera CDH7.
   * A no-op if not implementedc.
   *
   * @throws IOException If an I/O error occurred.
   * @throws UnsupportedOperationException if the operation is unsupported.
   */
  public void msync() throws IOException, UnsupportedOperationException {
    if (msyncFound()) {
      try {
        msyncMethod.invoke(getInstance());
      } catch (IllegalArgumentException e) {
        LOG.debug("Failure of msync()", e);
      }
    }
  }

  /**
   * Build through the classic API.
   * The opening is asynchronous.
   */
  private class OpenFileThroughClassicAPI implements ExecuteOpenFile {
    @Override
    public CompletableFuture<FSDataInputStream> executeOpenFile(final OpenFileBuilder builder)
        throws IllegalArgumentException, UnsupportedOperationException, IOException {
      if (!builder.getMandatoryKeys().isEmpty()) {
        throw new IllegalArgumentException("Mandatory keys not supported");
      }
      return eval(() ->
          getInstance().open(builder.getPath()));
    }
  }

  /**
   * Open a file through the builder API.
   */
  private final class OpenFileThroughBuilder implements ExecuteOpenFile {

    @Override
    public CompletableFuture<FSDataInputStream> executeOpenFile(final OpenFileBuilder source)
        throws IOException {

      FileStatus status = source.getStatus();
      FileSystem fs = getInstance();
      Path path = source.getPath();

      try {
        Object builder = openFileMethod.invoke(fs, path);
        Class<?> builderClass = builder.getClass();
        Method opt = builderClass.getMethod("opt", String.class, String.class);
        Configuration options = source.getOptions();
        for (Map.Entry<String, String> option : options) {
          opt.invoke(builder, option.getKey(), option.getValue());
        }
        Method must = builderClass.getMethod("must", String.class, String.class);
        for (String k : source.getMandatoryKeys()) {
          must.invoke(builder, k, options.get(k));
        }


        if (status != null) {
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

      } catch (IllegalArgumentException e) {
        // RTE we are happy to downgrade
        openFileException(path, e);
      } catch (ClassCastException e) {
        // this a bug in the code, rethrow
        openFileException(path, e);
        throw e;
      } catch (IllegalAccessException | InvocationTargetException | NoSuchMethodException e) {
        // downgrade on all failures, even classic IOEs...let the fallback handle them.
        openFileException(path, e);
      }

      // the new API failed, fall back.
      return classicOpenFile.executeOpenFile(source);
    }

  }

}
