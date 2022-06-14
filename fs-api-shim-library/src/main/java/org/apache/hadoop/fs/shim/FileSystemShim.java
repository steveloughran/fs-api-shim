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
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicInteger;
import javax.annotation.Nullable;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.shim.impl.Invocation;

import static java.util.Objects.requireNonNull;
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

  /**
   * Last exception swallowed in openFile();
   */
  private volatile Exception lastOpenFileException;

  private final boolean raiseExceptionsOnOpenFileFailures;

  /**
   * Constructor
   * @param instance instance to bind to.
   */
  public FileSystemShim(final FileSystem instance) {
    this(instance, false);
  }

  /**
   * Costructor, primarily for testing.
   * @param instance instance to bind to.
   * @param raiseExceptionsOnOpenFileFailures raise exceptions rather than downgrade on openFile builder failures.
   */
  public FileSystemShim(
      final FileSystem instance,
      final boolean raiseExceptionsOnOpenFileFailures) {
    super(FileSystem.class, instance);
    Class<FileSystem> clazz = getClazz();
    openFileMethod = getMethod(clazz, "openFile", Path.class);
    hasPathCapabilityMethod = getInvocation(clazz, "hasPathCapability",
        Path.class, String.class);

    msyncMethod = getInvocation(clazz, "msync");
    this.raiseExceptionsOnOpenFileFailures = raiseExceptionsOnOpenFileFailures;
  }

  private void setLongOpt(Object builder, Method opt, String key, long len)
      throws InvocationTargetException, IllegalAccessException {
    if (len >= 0) {
      opt.invoke(builder, key, Long.toString(len));
    }
  }

  /**
   * Open a file.
   * On hadoop 3.3.0+ the {@code openFile()} builder API is used,
   * which on some filesystems (initially s3a) lets the caller set
   * seek policy and split start/end hints.
   * If a filestatus is passed in (or sometimes just length) then
   * stores may skip HEAD checks for file existence.
   * S3A does this since Hadoop 3.3.0;
   * Abfs supports this on all branches with HADOOP-17682.
   *
   * In HADOOP-16202 standard keys were defined, with an ordered
   * and extensible list of read policies permitted in the
   *  "fs.option.openfile.read.policy" key.
   * The s3a key "fs.s3a.experimental.input.fadvise"
   * only takes the original set of values
   * "sequential", "random", "adaptive"; if the seek param
   * is not one of those it is not set.
   *
   * If the builder can't be invoked for some reason,
   * or fails to open (permissions, file not found...)
   * then the method falls back to the class openFile()
   * call.
   *
   * Note that the if status/length is passed in and the file
   * is missing, unreadable etc, the error may not surface
   * until the first read() call.
   *
   * See https://github.com/apache/hadoop/blob/trunk/hadoop-common-project/hadoop-common/src/site/markdown/filesystem/fsdatainputstream.md
   * @param path path
   * @param seekPolicy seek policy, one of
   * @param status nullable file status
   * @param len length; -1 if not known
   * @param splitStart start of split ; -1 if not known
   * @param splitEnd end of split ; -1 if not known
   * @return an open stream.
   * @throws IOException failure to open
   */
  public FSDataInputStream openFile(
      Path path,
      String seekPolicy,
      @Nullable FileStatus status,
      long len,
      long splitStart,
      long splitEnd) throws IOException {

    FileSystem fs = getInstance();
    requireNonNull(path, "Null path parameter");
    if (openFileMethod != null) {
      try {
        Object builder = openFileMethod.invoke(path);
        Class<?> builderClass = builder.getClass();
        Method opt = builderClass.getMethod("opt", String.class, String.class);

        setLongOpt(builder, opt, ShimConstants.FS_OPTION_OPENFILE_LENGTH, len);
        setLongOpt(builder, opt, ShimConstants.FS_OPTION_OPENFILE_SPLIT_START, splitStart);
        setLongOpt(builder, opt, ShimConstants.FS_OPTION_OPENFILE_SPLIT_END, splitEnd);

        if (seekPolicy != null && !seekPolicy.isEmpty()) {
          if (ShimConstants.S3A_READ_POLICIES.contains(seekPolicy)) {
            // use the subset of seek policies which s3a always knows of.
            opt.invoke(builder, ShimConstants.S3A_INPUT_FADVISE, seekPolicy);
          }
          opt.invoke(builder, ShimConstants.FS_OPTION_OPENFILE_READ_POLICY, seekPolicy);
        }
        if (status != null) {
          // filestatus
          Method withFileStatus =
              builderClass.getMethod("withFileStatus", FileStatus.class);
          withFileStatus.invoke(builder, status);
        }
        Method build = builderClass.getMethod("build");
        Object result = build.invoke(builder);
        // cast and return. will raise ClassCastException which will
        // be thrown.
        Future<FSDataInputStream> future = (Future<FSDataInputStream>) result;
        return future.get();

      } catch (IllegalArgumentException e) {
        // RTE we are happy to downgrade
        openFileException(path, e);
      } catch (ClassCastException e) {
        // this a bug in the code, rethrow
        openFileException(path, e);
        throw e;
      } catch (IllegalAccessException | InvocationTargetException | NoSuchMethodException
               | InterruptedException e) {
        // downgrade on all failures, even classic IOEs...let the fallback handle them.
        openFileException(path, e);
      } catch (ExecutionException e) {
        openFileException(path, e);
      }
    }

    // if there is no openfile, use the classic API
    return fs.open(path);
  }

  /**
   * Record an exception during openFile.
   * Increments the coutner and sets the
   * {@link #lastOpenFileException} to the value
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
   * @param path path to query the capability of.
   * @param capability non-null, non-empty string to query the path for support.
   * @return true if the capability is supported under that part of the FS.
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
}
