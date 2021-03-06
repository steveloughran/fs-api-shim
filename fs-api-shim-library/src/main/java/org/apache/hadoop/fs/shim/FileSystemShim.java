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
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicBoolean;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.shim.functional.FutureDataInputStreamBuilder;
import org.apache.hadoop.fs.shim.impl.ExecuteOpenFile;
import org.apache.hadoop.fs.shim.impl.Invocation;
import org.apache.hadoop.fs.shim.impl.OpenFileBuilder;
import org.apache.hadoop.fs.shim.impl.OpenFileThroughBuilderAPI;
import org.apache.hadoop.fs.shim.impl.OpenFileThroughClassicAPI;

import static org.apache.hadoop.fs.shim.ShimConstants.FS_OPTION_SHIM_OPENFILE_ENABLED;
import static org.apache.hadoop.fs.shim.ShimConstants.FS_OPTION_SHIM_OPENFILE_FILESTATUS_ENABLED;
import static org.apache.hadoop.fs.shim.ShimConstants.FS_OPTION_SHIM_OPENFILE_FILESTATUS_ENABLED_DEFAULT;
import static org.apache.hadoop.fs.shim.impl.ShimUtils.getInvocation;

/**
 * Shim for the Hadoop {@code FileSystem} class.
 * Some of this is fairly complex, especially when fallback methods are provided...
 * separate shims are used to help here.
 */
public class FileSystemShim extends AbstractAPIShim<FileSystem> {
  private static final Logger LOG = LoggerFactory.getLogger(FileSystemShim.class);

  private final Invocation hasPathCapabilityMethod;
  private final Invocation msyncMethod;

  private final ExecuteOpenFile classicOpenFile;

  /**
   * Builder API; may be null
   */
  private final OpenFileThroughBuilderAPI openFileThroughBuilder;

  private final AtomicBoolean useOpenFileAPI = new AtomicBoolean(false);
  private final OpenFileThroughAvailableOperation executeOpenFile;

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

    // this is always present and used as the fallback
    classicOpenFile = new OpenFileThroughClassicAPI(getInstance());

    // use the builder if present, and configured.
    OpenFileThroughBuilderAPI builderAPI = null;
    Configuration conf = instance.getConf();
    if (conf.getBoolean(FS_OPTION_SHIM_OPENFILE_ENABLED, true)) {

      boolean withFileStatus = conf.getBoolean(FS_OPTION_SHIM_OPENFILE_FILESTATUS_ENABLED,
          FS_OPTION_SHIM_OPENFILE_FILESTATUS_ENABLED_DEFAULT);
      builderAPI = new OpenFileThroughBuilderAPI(getInstance(),
          withFileStatus);
      if (builderAPI.openFileFound()) {
        //the method is present, so bind to it.
        openFileThroughBuilder = builderAPI;
        useOpenFileAPI.set(true);
      } else {
        LOG.debug("Builder API enabled but not found");
        openFileThroughBuilder = null;
      }
    } else {
      // builder not enabled
      LOG.debug("Builder API not enabled");
      openFileThroughBuilder = null;
    }

    // the simpler methods.
    Class<FileSystem> clazz = getClazz();

    hasPathCapabilityMethod = getInvocation(clazz, "hasPathCapability",
        Path.class, String.class);

    msyncMethod = getInvocation(clazz, "msync");

    executeOpenFile = new OpenFileThroughAvailableOperation();
  }

  private void setLongOpt(Object builder, Method opt, String key, long len)
      throws InvocationTargetException, IllegalAccessException {
    if (len >= 0) {
      opt.invoke(builder, key, Long.toString(len));
    }
  }

  public FutureDataInputStreamBuilder openFile(Path path)
      throws IOException, UnsupportedOperationException {
    return new OpenFileBuilder(executeOpenFile, path);
  }

  /**
   * Static class to handle the openFile operation.
   * Why not just make the shim claas implement the method?
   * Exposes too much of the implementation.
   */
  private final class OpenFileThroughAvailableOperation implements ExecuteOpenFile {
    @Override
    public CompletableFuture<FSDataInputStream> executeOpenFile(final OpenFileBuilder builder)
        throws IllegalArgumentException, UnsupportedOperationException, IOException {

      if (openFileFound()) {
        try {
          // use the builder API; return the result
          return openFileThroughBuilder.executeOpenFile(builder);
        } catch (ClassCastException | UnsupportedOperationException e) {
          LOG.warn(
              "Failed to open file using openFileThroughBuilder, falling back to classicOpenFile",
              e);
          // disable the API for this shim instance and fall back to classicOpenFile.
          useOpenFileAPI.set(false);
        }
      }
      return classicOpenFile.executeOpenFile(builder);
    }
  }

  /**
   * Is the openFile method available?
   * @return true if the FS implements the method.
   */
  public boolean openFileFound() {
    return useOpenFileAPI.get();
  }

  /**
   * Count of openfile failures.
   *
   * @return counter
   */
  public int getOpenFileFailures() {
    return openFileThroughBuilder.getOpenFileFailures();
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

  /**
   * Is msync available?
   *
   * @return true if the mmsync method is available.
   */
  public boolean msyncFound() {
    return msyncMethod.available();
  }

  /**
   * Synchronize client metadata state.
   * <p>
   * In many versions of hadoop, but not cloudera CDH7.
   * A no-op if not implemented.
   *
   * @throws IOException If an I/O error occurred.
   */
  public void msync() throws IOException {
    if (msyncFound()) {
      try {
        msyncMethod.invoke(getInstance());
      } catch (IllegalArgumentException | UnsupportedOperationException e) {
        LOG.debug("Failure of msync()", e);
      }
    }
  }

}
