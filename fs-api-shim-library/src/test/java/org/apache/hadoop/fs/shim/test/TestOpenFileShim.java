/*
 * Licensed to the Apache Software Foundation (ASF) under one
 *  or more contributor license agreements.  See the NOTICE file
 *  distributed with this work for additional information
 *  regarding copyright ownership.  The ASF licenses this file
 *  to you under the Apache License, Version 2.0 (the
 *  "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */
package org.apache.hadoop.fs.shim.test;

import java.io.FileNotFoundException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import org.junit.Test;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeysPublic;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.shim.FileSystemShim;
import org.apache.hadoop.fs.shim.functional.FutureDataInputStreamBuilder;
import org.apache.hadoop.fs.shim.test.binding.ShimTestUtils;
import org.apache.hadoop.io.IOUtils;

import static org.apache.hadoop.fs.contract.ContractTestUtils.compareByteArrays;
import static org.apache.hadoop.fs.contract.ContractTestUtils.createFile;
import static org.apache.hadoop.fs.contract.ContractTestUtils.dataset;
import static org.apache.hadoop.fs.shim.OpenFileConstants.FS_OPTION_OPENFILE_BUFFER_SIZE;
import static org.apache.hadoop.fs.shim.OpenFileConstants.FS_OPTION_OPENFILE_LENGTH;
import static org.apache.hadoop.fs.shim.OpenFileConstants.FS_OPTION_OPENFILE_READ_POLICY;
import static org.apache.hadoop.fs.shim.functional.FutureIO.awaitFuture;
import static org.apache.hadoop.fs.shim.test.binding.ShimTestUtils.interceptFuture;
import static org.apache.hadoop.test.LambdaTestUtils.intercept;

/**
 * Test Open operations.
 */
public class TestOpenFileShim
    extends AbstractShimContractTest {

  private FSDataInputStream instream;
  private FileSystemShim fsShim;

  public TestOpenFileShim() {
  }

  @Override
  protected Configuration createConfiguration() {
    Configuration conf = super.createConfiguration();
    conf.setInt(CommonConfigurationKeysPublic.IO_FILE_BUFFER_SIZE_KEY, 4096);
    return conf;
  }

  @Override
  public void setup() throws Exception {
    super.setup();
    fsShim = new FileSystemShim(getFileSystem());
  }

  @Override
  public void teardown() throws Exception {
    IOUtils.closeStream(instream);
    instream = null;
    super.teardown();
  }


  @Test
  public void testOpenFileReadZeroByte() throws Throwable {
    describe("create & read a 0 byte file through the builders");
    Path path = path("zero.txt");
    FileSystem fs = getFileSystem();
    fs.createFile(path).overwrite(true).build().close();
    try (FSDataInputStream is = fsShim.openFile(path)
        .opt("fs.test.something", true)
        .opt("fs.test.something2", 3)
        .opt("fs.test.something3", "3")
        .build().get()) {
      assertMinusOne("initial byte read", is.read());
    }
  }

  @Test
  public void testOpenFileUnknownOption() throws Throwable {
    describe("calling openFile fails when a 'must()' option is unknown");
    FutureDataInputStreamBuilder builder =
        fsShim.openFile(path("testOpenFileUnknownOption"))
            .opt("fs.test.something", true)
            .must("fs.test.something", true);
    intercept(IllegalArgumentException.class,
        () -> builder.build());
  }

  @Test
  public void testOpenFileLazyFail() throws Throwable {
    describe("openFile fails on a missing file in the get() and not before");
    FutureDataInputStreamBuilder builder =
        fsShim.openFile(path("testOpenFileLazyFail"))
            .opt("fs.test.something", true);
    interceptFuture(FileNotFoundException.class, "", builder.build());
  }

  @Test
  public void testOpenFileFailExceptionally() throws Throwable {
    describe("openFile missing file chains into exceptionally()");
    FutureDataInputStreamBuilder builder =
        fsShim.openFile(path("testOpenFileFailExceptionally"))
            .opt("fs.test.something", true);
    assertNull("exceptional uprating",
        builder.build().exceptionally(ex -> null).get());
  }

  @Test
  public void testAwaitFutureFailToFNFE() throws Throwable {
    describe("Verify that FutureIOSupport.awaitFuture extracts IOExceptions");
    FutureDataInputStreamBuilder builder =
        fsShim.openFile(path("testAwaitFutureFailToFNFE"))
            .opt("fs.test.something", true);
    intercept(FileNotFoundException.class,
        () -> awaitFuture(builder.build()));
  }

  @Test
  public void testAwaitFutureTimeoutFailToFNFE() throws Throwable {
    describe("Verify that FutureIOSupport.awaitFuture with a timeout works");
    FutureDataInputStreamBuilder builder =
        fsShim.openFile(path("testAwaitFutureFailToFNFE"))
            .opt("fs.test.something", true);
    intercept(FileNotFoundException.class,
        () -> awaitFuture(builder.build(),
            10, TimeUnit.DAYS));
  }

  @Test
  public void testOpenFileExceptionallyTranslating() throws Throwable {
    describe("openFile missing file chains into exceptionally()");
    CompletableFuture<FSDataInputStream> f = fsShim
        .openFile(path("testOpenFileExceptionallyTranslating")).build();
    interceptFuture(RuntimeException.class,
        "exceptionally",
        f.exceptionally(ex -> {
          throw new RuntimeException("exceptionally", ex);
        }));
  }

  @Test
  public void testChainedFailureAwaitFuture() throws Throwable {
    describe("await Future handles chained failures");
    CompletableFuture<FSDataInputStream> f = fsShim
        .openFile(path("testChainedFailureAwaitFuture"))
        .withFileStatus(null)
        .build();
    intercept(RuntimeException.class,
        "exceptionally",
        () -> awaitFuture(
            f.exceptionally(ex -> {
              throw new RuntimeException("exceptionally", ex);
            })));
  }

  @Test
  public void testOpenFileApplyRead() throws Throwable {
    describe("use the apply sequence to read a whole file");
    Path path = path("testOpenFileApplyRead");
    FileSystem fs = getFileSystem();
    int len = 4096;
    createFile(fs, path, true,
        dataset(len, 0x40, 0x80));
    FileStatus st = fs.getFileStatus(path);
    CompletableFuture<Long> readAllBytes = fsShim.openFile(path)
        .withFileStatus(st)
        .build()
        .thenApply(ShimTestUtils::readStream);

    assertEquals("Wrong number of bytes read value",
        len,
        (long) readAllBytes.get());
    // now reattempt with a new FileStatus and a different path
    // other than the final name element
    // implementations MUST use path in openFile() call
    FileStatus st2 = new FileStatus(
        len, false,
        st.getReplication(),
        st.getBlockSize(),
        st.getModificationTime(),
        st.getAccessTime(),
        st.getPermission(),
        st.getOwner(),
        st.getGroup(),
        new Path("gopher:///localhost:/" + path.getName()));
    assertEquals("Wrong number of bytes read value",
        len,
        (long) fsShim.openFile(path)
            .withFileStatus(st2)
            .build()
            .thenApply(ShimTestUtils::readStream)
            .get());
  }

  @Test
  public void testOpenFileApplyAsyncRead() throws Throwable {
    describe("verify that async accept callbacks are evaluated");
    Path path = path("testOpenFileApplyAsyncRead");
    FileSystem fs = getFileSystem();
    createFile(fs, path, true,
        dataset(4, 0x40, 0x80));
    CompletableFuture<FSDataInputStream> future = fsShim.openFile(path).build();
    AtomicBoolean accepted = new AtomicBoolean(false);
    future.thenApply(stream -> {
      accepted.set(true);
      return stream;
    }).get().close();
    assertTrue("async accept operation not invoked",
        accepted.get());
  }

  /**
   * Open a file with a null status, and the length
   * passed in as an opt() option (along with sequential IO).
   * The file is opened, the data read, and it must match
   * the source data.
   * opt() is used so that integration testing with external
   * filesystem connectors will downgrade if the option is not
   * recognized.
   */
  @Test
  public void testOpenFileNullStatusButFileLength() throws Throwable {
    describe("use openFile() with a null status and expect the status to be"
        + " ignored. block size, fadvise and length are passed in as"
        + " opt() options");
    Path path = path("testOpenFileNullStatus");
    FileSystem fs = getFileSystem();
    int len = 4;
    byte[] result = new byte[len];
    byte[] dataset = dataset(len, 0x40, 0x80);
    createFile(fs, path, true,
        dataset);
    CompletableFuture<FSDataInputStream> future = fsShim.openFile(path)
        .withFileStatus(null)
        .opt(FS_OPTION_OPENFILE_READ_POLICY,
            "unknown, sequential, random")
        .opt(FS_OPTION_OPENFILE_BUFFER_SIZE, 32768)
        .opt(FS_OPTION_OPENFILE_LENGTH, len)
        .build();

    try (FSDataInputStream in = future.get()) {
      in.readFully(result);
    }
    compareByteArrays(dataset, result, len);
  }

}
