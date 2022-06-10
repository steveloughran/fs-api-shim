# fs-api-shim

Shim library for allowing hadoop libraries to call the recent higher performance filesystem API
methods when available, but still compile against older hadoop binaries.

This allows them to call operations which are potentially significantly
higher performance, especially when working with object stores.

For example, the `openFile()` builder API allows a library to declare
the seek policy to use (sequential, random, ...) and if a `FileStatus`
is passed in, the initial HEAD request to determine the file size
can be skipped. This will reduce latency and significantly
improve read performance.

Another example is the high performance vectored IO API
of (HADOOP-18103)[https://issues.apache.org/jira/browse/].
On hadoop releases with this API, it will parallelize a list
of read operations.
The base implementation converts these to seek/read calls,
but object stores may implement the reads as a set of
parallel ranged GET requests (as does s3a).
File formats which read stripes of data can use this API
call to gain significant performance improvements.

The library contains a number of shim classes, which can
be instantiated with a supplied instance of a a class available
in the Hadoop 3.2.0 APIs.

These classes use reflection to bind to methods not available in the
lhadoop 3.2.0 release.

These API calls can be probed for before invocation.
When unavailable, the operations will either fall back to a default
implementation (example: `openFile()`), or raise
an `UnsupportedOperationException`

Note that the presence of the methods does not guarantee that the
invoked instance will succeed, rather that the outcome is the same
as it would be if the method was called directly.

### Why 3.2.0?

Hadoop branch-3.2 is the oldest hadoop-3.x branch which has active releases for critical
bugs. It is the de-facto baseline API for all Hadoop runtimes/platforms.

Claiming to support hadoop branch-2 would mean using Java 7 APIs, as those running
Hadoop clusters on branch 2 builds generally do this.
It would also force the shim library to add shim methods for more API calls.

Finally, it would really complicate testing. The shim library would need to
be built on a java7 JDK, but integration tests running against hadoop 3 releases
would need to be on java 8.

Libraries which want to use these new APIs must build with a hadoop version of 3.2.0
*or later*.


## `org.apache.hadoop.fs.shim.FSDataInputStreamShim`

| Method               | Version | JIRA         | Fallback                        |
|----------------------|---------|--------------|---------------------------------|
| `openFile()`         | 3.3.0   | HADOOP-15229 | `open(Path)`                    |
| `msync()`            | 3.3.2   | HDFS-15567   | `UnsupportedOperationException` |
| `hasPathCapability()`| 3.3.0   | HADOOP-15691 | `false`                         |
|                      |         |              |                                 |

## `org.apache.hadoop.fs.shim.FSDataInputStreamShim`

| Method                     | Version | JIRA         | Fallback                        |
|----------------------------|---------|--------------|---------------------------------|
| `ByteBufferPositionedRead` | 3.3.0   | HDFS-3246    | `UnsupportedOperationException` |
| Vectored IO                | (3.4.0) | HADOOP-18103 | `UnsupportedOperationException` |
|                            |         |              |                                 |

### `ByteBufferPositionedRead` interface

The `ByteBufferPositionedRead` interface allows an application to read/readFully into
a byte buffer from a specific offset.

If the `ByteBufferPositionedRead` interface is not implemented by the wrapped input stream,
the methods it exports, `FSDataInputStream.read(long position, ByteBuffer buf)` and
`FSDataInputStream.readFully(long position, ByteBuffer buf)` 
will both throw `UnsupportedOperationException`.

To verify that a stream implements the API all the way through to the filesystem, 
call `hasCapability("in:preadbytebuffer")`
on the stream.
All stream which implement `ByteBufferPositionedRead` MUST return `true` on this probe.

## Testing the library.


_this is going to be fun_

The plan is to provide contract tests (subclasses of hadoop-common test `AbstractFSContractTestBase`) for different shim classes.
the library module's implementations will verify that when executed on hadoop 3.2.0
they work/downgrade/fail as expected.

Implementations will be provided to run against: local fs, minihdfs, s3a and abfs.

The tests will be lifted from hadoop's contract tests, where possible, so as to provide
a reference implementation. 

A separate module will run the same test suites against a later version of hadoop, and expect different outcomes.

The outcomes will vary with build, so will have to be dynamic.
There will be profiles for hadoop 3.2.4, hadoop 3.3.4, cloudera cdp and hadoop-3.4.0 (-SNAPSHOT) initially.
