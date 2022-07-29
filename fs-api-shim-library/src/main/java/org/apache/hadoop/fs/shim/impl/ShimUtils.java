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
import java.io.UncheckedIOException;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.Optional;
import java.util.concurrent.ExecutionException;
import javax.annotation.Nullable;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Shim utilities.
 */
public final class ShimUtils {

  private static final Logger LOG = LoggerFactory.getLogger(ShimUtils.class);

  /**
   * convert any wrapped exception to an IOE.
   * If there is no cause, convert the supplied exception instead.
   * @param e exception
   * @return an IOException to throw.
   */
  public static IOException unwrapAndconvertToIOException(Exception e) {
    Throwable cause = e.getCause();
    return convertUnwrappedExceptionToIOE(cause != null ? cause : e);
  }

  /**
   * Convert to an IOE and return for throwing.
   * Wrapper exceptions (invocation, execution)
   * are unwrapped first.
   * If the cause is actually a RuntimeException
   * other than UncheckedIOException
   * or Error, it is thrown
   * @param e exception
   * @throws RuntimeException if that is the type
   * @throws Error if that is the type
   */
  public static IOException convertToIOException(Exception e) {
    if (e instanceof InvocationTargetException
        || e instanceof ExecutionException) {
      return unwrapAndconvertToIOException(e);
    } else {
      return convertUnwrappedExceptionToIOE(e);
    }
  }

  /**
   * Convert to an IOE and return for throwing.
   * If the cause is actually a RuntimeException
   * other than UncheckedIOException
   * or Error, it is thrown.
   * @param thrown exception
   * @throws RuntimeException if that is the type of {@code thrown}.
   * @throws Error if that is the type  of {@code thrown}.
   */
  public static IOException convertUnwrappedExceptionToIOE(final Throwable thrown) {
    if (thrown instanceof UncheckedIOException) {
      return ((UncheckedIOException) thrown).getCause();
    }
    if (thrown instanceof RuntimeException) {
      throw (RuntimeException) thrown;
    }
    if (thrown instanceof Error) {
      throw (Error) thrown;
    }
    if (thrown instanceof IOException) {
      return (IOException) thrown;
    }
    return new IOException(thrown);
  }

  /**
   * Get a method from the source class, or null if not found.
   * @param source source
   * @param name method name
   * @param parameterTypes parameters
   * @return the method or null
   */
  public static Method getMethod(Class<?> source, String name, Class<?>... parameterTypes) {
    try {
      return source.getMethod(name, parameterTypes);
    } catch (NoSuchMethodException | SecurityException e) {
      LOG.debug("Class {} does not implement {}", source, name);
      return null;
    }
  }

  /**
   * Get a method from the source class, or null if not found.
   * @param source source
   * @param name method name
   * @param parameterTypes parameters
   * @return the method or null
   */
  public static Invocation getInvocation(
      Class<?> source, String name, Class<?>... parameterTypes) {
    try {
      return new Invocation(name, source.getMethod(name, parameterTypes));
    } catch (NoSuchMethodException | SecurityException e) {
      LOG.debug("Class {} does not implement {}", source, name);
      return new Invocation(name, null);
    }
  }

  /**
   * Get the method as a possibly empty Optional value.
   * @param source source
   * @param name method name
   * @param parameterTypes parameters
   * @return the method or Optional.empty()
   */
  public static Optional<Method> getMethodOptional(Class<?> source,
      String name,
      Class<?>... parameterTypes) {
    return Optional.ofNullable(getMethod(source, name, parameterTypes));
  }

  /**
   * Invoke a method with exception unwrap/uprate.
   * If the method is null, raise UnsupportedOperationException
   * @param operation operation name for errors
   * @param instance instance to invoke
   * @param method method, may be null
   * @param parameters parameters
   * @return the result
   * @throws UnsupportedOperationException if the method is null
   * @throws RuntimeException for all RTEs raised by invoked methods except UncheckedIOEs
   * @throws IOException when converting/unwrappping thrown exceptions
   */
  public static Object invokeOperation(
      String operation,
      Object instance,
      @Nullable Method method,
      Object... parameters) throws IOException {
    if (method == null) {
      throw new UnsupportedOperationException("No " +
          operation + " in " + instance);
    }
    try {
      return method.invoke(instance, parameters);
    } catch (IllegalAccessException
             | InvocationTargetException
             | IllegalArgumentException  ex) {
      throw convertToIOException(ex);
    }
  }

}
