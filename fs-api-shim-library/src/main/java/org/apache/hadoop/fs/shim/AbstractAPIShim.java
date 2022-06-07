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
import java.io.UncheckedIOException;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.Optional;
import java.util.concurrent.ExecutionException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static java.util.Objects.requireNonNull;

public abstract class AbstractAPIShim<T> {
  private static final Logger LOG = LoggerFactory.getLogger(AbstractAPIShim.class);
  private final Class<T> clazz;
  private final T instance;

  public AbstractAPIShim(final Class<T> clazz, final T instance) {
    this.clazz = requireNonNull(clazz);
    this.instance = requireNonNull(instance);
  }

  public Class<T> getClazz() {
    return clazz;
  }

  public T getInstance() {
    return instance;
  }

  /**
   * Get a method from the source class, or null if not found.
   * @param source source
   * @param name method name
   * @param parameterTypes parameters
   * @return the method or null
   */
  protected Method getMethod(Class<?> source, String name, Class<?>... parameterTypes) {
    try {
      return source.getMethod(name, parameterTypes);
    } catch (NoSuchMethodException | SecurityException e) {
      LOG.debug("Class {} does not implement {}", source, name);
      return null;
    }
  }

  /**
   * Get the method as a possibly empty Optional value.
   * @param source source
   * @param name method name
   * @param parameterTypes parameters
   * @return the method or Optional.empty()
   */
  protected Optional<Method> getMethodOptional(Class<?> source,
      String name,
      Class<?>... parameterTypes) {
    return Optional.ofNullable(getMethod(source, name, parameterTypes));
  }

  protected IOException unwrapAndconvertToIOException(Exception e) {
    Throwable cause = e.getCause();
    return convertUnwrappedException(cause != null ? cause : e);
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
  protected IOException convertToIOException(Exception e) {
    if (e instanceof InvocationTargetException || e instanceof ExecutionException) {
      return unwrapAndconvertToIOException(e);
    } else {
      return convertUnwrappedException(e);
    }
  }

  /**
   * Convert to an IOE and return for throwing.
   * If the cause is actually a RuntimeException
   * other than UncheckedIOException
   * or Error, it is thrown
   * @param cause exception
   * @throws RuntimeException if that is the type
   * @throws Error if that is the type
   */
  protected IOException convertUnwrappedException(final Throwable cause) {
    if (cause instanceof UncheckedIOException) {
      return ((UncheckedIOException) cause).getCause();
    }
    if (cause instanceof RuntimeException) {
      throw (RuntimeException) cause;
    }
    if (cause instanceof Error) {
      throw (Error) cause;
    }
    if (cause instanceof IOException) {
      return (IOException) cause;
    }
    return new IOException(cause);
  }

}
