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
import java.lang.reflect.Method;
import javax.annotation.Nullable;

/**
 * A method which can be invoked.
 */
public final class Invocation {

  /**
   * Method name for error messages.
   */
  private final String name;

  /**
   * Method to invoke.
   */
  private final @Nullable Method method;

  public Invocation(final String name, final @Nullable Method method) {
    this.name = name;
    this.method = method;
  }

  public boolean available() {
    return method != null;
  }

  /**
   * Invoke the method with exception unwrap/uprate.
   * If {@link #method} is null, raise UnsupportedOperationException
   * @param instance instance to invoke
   * @param parameters parameters
   * @return the result
   * @throws IOException when converting/unwrappping thrown exceptions other than RTEs.
   */
 public Object invoke(
     final Object instance,
     final Object... parameters) throws IOException {
    return ShimUtils.invokeOperation(name, instance, method, parameters);
  }

  /**
   * Generate an invocation which is always unavailable.
   * @param name name for the exception text.
   * @return an invocation which always raises
   */
  public static Invocation unavailable(String name) {
    return new Invocation(name, null);
  }
}
