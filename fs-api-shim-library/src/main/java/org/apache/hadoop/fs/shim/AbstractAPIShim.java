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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static java.util.Objects.requireNonNull;

public abstract class AbstractAPIShim<T> {
  private static final Logger LOG = LoggerFactory.getLogger(AbstractAPIShim.class);

  /**
   * Class being shimmed.
   */
  private final Class<T> clazz;

  /**
   * Instance being shimmed.
   */
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

}
