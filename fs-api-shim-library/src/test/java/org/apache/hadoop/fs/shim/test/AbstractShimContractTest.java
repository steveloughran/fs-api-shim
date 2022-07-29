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

package org.apache.hadoop.fs.shim.test;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.StreamCapabilities;
import org.apache.hadoop.fs.contract.AbstractFSContract;
import org.apache.hadoop.fs.contract.AbstractFSContractTestBase;
import org.apache.hadoop.fs.shim.test.binding.FileContract;
import org.apache.hadoop.fs.shim.test.binding.Hadoop320Features;

/**
 * Abstract FS contract test.
 * This implementation always returns the local FS as the contract, though
 * it can be overridden.
 */
public class AbstractShimContractTest extends AbstractFSContractTestBase
    implements StreamCapabilities {

  private StreamCapabilities versionCapabilities;

  public AbstractShimContractTest() {
    versionCapabilities = new Hadoop320Features();
  }

  public StreamCapabilities getVersionCapabilities() {
    return versionCapabilities;
  }

  public void setVersionCapabilities(final StreamCapabilities versionCapabilities) {
    this.versionCapabilities = versionCapabilities;
  }

  @Override
  protected AbstractFSContract createContract(final Configuration conf) {
    return new FileContract(conf);
  }

  @Override
  public boolean hasCapability(final String capability) {
    return versionCapabilities.hasCapability(capability);
  }
}
