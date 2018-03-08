/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */


package org.apache.rocketmq.common.downgrade;

import org.apache.rocketmq.common.TimedConfig;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.HashMap;

public class DowngradeUtilsTest {

    private DowngradeConfig downgradeConfig;
    private long maxTime;

    @Before
    public void init() {
        maxTime = System.currentTimeMillis() + 10000;

        downgradeConfig = new DowngradeConfig();
        downgradeConfig.setDownTimeout(System.currentTimeMillis());
        downgradeConfig.setTopic("simple");
        downgradeConfig.setDowngradeEnable(true);

        downgradeConfig.setHostDownTimeout(new HashMap<String, Long>());
        downgradeConfig.getHostDownTimeout().put("host1", maxTime);
    }

    @Test
    public void testConvertTimedConfig() {
        TimedConfig timedConfig = DowngradeUtils.toTimedConfig(downgradeConfig);
        Assert.assertTrue(timedConfig != null);
        Assert.assertTrue(timedConfig.getTimeout() == maxTime);
    }

    @Test
    public void testMaxTimeout() {
        long maxTimeout = DowngradeUtils.getMaxTimeout(downgradeConfig);
        Assert.assertTrue(maxTimeout == this.maxTime);
    }

    @Test
    public void testConvertDowngradeConfig() {
        TimedConfig timedConfig = DowngradeUtils.toTimedConfig(downgradeConfig);
        DowngradeConfig downgradeConfig = DowngradeUtils.fromTimedConfig(timedConfig);
        Assert.assertTrue(downgradeConfig.getHostDownTimeout().size() > 0);
        Assert.assertTrue(downgradeConfig.isDowngradeEnable());
        Assert.assertTrue(downgradeConfig.getTopic().equals("simple"));
    }
}
