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
import java.util.Map;

public class DowngradeUtilsTest {

    private Map<String,DowngradeConfig> downgradeConfigTable = new HashMap<String, DowngradeConfig>();
    private long maxTime;

    @Before
    public void init() {
        maxTime = System.currentTimeMillis() + 10000;

        DowngradeConfig downgradeConfig = new DowngradeConfig();
        downgradeConfig.setDownTimeout(System.currentTimeMillis());
        downgradeConfig.setDowngradeEnable(true);

        downgradeConfig.setHostDownTimeout(new HashMap<String, Long>());
        downgradeConfig.getHostDownTimeout().put("host1", maxTime);
        downgradeConfigTable.put("simple",downgradeConfig);
    }

    @Test
    public void testConvertTimedConfig() {
        TimedConfig timedConfig = DowngradeUtils.toTimedConfig(downgradeConfigTable);
        Assert.assertTrue(timedConfig != null);
        Assert.assertTrue(timedConfig.getTimeout() == maxTime);
    }

    @Test
    public void testMaxTimeout() {
        long maxTimeout = DowngradeUtils.getMaxTimeout(downgradeConfigTable.get("simple"));
        Assert.assertTrue(maxTimeout == this.maxTime);
    }

    @Test
    public void testConvertDowngradeConfig() {
        TimedConfig timedConfig = DowngradeUtils.toTimedConfig(downgradeConfigTable);
        Map<String, DowngradeConfig> downgradeConfigMap = DowngradeUtils.fromTimedConfig(timedConfig);
        DowngradeConfig downgradeConfig = downgradeConfigMap.get("simple");
        Assert.assertTrue(downgradeConfig.getHostDownTimeout().size() > 0);
        Assert.assertTrue(downgradeConfig.isDowngradeEnable());
    }
}
