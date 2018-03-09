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

package org.apache.rocketmq.namesrv.kvconfig;

import com.alibaba.fastjson.JSON;
import org.apache.rocketmq.common.TimedConfig;
import org.apache.rocketmq.common.downgrade.DowngradeConfig;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.HashMap;

public class TimedKVConfigSerializeWrapperTest {

    private TimedKVConfigSerializeWrapper timedKVConfigSerializeWrapper;

    @Before
    public void init() {
        timedKVConfigSerializeWrapper = new TimedKVConfigSerializeWrapper();
        timedKVConfigSerializeWrapper.setConfigTable(new HashMap<String, HashMap<String, TimedConfig>>());
    }

    @Test
    public void testSerializeAndDeserilize() {
        timedKVConfigSerializeWrapper.getConfigTable().put("namespace1", new HashMap<String, TimedConfig>());
        timedKVConfigSerializeWrapper.getConfigTable().put("namespace2", new HashMap<String, TimedConfig>());
        timedKVConfigSerializeWrapper.getConfigTable().get("namespace1").put("testSerializeAndDeserilize",
            new TimedConfig("testSerializeAndDeserilize config", System.currentTimeMillis()));
        timedKVConfigSerializeWrapper.getConfigTable().get("namespace1").put("testHello",
            new TimedConfig("testHello config", System.currentTimeMillis()));
        timedKVConfigSerializeWrapper.getConfigTable().get("namespace1").put("timedKV",
            new TimedConfig("timedKV config", System.currentTimeMillis()));

        DowngradeConfig downgradeConfig = new DowngradeConfig();
        downgradeConfig.setDownTimeout(System.currentTimeMillis());
        String downgradeConfigJson = JSON.toJSONString(downgradeConfig);
        timedKVConfigSerializeWrapper.getConfigTable().get("namespace2").put("downgrade",
            new TimedConfig(downgradeConfigJson, System.currentTimeMillis()));

        String jsonContext = timedKVConfigSerializeWrapper.toJson(true);
        Assert.assertTrue(jsonContext.contains("downgrade"));
        Assert.assertTrue(jsonContext.contains("testSerializeAndDeserilize"));
        Assert.assertTrue(jsonContext.contains("timedKV"));

        System.out.println(jsonContext);
        TimedKVConfigSerializeWrapper timedKVConfigSerializeWrapper = TimedKVConfigSerializeWrapper.fromJson(jsonContext,
            TimedKVConfigSerializeWrapper.class);

        Assert.assertTrue(timedKVConfigSerializeWrapper.getConfigTable().size()==timedKVConfigSerializeWrapper.getConfigTable().size());
    }

}
