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

import org.apache.rocketmq.common.MixAll;
import org.apache.rocketmq.common.TimedConfig;
import org.apache.rocketmq.common.UtilAll;
import org.apache.rocketmq.common.namesrv.NamesrvConfig;
import org.apache.rocketmq.common.namesrv.NamesrvUtil;
import org.apache.rocketmq.namesrv.NamesrvController;
import org.apache.rocketmq.remoting.netty.NettyServerConfig;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

public class TimedKVConfigManagerTest {

    private TimedKVConfigManager timedKVConfigManager;

    private ScheduledExecutorService scheduledExecutorService;

    private long SEC_2 = 1000*2;

    private long MIN_1 = 1000*60;

    NettyServerConfig nettyServerConfig = new NettyServerConfig();
    NamesrvConfig namesrvConfig = new NamesrvConfig();

    protected String configFile = System.getProperty("user.home") + "/rocketmq/config";

    @Before
    public void setup() throws Exception {
        namesrvConfig.setTimedKVConfigPath(configFile);
        NamesrvController nameSrvController = new NamesrvController(namesrvConfig, nettyServerConfig);
        timedKVConfigManager = new TimedKVConfigManager(nameSrvController);
        timedKVConfigManager.load();
        scheduledExecutorService = Executors.newSingleThreadScheduledExecutor();
        scheduledExecutorService.scheduleAtFixedRate(new Runnable() {
            @Override
            public void run() {
                timedKVConfigManager.scanTimeoutKVConfig();
            }
        },100,100, TimeUnit.MILLISECONDS);
    }

    @After
    public void shutdown(){
        if(scheduledExecutorService!=null){
            scheduledExecutorService.shutdown();
        }
        UtilAll.deleteFile(new File(configFile));
    }

    @Test
    public void testPutKVConfig() throws InterruptedException {
        timedKVConfigManager.putTimedKVConfig(NamesrvUtil.TIMED_NAMESPACE_CLIENT_DOWNGRADE_CONFIG, "UnitTest", new TimedConfig("unit",System.currentTimeMillis()+SEC_2));
        byte[] kvConfig = timedKVConfigManager.getTimedKVListByNamespace(NamesrvUtil.TIMED_NAMESPACE_CLIENT_DOWNGRADE_CONFIG);
        assertThat(kvConfig).isNotNull();
        TimedConfig timedKVConfig = timedKVConfigManager.getTimedKVConfig(NamesrvUtil.TIMED_NAMESPACE_CLIENT_DOWNGRADE_CONFIG, "UnitTest");
        assertThat(timedKVConfig!=null).isTrue();

        Thread.sleep(SEC_2);
        timedKVConfig = timedKVConfigManager.getTimedKVConfig(NamesrvUtil.TIMED_NAMESPACE_CLIENT_DOWNGRADE_CONFIG, "UnitTest");
        assertThat(timedKVConfig!=null).isFalse();
    }

    @Test
    public void testDeleteKVConfig() {
        timedKVConfigManager.putTimedKVConfig(NamesrvUtil.TIMED_NAMESPACE_CLIENT_DOWNGRADE_CONFIG, "UnitTest2", new TimedConfig("unit",System.currentTimeMillis()+MIN_1));
        timedKVConfigManager.deleteTimedKVConfig(NamesrvUtil.TIMED_NAMESPACE_CLIENT_DOWNGRADE_CONFIG, "UnitTest2");
        byte[] kvConfig = timedKVConfigManager.getTimedKVListByNamespace(NamesrvUtil.TIMED_NAMESPACE_CLIENT_DOWNGRADE_CONFIG);
        String configStr = new String(kvConfig);
        System.out.print(configStr);
        Assert.assertTrue(!configStr.contains("UnitTest2"));

        TimedConfig timedKVConfig = timedKVConfigManager.getTimedKVConfig(NamesrvUtil.TIMED_NAMESPACE_CLIENT_DOWNGRADE_CONFIG, "UnitTest2");
        assertThat(timedKVConfig).isNull();
    }

    @Test
    public void testLoadAndPersist() throws IOException {
        timedKVConfigManager.putTimedKVConfig(NamesrvUtil.TIMED_NAMESPACE_CLIENT_DOWNGRADE_CONFIG, "UnitTest3", new TimedConfig("unit",System.currentTimeMillis()+MIN_1));
        timedKVConfigManager.persist();
        String context = MixAll.file2String(configFile);
        Assert.assertTrue(context.contains("UnitTest3"));
        timedKVConfigManager.deleteTimedKVConfig(NamesrvUtil.TIMED_NAMESPACE_CLIENT_DOWNGRADE_CONFIG, "UnitTest3");
        context = MixAll.file2String(configFile);
        Assert.assertTrue(!context.contains("UnitTest3"));
    }
}
