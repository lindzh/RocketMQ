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
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package org.apache.rocketmq.test.admin;

import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.common.TimedConfig;
import org.apache.rocketmq.common.UtilAll;
import org.apache.rocketmq.common.constant.GroupType;
import org.apache.rocketmq.common.downgrade.DowngradeConfig;
import org.apache.rocketmq.common.downgrade.DowngradeUtils;
import org.apache.rocketmq.common.namesrv.NamesrvUtil;
import org.apache.rocketmq.common.protocol.ResponseCode;
import org.apache.rocketmq.common.protocol.body.TimedKVTable;
import org.apache.rocketmq.remoting.exception.RemotingException;
import org.apache.rocketmq.test.base.BaseConf;
import org.apache.rocketmq.test.util.MQAdmin;
import org.apache.rocketmq.tools.admin.DefaultMQAdminExt;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.util.HashMap;

public class AdminTimedKVTestIT extends BaseConf {

    private DefaultMQAdminExt defaultMQAdminExt;

    public static final String NAMESPACE = "test-namespace";

    @Before
    public void initAdminExt() throws MQClientException {
        defaultMQAdminExt = MQAdmin.createMQAdminExt(nsAddr);
        defaultMQAdminExt.start();
    }

    @Test
    public void testTimedKV() throws RemotingException, MQClientException, InterruptedException {

        TimedKVTable timedKVTable = null;
        try {
            timedKVTable = defaultMQAdminExt.getTimedKVListByNamespace(NAMESPACE);
        }catch (MQClientException e){
            Assert.assertTrue(e.getResponseCode()== ResponseCode.QUERY_NOT_FOUND);
        }
        Assert.assertTrue(timedKVTable==null);

        long timeout = System.currentTimeMillis()+300;

        defaultMQAdminExt.putTimedKVConfig(NAMESPACE,"key1",new TimedConfig("value",timeout));
        defaultMQAdminExt.putTimedKVConfig(NAMESPACE,"key2",new TimedConfig("value2",timeout+5000));
        defaultMQAdminExt.putTimedKVConfig(NAMESPACE,"key3",new TimedConfig("value3",timeout+7000));
        TimedConfig timedKVConfig = defaultMQAdminExt.getTimedKVConfig(NAMESPACE, "key1");
        Assert.assertTrue(timedKVConfig!=null);
        Assert.assertTrue(timedKVConfig.getTimeout()==timeout&&timedKVConfig.getValue().equals("value"));

        timedKVTable = defaultMQAdminExt.getTimedKVListByNamespace(NAMESPACE);
        Assert.assertTrue(timedKVTable!=null);
        Assert.assertTrue(timedKVTable.getTable().size()==3);

        Thread.sleep(300);
        timedKVConfig = defaultMQAdminExt.getTimedKVConfig(NAMESPACE, "key1");
        Assert.assertTrue(timedKVConfig==null);

        TimedConfig kvConfig = defaultMQAdminExt.getTimedKVConfig(NAMESPACE, "key2");
        Assert.assertTrue(kvConfig!=null);
        defaultMQAdminExt.deleteTimedKVConfig(NAMESPACE, "key2");
        kvConfig = defaultMQAdminExt.getTimedKVConfig(NAMESPACE, "key2");
        Assert.assertTrue(kvConfig==null);

        long tt = System.currentTimeMillis()+500;

        DowngradeConfig downgradeConfig = new DowngradeConfig();
        downgradeConfig.setHostDownTimeout(new HashMap<String, Long>());
        downgradeConfig.setDowngradeEnable(true);
        downgradeConfig.setTopic("topic1");
        downgradeConfig.getHostDownTimeout().put("host1",tt);
        defaultMQAdminExt.updateDowngradeConfig(GroupType.CONSUMER,"consumer1","topic1",downgradeConfig);

        DowngradeConfig config = defaultMQAdminExt.getDowngradeConfig(GroupType.CONSUMER, "consumer1", "topic1");
        Assert.assertTrue(config!=null);

        String downgradeKey = DowngradeUtils.genDowngradeKey(GroupType.CONSUMER, "consumer1", "topic1");
        TimedConfig downgradeTimedConfig = defaultMQAdminExt.getTimedKVConfig(NamesrvUtil.TIMED_NAMESPACE_CLIENT_DOWNGRADE_CONFIG, downgradeKey);
        Assert.assertTrue(downgradeTimedConfig!=null);
        Assert.assertTrue(downgradeTimedConfig.getTimeout()==tt);
        Assert.assertTrue(downgradeTimedConfig.getValue().contains("host1"));
        System.out.print("downgradeconfig:"+downgradeTimedConfig.getValue()+"%n");
        Thread.sleep(500);
        config = defaultMQAdminExt.getDowngradeConfig(GroupType.CONSUMER, "consumer1", "topic1");
        Assert.assertTrue(config==null);
    }

    @After
    public void shutdownAdminExt(){
        if(defaultMQAdminExt!=null){
            defaultMQAdminExt.shutdown();
        }

        String timedKVConfigPath = namesrvController.getNamesrvConfig().getTimedKVConfigPath();
        UtilAll.deleteFile(new File(timedKVConfigPath));
    }

}
