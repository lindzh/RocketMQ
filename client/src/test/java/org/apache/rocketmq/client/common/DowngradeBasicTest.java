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

package org.apache.rocketmq.client.common;

import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.impl.factory.MQClientInstance;
import org.apache.rocketmq.common.UtilAll;
import org.apache.rocketmq.common.constant.GroupType;
import org.apache.rocketmq.common.downgrade.DowngradeConfig;
import org.apache.rocketmq.common.downgrade.DowngradeUtils;
import org.apache.rocketmq.remoting.exception.RemotingException;
import java.lang.reflect.Field;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class DowngradeBasicTest {

    protected String topic2 = "Simple";
    protected String topic3 = "Foo";

    protected Map<String,DowngradeConfig> downgradeConfigMap = new HashMap<String,DowngradeConfig>();

    public void initDowngradeTest(MQClientInstance mQClientFactory,GroupType groupType,String group) throws RemotingException, MQClientException, InterruptedException, IllegalAccessException, NoSuchFieldException {
        DowngradeConfig downgradeConfig2 = new DowngradeConfig();
        downgradeConfig2.setHostDownTimeout(new HashMap<String, Long>());
        downgradeConfig2.getHostDownTimeout().put(new String(UtilAll.ipToIPv4Str(UtilAll.getIP())),System.currentTimeMillis()+1000000);
        downgradeConfig2.setDowngradeEnable(true);
        downgradeConfigMap.put(topic2,downgradeConfig2);

        DowngradeConfig downgradeConfig = new DowngradeConfig();
        downgradeConfig.setDownTimeout(System.currentTimeMillis()+1000000);
        downgradeConfig.setDowngradeEnable(true);
        downgradeConfigMap.put(topic3,downgradeConfig);

        ConcurrentHashMap<String, Map<String, DowngradeConfig>> configMap = new ConcurrentHashMap<String, Map<String, DowngradeConfig>>();
        configMap.put(DowngradeUtils.genDowngradeKey(groupType,group),downgradeConfigMap);
        Field field = MQClientInstance.class.getDeclaredField("downgradeConfigTable");
        field.setAccessible(true);
        field.set(mQClientFactory, configMap);
    }
}
