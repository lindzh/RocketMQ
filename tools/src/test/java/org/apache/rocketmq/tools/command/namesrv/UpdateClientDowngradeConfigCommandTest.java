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

package org.apache.rocketmq.tools.command.namesrv;

import org.apache.commons.cli.CommandLine;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.impl.MQClientAPIImpl;
import org.apache.rocketmq.common.TimedConfig;
import org.apache.rocketmq.common.constant.GroupType;
import org.apache.rocketmq.common.downgrade.DowngradeUtils;
import org.apache.rocketmq.common.namesrv.NamesrvUtil;
import org.apache.rocketmq.remoting.exception.RemotingException;
import org.apache.rocketmq.tools.command.SubCommandException;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;

import static org.mockito.Mockito.mock;

public class UpdateClientDowngradeConfigCommandTest extends BasicTimedKVTest{

    private static UpdateClientDowngradeConfigCommand updateClientDowngradeConfigCommand = Mockito.spy(UpdateClientDowngradeConfigCommand.class);

    @Before
    public void init() throws NoSuchFieldException, IllegalAccessException, RemotingException, MQClientException, InterruptedException, SubCommandException {

        MQClientAPIImpl mQClientAPIImpl = mock(MQClientAPIImpl.class);
        init4TimedKV(updateClientDowngradeConfigCommand,mQClientAPIImpl);
        initTimedKV(mQClientAPIImpl);
        initPrintStream();
        timedConfigHashMap.put(NamesrvUtil.TIMED_NAMESPACE_CLIENT_DOWNGRADE_CONFIG,new HashMap<String, TimedConfig>());
    }

    @After
    public void shutdown(){
        shutdownPrintStream();
    }

    @Test
    public void testExec() throws SubCommandException {
        long timeout = System.currentTimeMillis()+100000;
        String format = new SimpleDateFormat("yyyy-MM-dd#HH:mm").format(new Date(timeout));
        CommandLine commandLine = parseCommandLine(updateClientDowngradeConfigCommand, new String[]{"-c","consumer1","-t","topic10","-o",format});
        updateClientDowngradeConfigCommand.execute(commandLine,null,null);
        String printContent = getPrintContent();
        System.err.println(printContent);
        Assert.assertTrue(printContent.contains("success"));
        String key = DowngradeUtils.genDowngradeKey(GroupType.CONSUMER, "consumer1", "topic10");
        Assert.assertTrue(timedConfigHashMap.get(NamesrvUtil.TIMED_NAMESPACE_CLIENT_DOWNGRADE_CONFIG).get(key).getValue().contains("downgradeEnable"));
        System.err.println(timedConfigHashMap.get(NamesrvUtil.TIMED_NAMESPACE_CLIENT_DOWNGRADE_CONFIG).get(key).getValue());
    }
}
