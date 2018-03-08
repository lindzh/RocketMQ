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

public class UpdateTimedKvConfigCommandTest extends BasicTimedKVTest {

    private static UpdateTimedKvConfigCommand updateTimedKvConfigCommand = Mockito.spy(UpdateTimedKvConfigCommand.class);

    public static final String TIMED_NAMESPACE = "namespace1";

    @Before
    public void init() throws NoSuchFieldException, IllegalAccessException, RemotingException, MQClientException, InterruptedException, SubCommandException {

        MQClientAPIImpl mQClientAPIImpl = mock(MQClientAPIImpl.class);
        init4TimedKV(updateTimedKvConfigCommand, mQClientAPIImpl);
        initTimedKV(mQClientAPIImpl);
        initPrintStream();

        timedConfigHashMap.put(TIMED_NAMESPACE, new HashMap<String, TimedConfig>());
        timedConfigHashMap.get(TIMED_NAMESPACE).put("del1", new TimedConfig("value1", System.currentTimeMillis() + 1000));
        timedConfigHashMap.get(TIMED_NAMESPACE).put("del3", new TimedConfig("value3", System.currentTimeMillis() + 300));
    }

    @After
    public void shutdown() {
        shutdownPrintStream();
    }

    @Test
    public void testExec() throws SubCommandException {
        long timeout = System.currentTimeMillis() + 1000000;
        SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd#HH:mm");
        String format = simpleDateFormat.format(new Date(timeout));
        Assert.assertTrue(timedConfigHashMap.get(TIMED_NAMESPACE).get("del1").getValue().equals("value1"));
        CommandLine commandLine = parseCommandLine(updateTimedKvConfigCommand, new String[]{"-s", TIMED_NAMESPACE, "-k",
            "del1", "-v", "value3333", "-t", format});
        updateTimedKvConfigCommand.execute(commandLine, null, null);
        String printContent = getPrintContent();
        System.err.println(printContent);
        Assert.assertTrue(printContent.contains("success"));
        Assert.assertTrue(timedConfigHashMap.get(TIMED_NAMESPACE).get("del1").getValue().equals("value3333"));
        String format1 = simpleDateFormat.format(new Date(timedConfigHashMap.get(TIMED_NAMESPACE).get("del1").getTimeout()));
        Assert.assertTrue(format.equals(format1));
    }
}
