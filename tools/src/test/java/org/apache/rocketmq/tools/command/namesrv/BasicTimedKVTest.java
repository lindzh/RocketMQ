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
import org.apache.commons.cli.Options;
import org.apache.commons.cli.PosixParser;
import org.apache.rocketmq.client.ClientConfig;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.impl.MQClientAPIImpl;
import org.apache.rocketmq.client.impl.MQClientManager;
import org.apache.rocketmq.client.impl.factory.MQClientInstance;
import org.apache.rocketmq.common.TimedConfig;
import org.apache.rocketmq.common.protocol.body.TimedKVTable;
import org.apache.rocketmq.remoting.exception.RemotingException;
import org.apache.rocketmq.srvutil.ServerUtil;
import org.apache.rocketmq.tools.admin.DefaultMQAdminExt;
import org.apache.rocketmq.tools.admin.DefaultMQAdminExtImpl;
import org.apache.rocketmq.tools.command.AbstractCommand;
import org.apache.rocketmq.tools.command.SubCommand;
import org.mockito.Mockito;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.lang.reflect.Field;
import java.util.HashMap;
import java.util.Map;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;

public class BasicTimedKVTest {

    protected static HashMap<String, Map<String, TimedConfig>> timedConfigHashMap = new HashMap<>();

    private static DefaultMQAdminExt defaultMQAdminExt;
    private static DefaultMQAdminExtImpl defaultMQAdminExtImpl;
    private static MQClientInstance mqClientInstance = MQClientManager.getInstance().getAndCreateMQClientInstance(new ClientConfig());
    private static MQClientAPIImpl mQClientAPIImpl;

    private static ByteArrayOutputStream bos;

    private static PrintStream printStream;

    public static void initPrintStream(){
        printStream = System.out;
        bos = new ByteArrayOutputStream();
        System.setOut(new PrintStream(bos));
    }

    public static void shutdownPrintStream(){
        System.setOut(printStream);
    }

    public static String getPrintContent(){
        return bos.toString();
    }

    public static MQClientAPIImpl getmQClientAPIImpl(){
        return mQClientAPIImpl;
    }

    public static void initMQAdmin() throws IllegalAccessException, NoSuchFieldException {
        mQClientAPIImpl = mock(MQClientAPIImpl.class);
        defaultMQAdminExt = new DefaultMQAdminExt();
        defaultMQAdminExtImpl = new DefaultMQAdminExtImpl(defaultMQAdminExt, 1000);

        Field field = MQClientInstance.class.getDeclaredField("mQClientAPIImpl");
        field.setAccessible(true);
        field.set(mqClientInstance, mQClientAPIImpl);

        field = DefaultMQAdminExtImpl.class.getDeclaredField("mqClientInstance");
        field.setAccessible(true);
        field.set(defaultMQAdminExtImpl, mqClientInstance);

        field = DefaultMQAdminExt.class.getDeclaredField("defaultMQAdminExtImpl");
        field.setAccessible(true);
        field.set(defaultMQAdminExt, defaultMQAdminExtImpl);
    }

    protected static DefaultMQAdminExt getDefaultMQAdminExt() {
        return defaultMQAdminExt;
    }

    public static void initTimedKV(MQClientAPIImpl mQClientAPI) throws RemotingException, MQClientException, InterruptedException {
        if(mQClientAPI==null){
            mQClientAPI = mQClientAPIImpl;
        }
        doAnswer(new Answer() {
            @Override
            public Object answer(InvocationOnMock invocationOnMock) throws Throwable {
                Object[] arguments = invocationOnMock.getArguments();
                String namespace = (String) arguments[0];
                String key = (String) arguments[1];
                TimedConfig config = (TimedConfig) arguments[2];
                Map<String, TimedConfig> timedConfigMap = timedConfigHashMap.get(namespace);
                if (timedConfigMap == null) {
                    timedConfigMap = new HashMap<String, TimedConfig>();
                    timedConfigHashMap.put(namespace, timedConfigMap);
                }
                timedConfigMap.put(key, config);
                return null;
            }
        }).when(mQClientAPI).putTimedKVConfigValue(anyString(), anyString(), any(TimedConfig.class), anyLong());

        doAnswer(new Answer() {
            @Override
            public Object answer(InvocationOnMock invocationOnMock) throws Throwable {
                Object[] arguments = invocationOnMock.getArguments();
                String namespace = (String) arguments[0];
                String key = (String) arguments[1];
                Map<String, TimedConfig> timedConfigMap = timedConfigHashMap.get(namespace);
                if (timedConfigMap != null) {
                    TimedConfig timedConfig = timedConfigMap.get(key);
                    if (timedConfig != null && timedConfig.getTimeout() > System.currentTimeMillis()) {
                        return timedConfig;
                    }
                }
                return null;
            }
        }).when(mQClientAPI).getTimedKVConfigValue(anyString(), anyString(), anyLong());

        doAnswer(new Answer() {
            @Override
            public Object answer(InvocationOnMock invocationOnMock) throws Throwable {
                Object[] arguments = invocationOnMock.getArguments();
                String namespace = (String) arguments[0];
                String key = (String) arguments[1];
                Map<String, TimedConfig> timedConfigMap = timedConfigHashMap.get(namespace);
                if (timedConfigMap != null) {
                    timedConfigMap.remove(key);
                }
                return null;
            }
        }).when(mQClientAPI).deleteTimedKVConfigValue(anyString(), anyString(), anyLong());

        doAnswer(new Answer() {
            @Override
            public Object answer(InvocationOnMock invocationOnMock) throws Throwable {
                Object[] arguments = invocationOnMock.getArguments();
                String namespace = (String) arguments[0];
                TimedKVTable timedKVTable = new TimedKVTable();
                timedKVTable.setTable(new HashMap<String, TimedConfig>());
                Map<String, TimedConfig> timedConfigMap = timedConfigHashMap.get(namespace);
                if (timedConfigMap != null) {
                    timedKVTable.getTable().putAll(timedConfigMap);
                }
                return timedKVTable;
            }
        }).when(mQClientAPI).getTimedKVListByNamespace(anyString(), anyLong());
    }

    protected static CommandLine parseCommandLine(SubCommand cmd, String[] args) {
        Options options = ServerUtil.buildCommandlineOptions(new Options());
        String[] subargs = args;
        return ServerUtil.parseCmdLine("mqadmin " + cmd.commandName(), subargs, cmd.buildCommandlineOptions(options), new PosixParser());
    }

    public static void init4TimedKV(AbstractCommand abstractCommand,MQClientAPIImpl mQClientAPIImpl) throws IllegalAccessException, NoSuchFieldException {
        MQClientInstance mqClientInstance = mock(MQClientInstance.class);
        Mockito.when(mqClientInstance.getMQClientAPIImpl()).thenReturn(mQClientAPIImpl);

        DefaultMQAdminExt defaultMQAdminExt = new DefaultMQAdminExt();
        DefaultMQAdminExtImpl defaultMQAdminExtImpl = new DefaultMQAdminExtImpl(defaultMQAdminExt, 1000);

        Field field = DefaultMQAdminExtImpl.class.getDeclaredField("mqClientInstance");
        field.setAccessible(true);
        field.set(defaultMQAdminExtImpl, mqClientInstance);

        field = DefaultMQAdminExt.class.getDeclaredField("defaultMQAdminExtImpl");
        field.setAccessible(true);
        field.set(defaultMQAdminExt, defaultMQAdminExtImpl);

        Field mqAdminExtField = AbstractCommand.class.getDeclaredField("defaultMQAdminExt");
        mqAdminExtField.setAccessible(true);
        mqAdminExtField.set(abstractCommand,defaultMQAdminExt);
    }

}
