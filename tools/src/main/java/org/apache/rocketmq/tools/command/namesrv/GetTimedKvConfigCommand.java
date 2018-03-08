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
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.rocketmq.common.TimedConfig;
import org.apache.rocketmq.common.protocol.body.TimedKVTable;
import org.apache.rocketmq.remoting.RPCHook;
import org.apache.rocketmq.tools.admin.DefaultMQAdminExt;
import org.apache.rocketmq.tools.command.AbstractCommand;
import org.apache.rocketmq.tools.command.SubCommand;
import org.apache.rocketmq.tools.command.SubCommandException;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

public class GetTimedKvConfigCommand extends AbstractCommand implements SubCommand {

    @Override
    public String commandName() {
        return "getTimedKvConfig";
    }

    @Override
    public String commandDesc() {
        return "get Timed KV config.";
    }

    @Override
    public Options buildCommandlineOptions(Options options) {
        Option opt = new Option("s", "namespace", true, "get the timed namespace");
        opt.setRequired(true);
        options.addOption(opt);

        opt = new Option("k", "key", true, "get the timed key name");
        opt.setRequired(false);
        options.addOption(opt);
        return options;
    }

    @Override
    public void execute(CommandLine commandLine, Options options, RPCHook rpcHook) throws SubCommandException {
        DefaultMQAdminExt defaultMQAdminExt = getMQAdminExt(rpcHook);
        defaultMQAdminExt.setInstanceName(Long.toString(System.currentTimeMillis()));
        try {
            String namespace = commandLine.getOptionValue('s').trim();

            SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd#HH:mm");
            if (commandLine.hasOption('k')) {
                String key = commandLine.getOptionValue('k').trim();
                TimedConfig timedKVConfig = defaultMQAdminExt.getTimedKVConfig(namespace, key);
                if (timedKVConfig != null) {
                    String format = simpleDateFormat.format(new Date(timedKVConfig.getTimeout()));
                    System.out.printf("config timeout :" + format + "%n");
                    System.out.printf("config value :" + timedKVConfig.getValue() + "%n");
                } else {
                    System.out.printf("config is empty or expired.%n");
                }
            } else {
                TimedKVTable timedKVTable = defaultMQAdminExt.getTimedKVListByNamespace(namespace);
                HashMap<String, TimedConfig> table = timedKVTable.getTable();
                if (table == null || table.size() < 1) {
                    System.out.printf("config is empty or expired.%n");
                } else {
                    Set<Map.Entry<String, TimedConfig>> entries = table.entrySet();
                    for (Map.Entry<String, TimedConfig> entry : entries) {
                        String timeformat = simpleDateFormat.format(new Date(entry.getValue().getTimeout()));
                        System.out.printf("key:" + entry.getKey() + " timeout:" + timeformat + " value:" + entry.getValue().getValue() + "%n");
                    }
                }
            }
        } catch (Exception e) {
            throw new SubCommandException(this.getClass().getSimpleName() + " command failed", e);
        } finally {
            defaultMQAdminExt.shutdown();
        }
    }
}
