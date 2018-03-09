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
import org.apache.rocketmq.common.constant.GroupType;
import org.apache.rocketmq.common.downgrade.DowngradeConfig;
import org.apache.rocketmq.remoting.RPCHook;
import org.apache.rocketmq.tools.admin.DefaultMQAdminExt;
import org.apache.rocketmq.tools.command.AbstractCommand;
import org.apache.rocketmq.tools.command.SubCommand;
import org.apache.rocketmq.tools.command.SubCommandException;

import java.text.SimpleDateFormat;
import java.util.HashMap;
import java.util.Map;

public class UpdateClientDowngradeConfigCommand extends AbstractCommand implements SubCommand {
    @Override
    public String commandName() {
        return "updateClientDowngradeConfig";
    }

    @Override
    public String commandDesc() {
        return "Create or update client downgrade config.";
    }

    @Override
    public Options buildCommandlineOptions(Options options) {
        Option opt = new Option("p", "producer", true, "set the producer downgrade group");
        opt.setRequired(false);
        options.addOption(opt);

        opt = new Option("c", "consumer", true, "set the consumer downgrade group");
        opt.setRequired(false);
        options.addOption(opt);

        opt = new Option("e", "enable", true, "set the downgrade group enable");
        opt.setRequired(false);
        options.addOption(opt);

        opt = new Option("t", "topic", true, "set topic to downgrade");
        opt.setRequired(true);
        options.addOption(opt);

        opt = new Option("o", "timeout", true, "set downgrade timeout in format yyyy-MM-dd#HH:mm");
        opt.setRequired(true);
        options.addOption(opt);

        opt = new Option("h", "host", true, "set downgrade host");
        opt.setRequired(false);
        options.addOption(opt);

        return options;
    }

    @Override
    public void execute(CommandLine commandLine, Options options, RPCHook rpcHook) throws SubCommandException {
        DefaultMQAdminExt defaultMQAdminExt = getMQAdminExt(rpcHook);
        defaultMQAdminExt.setInstanceName(Long.toString(System.currentTimeMillis()));

        try {
            String group;
            GroupType groupType;

            if (commandLine.hasOption('p')) {
                group = commandLine.getOptionValue('p').trim();
                groupType = GroupType.PRODUCER;
            } else if (commandLine.hasOption('c')) {
                group = commandLine.getOptionValue('c').trim();
                groupType = GroupType.CONSUMER;
            } else {
                throw new RuntimeException("Please input producer group or consumer group");
            }

            String timeout = commandLine.getOptionValue('o').trim();
            SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd#HH:mm");
            long timeoutValue = dateFormat.parse(timeout).getTime();

            String topic = commandLine.getOptionValue('t').trim();

            Map<String, DowngradeConfig> downgradeConfigMap = defaultMQAdminExt.getDowngradeConfig(groupType, group);
            if (downgradeConfigMap == null) {
                downgradeConfigMap = new HashMap<>();
            }
            DowngradeConfig downgradeConfig = downgradeConfigMap.get(topic);
            if (downgradeConfig == null) {
                downgradeConfig = new DowngradeConfig();
                downgradeConfigMap.put(topic, downgradeConfig);
                downgradeConfig.setDowngradeEnable(true);
            }
            if (downgradeConfig.getHostDownTimeout() == null) {
                downgradeConfig.setHostDownTimeout(new HashMap<String, Long>());
            }
            if (commandLine.hasOption('e')) {
                String enable = commandLine.getOptionValue('e').trim();
                downgradeConfig.setDowngradeEnable(Boolean.parseBoolean(enable));
            }

            if (commandLine.hasOption('h')) {
                String host = commandLine.getOptionValue('h').trim();
                downgradeConfig.getHostDownTimeout().put(host, timeoutValue);
            } else {
                downgradeConfig.setDownTimeout(timeoutValue);
            }

            defaultMQAdminExt.updateDowngradeConfig(groupType, group, downgradeConfigMap);
            System.out.printf("create or update downgrade config to namespace success.%n");
        } catch (Exception e) {
            throw new SubCommandException(this.getClass().getSimpleName() + " command failed", e);
        } finally {
            defaultMQAdminExt.shutdown();
        }
    }
}
