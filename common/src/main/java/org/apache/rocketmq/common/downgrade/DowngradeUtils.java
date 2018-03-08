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

import com.alibaba.fastjson.JSON;
import org.apache.rocketmq.common.TimedConfig;
import org.apache.rocketmq.common.constant.GroupType;

import java.util.Map;
import java.util.Set;

public class DowngradeUtils {

    public static DowngradeConfig fromTimedConfig(TimedConfig timedConfig) {
        String value = timedConfig.getValue();
        return JSON.parseObject(value, DowngradeConfig.class);
    }

    public static TimedConfig toTimedConfig(DowngradeConfig downgradeConfig) {
        if (downgradeConfig == null || !downgradeConfig.isDowngradeEnable()) {
            return null;
        }
        long maxTimeout = getMaxTimeout(downgradeConfig);
        if (maxTimeout < System.currentTimeMillis()) {
            return null;
        }
        return new TimedConfig(JSON.toJSONString(downgradeConfig), maxTimeout);
    }

    public static long getMaxTimeout(DowngradeConfig downgradeConfig) {
        long timeout = -1;
        if (!downgradeConfig.isDowngradeEnable()) {
            return timeout;
        }
        timeout = timeout > downgradeConfig.getDownTimeout() ? timeout : downgradeConfig.getDownTimeout();
        if (downgradeConfig.getHostDownTimeout() != null) {
            Set<Map.Entry<String, Long>> entries = downgradeConfig.getHostDownTimeout().entrySet();
            for (Map.Entry<String, Long> entry : entries) {
                Long value = entry.getValue();
                if (value != null) {
                    timeout = timeout > value.longValue() ? timeout : value.longValue();
                }
            }
        }
        return timeout;
    }

    public static String genDowngradeKey(GroupType groupType, String group, String topic) {
        return "%Downgrade%" + groupType.name() + "%" + group + "@" + topic;
    }
}
