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

import java.util.HashMap;
import java.util.Map;

public class DowngradeConfig {

    private String topic;

    private boolean downgradeEnable;

    private long downTimeout;

    private Map<String, Long> hostDownTimeout;

    public DowngradeConfig() {
        this.downTimeout = -1;
        this.downgradeEnable = true;
    }

    public DowngradeConfig(String topic, long downTimeout) {
        this();
        this.topic = topic;
        this.downTimeout = downTimeout;
    }

    public DowngradeConfig(String topic, String host, long downTimeout) {
        this();
        this.topic = topic;
        hostDownTimeout = new HashMap<String, Long>();
        if (host != null && downTimeout > System.currentTimeMillis()) {
            hostDownTimeout.put(host, downTimeout);
        }
    }

    public String getTopic() {
        return topic;
    }

    public void setTopic(String topic) {
        this.topic = topic;
    }

    public boolean isDowngradeEnable() {
        return downgradeEnable;
    }

    public void setDowngradeEnable(boolean downgradeEnable) {
        this.downgradeEnable = downgradeEnable;
    }

    public long getDownTimeout() {
        return downTimeout;
    }

    public void setDownTimeout(long downTimeout) {
        this.downTimeout = downTimeout;
    }

    public Map<String, Long> getHostDownTimeout() {
        return hostDownTimeout;
    }

    public void setHostDownTimeout(Map<String, Long> hostDownTimeout) {
        this.hostDownTimeout = hostDownTimeout;
    }

}
