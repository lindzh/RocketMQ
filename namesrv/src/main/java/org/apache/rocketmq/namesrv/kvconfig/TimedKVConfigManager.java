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
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.common.namesrv.NamesrvUtil;
import org.apache.rocketmq.common.protocol.body.TimedKVTable;
import org.apache.rocketmq.namesrv.NamesrvController;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class TimedKVConfigManager {
    private static final Logger log = LoggerFactory.getLogger(LoggerName.NAMESRV_LOGGER_NAME);

    private final NamesrvController namesrvController;

    private final ReadWriteLock lock = new ReentrantReadWriteLock();

    private final HashMap<String/* Namespace */, HashMap<String/* Key */, TimedConfig/* Value */>> timedConfigTable =
        new HashMap<String, HashMap<String, TimedConfig>>();

    public TimedKVConfigManager(NamesrvController namesrvController) {
        this.namesrvController = namesrvController;
    }

    /**
     * add enable switch in default kv config for namespace
     */
    public boolean isTimeNamespaceEnabled(String namespace) {
        String enabled = this.namesrvController.getKvConfigManager().getKVConfig(NamesrvUtil.NAMESPACE_TIMED_KV_NAMESPACE_ENABLE, namespace);
        if (enabled != null && enabled.equalsIgnoreCase("false")) {
            return false;
        }
        return true;
    }

    public void load() {
        String content = null;
        try {
            content = MixAll.file2String(this.namesrvController.getNamesrvConfig().getTimedKVConfigPath());
        } catch (IOException e) {
            log.warn("Load Timed KV config table exception", e);
        }
        if (content != null) {
            TimedKVConfigSerializeWrapper kvConfigSerializeWrapper =
                TimedKVConfigSerializeWrapper.fromJson(content, TimedKVConfigSerializeWrapper.class);
            if (null != kvConfigSerializeWrapper) {
                this.timedConfigTable.putAll(kvConfigSerializeWrapper.getConfigTable());
                log.info("load Timed KV config table OK");
            }
        }
    }

    public void putTimedKVConfig(final String namespace, final String key, final TimedConfig value) {
        if (!isTimeNamespaceEnabled(namespace)) {
            log.info("TimedKVConfig namespace:{} not allowed for update");
        }
        try {
            this.lock.writeLock().lockInterruptibly();
            try {
                HashMap<String, TimedConfig> kvTable = this.timedConfigTable.get(namespace);
                if (null == kvTable) {
                    kvTable = new HashMap<String, TimedConfig>();
                    this.timedConfigTable.put(namespace, kvTable);
                    log.info("putTimedKVConfig create new Namespace {}", namespace);
                }

                final TimedConfig prev = kvTable.put(key, value);
                if (null != prev) {
                    log.info("putTimedKVConfig update config item, Namespace: {} Key: {} Value: {}",
                        namespace, key, value);
                } else {
                    log.info("putTimedKVConfig create new config item, Namespace: {} Key: {} Value: {}",
                        namespace, key, value);
                }
            } finally {
                this.lock.writeLock().unlock();
            }
        } catch (InterruptedException e) {
            log.error("putTimedKVConfig InterruptedException", e);
        }

        this.persist();
    }

    public void persist() {
        try {
            this.lock.readLock().lockInterruptibly();
            try {
                TimedKVConfigSerializeWrapper kvConfigSerializeWrapper = new TimedKVConfigSerializeWrapper();
                kvConfigSerializeWrapper.setConfigTable(this.timedConfigTable);

                String content = kvConfigSerializeWrapper.toJson();

                if (null != content) {
                    MixAll.string2File(content, this.namesrvController.getNamesrvConfig().getTimedKVConfigPath());
                }
            } catch (IOException e) {
                log.error("persist Timed kvconfig Exception, "
                    + this.namesrvController.getNamesrvConfig().getKvConfigPath(), e);
            } finally {
                this.lock.readLock().unlock();
            }
        } catch (InterruptedException e) {
            log.error("persist Timed kvconfig InterruptedException", e);
        }

    }

    public void deleteTimedKVConfig(final String namespace, final String key) {
        if (!isTimeNamespaceEnabled(namespace)) {
            log.info("TimedKVConfig namespace:{} not allowed for update");
        }
        try {
            this.lock.writeLock().lockInterruptibly();
            try {
                HashMap<String, TimedConfig> kvTable = this.timedConfigTable.get(namespace);
                if (null != kvTable) {
                    TimedConfig value = kvTable.remove(key);
                    log.info("deleteTimedKVConfig delete a config item, Namespace: {} Key: {} Value: {}",
                        namespace, key, value);
                }
            } finally {
                this.lock.writeLock().unlock();
            }
        } catch (InterruptedException e) {
            log.error("deleteTimedKVConfig InterruptedException", e);
        }

        this.persist();
    }

    public byte[] getTimedKVListByNamespace(final String namespace) {
        try {
            this.lock.readLock().lockInterruptibly();
            try {
                HashMap<String, TimedConfig> kvTable = this.timedConfigTable.get(namespace);
                if (null != kvTable) {
                    TimedKVTable table = new TimedKVTable();
                    table.setTable(kvTable);
                    return table.encode();
                }
            } finally {
                this.lock.readLock().unlock();
            }
        } catch (InterruptedException e) {
            log.error("getTimedKVListByNamespace InterruptedException", e);
        }

        return null;
    }

    public TimedConfig getTimedKVConfig(final String namespace, final String key) {
        if (!isTimeNamespaceEnabled(namespace)) {
            log.info("TimedKVConfig namespace:{} not allowed for read");
        }
        try {
            this.lock.readLock().lockInterruptibly();
            try {
                HashMap<String, TimedConfig> kvTable = this.timedConfigTable.get(namespace);
                if (null != kvTable) {
                    TimedConfig timedConfig = kvTable.get(key);
                    if (timedConfig != null && !isTimeout(timedConfig)) {
                        return timedConfig;
                    }
                }
            } finally {
                this.lock.readLock().unlock();
            }
        } catch (InterruptedException e) {
            log.error("getTimedKVConfig InterruptedException", e);
        }

        return null;
    }

    public void printAllPeriodically() {
        try {
            this.lock.readLock().lockInterruptibly();
            try {
                log.info("--------------------------TimedKV------------------------------");

                {
                    log.info("timedConfigTable SIZE: {}", this.timedConfigTable.size());
                    Iterator<Entry<String, HashMap<String, TimedConfig>>> it =
                        this.timedConfigTable.entrySet().iterator();
                    while (it.hasNext()) {
                        Entry<String, HashMap<String, TimedConfig>> next = it.next();
                        Iterator<Entry<String, TimedConfig>> itSub = next.getValue().entrySet().iterator();
                        while (itSub.hasNext()) {
                            Entry<String, TimedConfig> nextSub = itSub.next();
                            log.info("timedConfigTable NS: {} Key: {} Value: {}", next.getKey(), nextSub.getKey(),
                                nextSub.getValue());
                        }
                    }
                }
            } finally {
                this.lock.readLock().unlock();
            }
        } catch (InterruptedException e) {
            log.error("printTimedAllPeriodically InterruptedException", e);
        }
    }

    private boolean isTimeout(TimedConfig config) {
        long now = System.currentTimeMillis();
        return config == null || now > config.getTimeout();
    }

    public void scanTimeoutKVConfig() {
        boolean updated = false;
        try {
            this.lock.writeLock().lockInterruptibly();
            try {
                Set<Entry<String, HashMap<String, TimedConfig>>> entries = timedConfigTable.entrySet();
                for (Entry<String, HashMap<String, TimedConfig>> entry : entries) {
                    HashMap<String, TimedConfig> configs = entry.getValue();
                    Set<Entry<String, TimedConfig>> entries1 = configs.entrySet();
                    for (Entry<String, TimedConfig> timedConfigEntry : entries1) {
                        if (timedConfigEntry.getValue() != null && isTimeout(timedConfigEntry.getValue())) {
                            timedConfigTable.get(entry.getKey()).remove(timedConfigEntry.getKey());
                            updated = true;
                        }
                    }
                }
            } finally {
                this.lock.writeLock().unlock();
            }
            if (updated) {
                this.persist();
            }
        } catch (InterruptedException e) {
            log.error("deleteTimedKVConfig InterruptedException", e);
        }
    }
}
