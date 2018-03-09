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

package org.apache.rocketmq.test.client.downgrade;

import org.apache.rocketmq.client.consumer.DefaultMQPullConsumer;
import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.apache.rocketmq.client.consumer.MessageQueueListener;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyContext;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyStatus;
import org.apache.rocketmq.client.consumer.listener.MessageListenerConcurrently;
import org.apache.rocketmq.client.exception.MQBrokerException;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.impl.factory.MQClientInstance;
import org.apache.rocketmq.client.impl.producer.DefaultMQProducerImpl;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.common.TimedConfig;
import org.apache.rocketmq.common.constant.GroupType;
import org.apache.rocketmq.common.downgrade.DowngradeConfig;
import org.apache.rocketmq.common.downgrade.DowngradeUtils;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.common.message.MessageQueue;
import org.apache.rocketmq.common.namesrv.NamesrvUtil;
import org.apache.rocketmq.common.protocol.heartbeat.MessageModel;
import org.apache.rocketmq.remoting.exception.RemotingException;
import org.apache.rocketmq.test.base.BaseConf;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.lang.reflect.Field;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;

public class DowngradeTest extends BaseConf {

    private DefaultMQProducer producer;

    private DefaultMQPullConsumer pullConsumer;

    private DefaultMQPushConsumer pushConsumer;

    private Set<MessageQueue> mqDivided;

    private boolean pullUpdated = false;

    private AtomicBoolean received = new AtomicBoolean(false);

    @Before
    public void init() throws MQClientException {
        producer =  new DefaultMQProducer("downgradeProducer");
        producer.setUpdateDowngradeConfigInterval(500);
        producer.setNamesrvAddr(nsAddr);
        producer.setInstanceName("producer");
        producer.start();

        initTopic();



        pullConsumer = new DefaultMQPullConsumer("downgradePullConsumer");
        pullConsumer.setNamesrvAddr(nsAddr);
        pullConsumer.setInstanceName("pullConsumer");
        pullConsumer.setMessageModel(MessageModel.CLUSTERING);
        pullConsumer.setMessageQueueListener(new MessageQueueListener() {
            @Override
            public void messageQueueChanged(String topic, Set<MessageQueue> mqAll, Set<MessageQueue> mqDivided) {
                System.out.println("-------messageQueueChanged-------");
                if(DowngradeTest.this.mqDivided!=mqDivided){
                    if(mqDivided==null){
                        DowngradeTest.this.mqDivided = mqDivided;
                    }else{
                        if(DowngradeTest.this.mqDivided!=null&&DowngradeTest.this.mqDivided.size()>0&&mqDivided==null||mqDivided.size()==0){
                            pullUpdated = true;
                        }
                    }
                }
            }
        });
        pullConsumer.setUpdateDowngradeConfigInterval(500);
        pullConsumer.start();
        pullConsumer.fetchSubscribeMessageQueues(downgradeTopic);

        pushConsumer = new DefaultMQPushConsumer("downgradePushConsumer");
        pushConsumer.setNamesrvAddr(nsAddr);
        pushConsumer.setInstanceName("pushConsumer");
        pushConsumer.setMessageModel(MessageModel.CLUSTERING);
        pushConsumer.subscribe(downgradeTopic,"*");
        pushConsumer.registerMessageListener(new MessageListenerConcurrently() {
            @Override
            public ConsumeConcurrentlyStatus consumeMessage(List<MessageExt> msgs, ConsumeConcurrentlyContext context) {
                received.set(true);
                return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
            }
        });
        pushConsumer.setUpdateDowngradeConfigInterval(500);
        pushConsumer.start();
    }

    private ConcurrentHashMap<String,Map<String,DowngradeConfig>> getProducerDowngradeCache(DefaultMQProducer producer) throws NoSuchFieldException, IllegalAccessException {
        DefaultMQProducerImpl defaultMQProducerImpl = producer.getDefaultMQProducerImpl();
        Field mQClientFactoryField = DefaultMQProducerImpl.class.getDeclaredField("mQClientFactory");
        mQClientFactoryField.setAccessible(true);
        Object mQClientFactory = mQClientFactoryField.get(producer);
        Field downgradeConfigTableField = MQClientInstance.class.getDeclaredField("downgradeConfigTable");
        downgradeConfigTableField.setAccessible(true);
        return (ConcurrentHashMap<String,Map<String,DowngradeConfig>>)downgradeConfigTableField.get(mQClientFactory);
    }

    @Test
    public void testSendAndRecieve() throws InterruptedException, RemotingException, MQClientException, MQBrokerException {
        producer.send(new Message(downgradeTopic,("message test "+System.currentTimeMillis()).getBytes()));
        producer.send(new Message(downgradeTopic,("message test "+System.currentTimeMillis()).getBytes()));
        producer.send(new Message(downgradeTopic,("message test "+System.currentTimeMillis()).getBytes()));
        producer.send(new Message(downgradeTopic,("message test "+System.currentTimeMillis()).getBytes()));

        enableDowngrade(GroupType.PRODUCER,producer.getProducerGroup());
        Thread.sleep(1000);

        TimedConfig timedKVConfig = namesrvController.getTimedKVConfigManager().getTimedKVConfig(NamesrvUtil.TIMED_NAMESPACE_CLIENT_DOWNGRADE_CONFIG,
            DowngradeUtils.genDowngradeKey(GroupType.PRODUCER, producer.getProducerGroup()));
        Assert.assertTrue(timedKVConfig!=null);

        boolean hasException = false;
        try {
            producer.send(new Message(downgradeTopic,("message test "+System.currentTimeMillis()).getBytes()));
        }catch (MQClientException e){
            Assert.assertTrue(e.getErrorMessage().contains("not allowed for send now because of DowngradeConfig"));
            hasException = true;
        }
        Assert.assertTrue(hasException);
//        Assert.assertTrue(mqDivided.size()>0);
        Assert.assertTrue(received.get());

        disableDowngrade(GroupType.PRODUCER,producer.getProducerGroup());

        enableDowngrade(GroupType.CONSUMER,pullConsumer.getConsumerGroup());

        enableDowngrade(GroupType.CONSUMER,pushConsumer.getConsumerGroup());

        timedKVConfig = namesrvController.getTimedKVConfigManager().getTimedKVConfig(NamesrvUtil.TIMED_NAMESPACE_CLIENT_DOWNGRADE_CONFIG,
            DowngradeUtils.genDowngradeKey(GroupType.PRODUCER, producer.getProducerGroup()));
        Assert.assertTrue(timedKVConfig==null);

        Thread.sleep(3000);

        received.set(false);
//        Assert.assertTrue(mqDivided.size()==0);

        producer.send(new Message(downgradeTopic,("message test "+System.currentTimeMillis()).getBytes()));
        producer.send(new Message(downgradeTopic,("message test "+System.currentTimeMillis()).getBytes()));
        producer.send(new Message(downgradeTopic,("message test "+System.currentTimeMillis()).getBytes()));

//        Assert.assertTrue(mqDivided.size()==0);
        Assert.assertTrue(!received.get());

        disableDowngrade(GroupType.CONSUMER,pullConsumer.getConsumerGroup());
        disableDowngrade(GroupType.CONSUMER,pushConsumer.getConsumerGroup());
        Thread.sleep(2000);

//        Assert.assertTrue(mqDivided.size()>0);
        Assert.assertTrue(received.get());
    }

    @After
    public void shutdown1(){
        if(producer!=null){
            producer.shutdown();
        }
        if(pullConsumer!=null){
            pullConsumer.shutdown();
        }
        if(pushConsumer!=null){
            pullConsumer.shutdown();
        }
    }

}
