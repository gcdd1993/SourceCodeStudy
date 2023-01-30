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

package org.apache.rocketmq.broker;

import org.apache.commons.lang3.time.DateUtils;
import org.apache.rocketmq.broker.latency.FutureTaskExt;
import org.apache.rocketmq.common.BrokerConfig;
import org.apache.rocketmq.common.MQVersion;
import org.apache.rocketmq.common.UtilAll;
import org.apache.rocketmq.remoting.netty.NettyClientConfig;
import org.apache.rocketmq.remoting.netty.NettyServerConfig;
import org.apache.rocketmq.remoting.netty.RequestTask;
import org.apache.rocketmq.remoting.protocol.RemotingCommand;
import org.apache.rocketmq.store.config.FlushDiskType;
import org.apache.rocketmq.store.config.MessageStoreConfig;
import org.junit.After;
import org.junit.Test;

import java.io.File;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

public class BrokerControllerTest {

    public static void main(String[] args) throws Exception {
        // 设置版本号
        System.setProperty(RemotingCommand.REMOTING_VERSION_KEY, Integer.toString(MQVersion.CURRENT_VERSION));
        // NettyServerConfig 配置
        final NettyServerConfig nettyServerConfig = new NettyServerConfig();
        nettyServerConfig.setListenPort(10911);
        // BrokerConfig 配置
        final BrokerConfig brokerConfig = new BrokerConfig();
        brokerConfig.setBrokerName("broker-a");
        brokerConfig.setNamesrvAddr("127.0.0.1:9876");
        // MessageStoreConfig 配置
        final MessageStoreConfig messageStoreConfig = new MessageStoreConfig();
        messageStoreConfig.setDeleteWhen("04");
        messageStoreConfig.setFileReservedTime(48);
        messageStoreConfig.setFlushDiskType(FlushDiskType.ASYNC_FLUSH);
        messageStoreConfig.setDuplicationEnable(false);

//        BrokerPathConfigHelper.setBrokerConfigPath("/Users/yunai/百度云同步盘/开发/Javascript/Story/incubator-rocketmq/conf/broker.conf");
        // 创建 BrokerController 对象，并启动
        BrokerController brokerController = new BrokerController(//
                brokerConfig, //
                nettyServerConfig, //
                new NettyClientConfig(), //
                messageStoreConfig);
        brokerController.initialize();
        brokerController.start();
        // 睡觉，就不起来
        System.out.println("broker-a started.");
        Thread.sleep(DateUtils.MILLIS_PER_DAY);
    }

    @Test
    public void testBrokerRestart() throws Exception {
        BrokerController brokerController = new BrokerController(
                new BrokerConfig(),
                new NettyServerConfig(),
                new NettyClientConfig(),
                new MessageStoreConfig());
        assertThat(brokerController.initialize());
        brokerController.start();
        brokerController.shutdown();
    }

    @After
    public void destroy() {
        UtilAll.deleteFile(new File(new MessageStoreConfig().getStorePathRootDir()));
    }

    @Test
    public void testHeadSlowTimeMills() throws Exception {
        BrokerController brokerController = new BrokerController(
                new BrokerConfig(),
                new NettyServerConfig(),
                new NettyClientConfig(),
                new MessageStoreConfig());
        brokerController.initialize();
        BlockingQueue<Runnable> queue = new LinkedBlockingQueue<>();

        //create task is not instance of FutureTaskExt;
        Runnable runnable = new Runnable() {
            @Override
            public void run() {

            }
        };

        RequestTask requestTask = new RequestTask(runnable, null, null);
        // the requestTask is not the head of queue;
        queue.add(new FutureTaskExt<>(requestTask, null));

        long headSlowTimeMills = 100;
        TimeUnit.MILLISECONDS.sleep(headSlowTimeMills);
        assertThat(brokerController.headSlowTimeMills(queue)).isGreaterThanOrEqualTo(headSlowTimeMills);
    }

}
