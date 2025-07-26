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

package org.apache.rocketmq.samples.springboot;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.List;

import jakarta.annotation.Resource;
import org.apache.rocketmq.client.apis.message.MessageId;
import org.apache.rocketmq.client.apis.message.MessageView;
import org.apache.rocketmq.client.core.RocketMQClientTemplate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication
public class V5SimpleConsumerConsumerApplication implements CommandLineRunner {
    private static final Logger log = LoggerFactory.getLogger(V5SimpleConsumerConsumerApplication.class);


    @Resource
    private RocketMQClientTemplate rocketMQClientTemplate;

    public static void main(String[] args) {
        SpringApplication.run(V5SimpleConsumerConsumerApplication.class, args);
    }

    @Override
    public void run(String... args) throws Exception {
        for (int i = 0; i < 10; i++) {
            List<MessageView> messageList = rocketMQClientTemplate.receive(10, Duration.ofSeconds(60));
            log.info("Received1 {} message(s)", messageList.size());
            for (MessageView message : messageList) {
                log.info("receive message, topic:{} messageId:{}", message.getTopic(), message.getMessageId());
                String body =  StandardCharsets.UTF_8.decode(message.getBody().duplicate()).toString();
                log.info("Sir，我正在消费消息：{}", body);
                final MessageId messageId = message.getMessageId();
                try {
                    rocketMQClientTemplate.ack(message);
                    log.info("Message is acknowledged successfully, messageId={}", messageId);
                } catch (Throwable t) {
                    log.error("Message is failed to be acknowledged, messageId={}", messageId, t);
                }
            }
        }
    }
}
