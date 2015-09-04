package com.alibaba.middleware.race.momtest;

import com.alibaba.middleware.race.mom.ConsumeResult;
import com.alibaba.middleware.race.mom.ConsumeStatus;
import com.alibaba.middleware.race.mom.Consumer;
import com.alibaba.middleware.race.mom.DefaultConsumer;
import com.alibaba.middleware.race.mom.DefaultProducer;
import com.alibaba.middleware.race.mom.Message;
import com.alibaba.middleware.race.mom.MessageListener;
import com.alibaba.middleware.race.mom.Producer;

public class TestConsumerReconnect {
	public static void main(String[] args) throws Exception{
		Consumer consumer = new DefaultConsumer();
		//设置消费者id，groupid相同的消费者，broker会视为同一个消费者集群，每条消息只会投递给集群中的一台机器
		consumer.setGroupId("CG-test");
		consumer.start();
		Thread.currentThread().sleep(50000);
		consumer.stop();
	}
}
