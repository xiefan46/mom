package com.alibaba.middleware.race.momtest;

import com.alibaba.middleware.race.mom.DefaultProducer;
import com.alibaba.middleware.race.mom.Message;
import com.alibaba.middleware.race.mom.Producer;
import com.alibaba.middleware.race.mom.SendResult;
import com.alibaba.middleware.race.mom.SendStatus;

public class TestProducerReconnect {
	public static void main(String[] args) {
		Producer producer=new DefaultProducer();
		producer.setGroupId("PG-test");
		producer.setTopic("T-test");
		producer.start();
		
	}
}
