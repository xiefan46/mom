package com.alibaba.middleware.race.mom;

import com.alibaba.middleware.race.mom.producer.ProducerConnection;
import com.alibaba.middleware.race.mom.producer.PoolableProducerConnectionFactory;






public class DefaultProducer implements Producer{

	private String topic;
	
	private String groupId;
	
	private String host;
	
	private static int port = 9999;
	
	private PoolableProducerConnectionFactory producerConnectionFactory;  //handler的连接池
	
	
	public DefaultProducer() 
	{
		try
		{
			host=System.getProperty("SIP","127.0.0.1");
		}catch(Exception e)
		{
			e.printStackTrace();
		}
	}
	
	@Override
	public void start() 
	{	
		try {
			producerConnectionFactory = new PoolableProducerConnectionFactory(host, port);
			producerConnectionFactory.init();
			System.out.println("producer start! host"+host+"  port:"+port);
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		
	}

	@Override
	public void setTopic(String topic) {
		this.topic = topic;
		
	}

	@Override
	public void setGroupId(String groupId) {
		this.groupId = groupId;
	}

	@Override
	public SendResult sendMessage(Message message) 
	{
		try
		{
			processMessage(message);
			ProducerConnection h = producerConnectionFactory.getConnection();
			SendResult s = h.sendMessage(message);
			producerConnectionFactory.recycleConnection(h);
			return s;
		}catch(Exception e)
		{
			e.printStackTrace();
		}
		return null;
		
	}

	@Override
	public void asyncSendMessage(Message message, SendCallback callback) 
	{
		try
		{
			processMessage(message);
			ProducerConnection h = producerConnectionFactory.getConnection();
			h.asyncSendMessage(message, callback);
			producerConnectionFactory.recycleConnection(h);		
		}catch(Exception e)
		{
			e.printStackTrace();
		}
	}
	
	private void processMessage(Message message)
	{
		message.setMessageType(MessageType.PUBLISH);
		message.setTopic(this.topic);
		message.setGroupId(this.groupId);
		message.setBornTime(System.currentTimeMillis());
	}

	@Override
	public void stop() {
		producerConnectionFactory.clear();
		
	}
	
}
