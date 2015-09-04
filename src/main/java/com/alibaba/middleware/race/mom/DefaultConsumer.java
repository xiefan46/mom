package com.alibaba.middleware.race.mom;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

import io.netty.bootstrap.Bootstrap;
import io.netty.buffer.PooledByteBufAllocator;
import io.netty.channel.AdaptiveRecvByteBufAllocator;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;

import com.alibaba.middleware.race.mom.consumer.ConsumerConnection;
import com.alibaba.middleware.race.mom.consumer.ConsumerConnectionFactory;
import com.alibaba.middleware.race.mom.protobuf.ProtobufUtil;







public class DefaultConsumer implements Consumer{
	
	private String host;
	
	private int port = 9999;
	
	private String groupId;
	
	private String topic;
	
	private String filter;
	
	private MessageListener listener;
	
	private ConsumerConnectionFactory consumerConnectionFactory;
	
	public DefaultConsumer() {
		try
		{	
			host = System.getProperty("SIP","127.0.0.1");
		}catch(Exception e)
		{
			e.printStackTrace();
		}
	}
	
	@Override
	public void start() 
	{
		try {
			Message m = new Message();
			processMessage(m);
			consumerConnectionFactory = new ConsumerConnectionFactory(host, port,this.listener,m);
			System.out.println("consumer online");
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	
	
	@Override
	public void subscribe(String topic, String filter, MessageListener listener) 
	{
		try
		{
			this.topic = topic;
			this.filter = filter;
			this.listener = listener;
			Message m = new Message();
			processMessage(m);	
			consumerConnectionFactory = new ConsumerConnectionFactory(host, port, listener,m);
			consumerConnectionFactory.close();
		}catch(Exception e)
		{
			e.printStackTrace();
		}
		
	}
	
	private void processMessage(Message m)
	{
		m.setTopic(topic);
		m.setFilter(filter);
		m.setMessageType(MessageType.SUBSCRIBE);
		m.setGroupId(groupId);
		m.setBornTime(System.currentTimeMillis());
	}

	@Override
	public void setGroupId(String groupId) {
		this.groupId = groupId;
	}

	@Override
	public void stop() {
		try
		{
			consumerConnectionFactory.close();
			Thread.currentThread().sleep(1000);
		}catch(Exception e)
		{
			e.printStackTrace();
		}
		
	}

	
	
	
	
}
