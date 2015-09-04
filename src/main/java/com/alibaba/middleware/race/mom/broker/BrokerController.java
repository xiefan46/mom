package com.alibaba.middleware.race.mom.broker;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;

import com.alibaba.middleware.race.mom.Message;
import com.alibaba.middleware.race.mom.MessageType;


/*
 * BrokerController负责解析消息类型，并且做出相应的处理
 */
public class BrokerController {
	
	public static final ExecutorService executor = 
			Executors.newFixedThreadPool(Runtime.getRuntime().availableProcessors()*2); 
	
	public static final ScheduledExecutorService scheduledExecutor = 
			Executors.newScheduledThreadPool(Runtime.getRuntime().availableProcessors()*2);
	
	/*
	 * 在收到subscribe请求后，controller会新建一个主题队列并且在注册表中注册
	 */
	private ConcurrentHashMap<String, TopicQueue> topicRegistryTable
												= new ConcurrentHashMap<>();
	
	public BrokerController()
	{
		
	}
	
	public void handle(MessageRequest request) throws Exception
	{
		Message m = request.getMessage();
		MessageType type = m.getMessageType();
		if(type == MessageType.PUBLISH)
		{
			TopicQueue queue  = topicRegistryTable.get(m.getTopic());
			if(queue == null)  
			 {
				throw new Exception("无consumer监听此主题");
			 }
			  //将msg加入到特定消息队列中
			 queue.commit(request);  	  	  
		}
		else if(type == MessageType.SUBSCRIBE)
		 {
			   TopicQueue queue = topicRegistryTable.get(m.getTopic());
			   if(queue == null)
			   {
				   queue = new TopicQueue(m.getTopic());
				  topicRegistryTable.put(m.getTopic(),queue);
			   }
			   queue.subscribe(request);  //把connection加入到session
		  }
		  else if(type == MessageType.ACK || type == MessageType.NACK)
		  {
			  try
			  {
				 TopicQueue queue = topicRegistryTable.get(m.getTopic());
				 if(queue == null) throw new Exception("无效ack,topic不存在");
				 GroupConnection groupConn = queue.getGroupConnection(m.getGroupId());
				 if(groupConn == null) throw new Exception("无效ack,group不存在");
				 if(type == MessageType.ACK) {
					 groupConn.onAck(m);
				 }
				 else{
					 groupConn.onNack(m);
				 }
			  }catch(Exception e)
			  {
				  e.printStackTrace();
			  }
			   
		  }
		  else
		  {
			  throw new Exception("消息类型错误");
		  }
	}
}
