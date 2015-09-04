package com.alibaba.middleware.race.mom.broker;

import java.io.FileDescriptor;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.Collections;
import java.util.List;
import java.util.Random;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.PriorityBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import com.alibaba.middleware.race.mom.Message;

/*
 * 代表一个Consumer集群的连接，只要消息向集群中一台机器投递成功就算成功
 */
/**
 * @author aris
 *
 */
public class GroupConnection {
	
	private String groupId;
	
	private String filter;
	
	private List<BrokerHandler> connections = new CopyOnWriteArrayList<BrokerHandler>();
	
	private Random rand = new Random(System.currentTimeMillis());
	
	private String subFilePath;
	
	private RandomAccessFile subFile; //这个file会在BrokerController中生成或者是在RecoveryService中加载
	
	private boolean isClose = false;
	
	private int curOffset;
	
	private boolean isInitCurOffset = false;
	
	private static final long CLOSE_GROUP_CONNECTION_INTERVAL = 10;
	
	private static final long COMMIT_OFFSET_INTERVAL = 1;
	
	//这个queue中消息的顺序与消息被存储的顺序一致
	private ConcurrentLinkedQueue<SendMessageRequest> waitingToSendQueue = 
												new ConcurrentLinkedQueue<>();
	
	//一个等待消费者返回ack的queue,如果失败则需要重发
	private ConcurrentLinkedQueue<SendMessageRequest> waitingAckQueue = 
												new ConcurrentLinkedQueue<>();
	
	public GroupConnection(String groupId,String filter)
	{
		this.groupId = groupId;
		this.filter = filter;
	}
	
	public void init()
	{
		/*
		 * 启动一个线程定时扫描并且关闭无效通道
		 */
		BrokerController.scheduledExecutor.scheduleAtFixedRate(new Runnable() {		
			@Override
			public void run() {
				if(!isClose)
				{
					for(BrokerHandler handler : connections)
					{
						if(!handler.isActive())
						{
							connections.remove(handler);
						}
					}
				}else
				{
					Thread.currentThread().interrupt();
				}
			}
		}, CLOSE_GROUP_CONNECTION_INTERVAL, CLOSE_GROUP_CONNECTION_INTERVAL,TimeUnit.SECONDS);
		
		/*
		 * 启动一个线程用于扫描waitingToSendQueue
		 */
		BrokerController.executor.execute(new Runnable() {
			@Override
			public void run() {
				while(!isClose)
				{
					while(!waitingToSendQueue.isEmpty())
					{
						SendMessageRequest request = waitingToSendQueue.poll();
						Message m = request.getMessage();
						if(isMatch(m, filter))
						{
							sendMessage(request);
							if(!isInitCurOffset)
							{
								curOffset = request.getOffset();
								isInitCurOffset = true;
							}
						}
					}
				}
			}
		});
		
		BrokerController.scheduledExecutor.scheduleAtFixedRate(new Runnable() {	
			@Override
			public void run() {
				try {
					if(!isClose){
						subFile.seek(0);
						subFile.writeInt(curOffset);
						subFile.getFD().sync();
					}else{
						Thread.currentThread().interrupt();
					}
				} catch (IOException e) {
					e.printStackTrace();
				}
				
			}
		}, COMMIT_OFFSET_INTERVAL, COMMIT_OFFSET_INTERVAL, TimeUnit.SECONDS);
		
	}
	
	private void sendMessage(SendMessageRequest request) {
		try
		{
			Message m = request.getMessage();
			waitingAckQueue.offer(request);
			request.setStartWaitingTime(System.currentTimeMillis());
			BrokerHandler handler = findBrokerHandler(m);
			m.setGroupId(groupId);		
			handler.sendMessage(m);
		}catch(Exception e)
		{
			e.printStackTrace();
		}
	}
	
	
	
	public void commitMessage(SendMessageRequest request)
	{
		Message m = request.getMessage();
		this.waitingToSendQueue.offer(request);
	}
	
	public void addBrokerHandler(BrokerHandler brokerHandler)
	{
		connections.add(brokerHandler);
	}
	
	
	public void onAck(Message ackMsg) throws Exception
	{
		
		SendMessageRequest request;
		if(!waitingAckQueue.isEmpty())
		{
			request = waitingAckQueue.poll();
			curOffset = request.getOffset();
			/*subFile.seek(0);
			subFile.writeInt(curOffset);
			subFile.getFD().sync();*/						
		}
		
	}
	
	/*
	 * 收到nack后，如果原本状态为SUCCESS，则不做处理，否则将状态设置为FAIL
	 */
	public void onNack(Message nackMsg) throws Exception
	{
		SendMessageRequest request = waitingAckQueue.poll();
		waitingToSendQueue.offer(request);
	}

	public String getGroupId() {
		return groupId;
	}

	public void setGroupId(String groupId) {
		this.groupId = groupId;
	}

	public String getFilter() {
		return filter;
	}

	public void setFilter(String filter) {
		this.filter = filter;
	}

	public boolean isActive() {
		if(connections == null || connections.isEmpty()) return false;
		for(BrokerHandler h : connections)
		{
			if(h.isActive()) return true;
		}
		return false;
	}

	
	
	/*
	 * 找到集群中一台机器进行消息的投递
	 */
	private BrokerHandler findBrokerHandler(Message m)
	{
		int id = rand.nextInt(connections.size());
		BrokerHandler handler = connections.get(id);
		if(handler != null && handler.isActive())
		{
			return handler;
		}
		else
		{
			for(BrokerHandler h : connections)
			{
				if(h != null && h.isActive())
				{
					return h;
				}
			}
		}
		return null;
	}
	
	//这个方法只有在重建消费进度的时候需要被调用
	public void setWaitingToSendQueue(ConcurrentLinkedQueue<SendMessageRequest> queue)
	{
		this.waitingToSendQueue = queue;
	}

	public String getSubFilePath() {
		return subFilePath;
	}

	public void setSubFilePath(String subFilePath) {
		this.subFilePath = subFilePath;
	}

	public RandomAccessFile getSubFile() {
		return subFile;
	}

	public void setSubFile(RandomAccessFile subFile) {
		this.subFile = subFile;
	}

	public int getCurOffset() {
		return curOffset;
	}

	public void setCurOffset(int curOffset) {
		this.curOffset = curOffset;
		try{
			subFile.seek(0);
			subFile.writeInt(curOffset);
			subFile.getFD().sync();	
		}catch(Exception e){
			e.printStackTrace();
		}
			
		
	}

	public boolean isInitCurOffset() {
		return isInitCurOffset;
	}

	public void setInitCurOffset(boolean isInitCurOffset) {
		this.isInitCurOffset = isInitCurOffset;
	}

	
	
	public void close() 
	{
		try {
			isClose = true;
			this.subFile.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	private boolean isMatch(Message m,String filter)
	 {
		 if(filter.equals("")|| filter == null) return true; 
		 String[] kv = filter.split("=");
		 String key = kv[0].trim();
		 String value = m.getProperty(key);
		 if(value == null || !value.equals(kv[1].trim()))
		 {
			 return false;
		 }
		 return true;
	 }
	
}
