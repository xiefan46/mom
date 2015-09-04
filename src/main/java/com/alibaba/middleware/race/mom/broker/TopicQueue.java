package com.alibaba.middleware.race.mom.broker;

import io.netty.channel.Channel;

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileDescriptor;
import java.io.FileOutputStream;
import java.io.PrintStream;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.FutureTask;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import com.alibaba.middleware.race.mom.Message;
import com.alibaba.middleware.race.mom.MessageType;
import com.alibaba.middleware.race.mom.protobuf.ProtobufUtil;
import com.alibaba.middleware.race.mom.store.ConsumerGroupRecoveryService;
import com.alibaba.middleware.race.mom.store.PersistentCallback;
import com.alibaba.middleware.race.mom.store.PersistentService;
import com.alibaba.middleware.race.mom.store.MbbPersistentService;
public class TopicQueue implements PersistentCallback{
	
	private String topic;
	
	//private static final String rootPath = System.getProperty("user.home","./")+"/store/";
	private static final String rootPath = "./store/";
	
	private static final long SCAN_GROUP_REGISTRY_INTERVAL = 10;
	
	private String path;
	
	private PersistentService persistentService;
	
	/*
	 * 消费者集群注册表 groupId -- GroupConnection
	 */
	private ConcurrentHashMap<String, GroupConnection> groupRegistryTable
													= new ConcurrentHashMap<>();
	
	
	public TopicQueue(String topic)
	{
		this.topic = topic;
		init();
	}
	
	private void init() 
	{
		try
		{
			path = rootPath + topic+"/";  //msg.dat存放的是当前topic下所有message
			tryCreate();
			persistentService = new MbbPersistentService(path,this);
			BrokerController.executor.execute(persistentService);
			
			/*
			 * 启动一个定时线程不断扫描关掉无效的GroupConnection
			 */
			BrokerController.scheduledExecutor.scheduleAtFixedRate(new Runnable(){

				@Override
				public void run() {
					for(String key : groupRegistryTable.keySet())
					{
						GroupConnection conn = groupRegistryTable.get(key);
						if(conn == null )
						{
							groupRegistryTable.remove(key);
						}
						else if(!conn.isActive())
						{
							groupRegistryTable.remove(key);
							conn.close();
						}
					}
				}
				
			}, SCAN_GROUP_REGISTRY_INTERVAL, SCAN_GROUP_REGISTRY_INTERVAL, TimeUnit.SECONDS);
			
		}catch(Exception e)
		{
			e.printStackTrace();
		}
	}
	
	private void tryCreate() throws Exception
	{
		File file = new File(path+"/subscribe/init.dat");
		File parent = file.getParentFile();
		if(parent!=null&&!parent.exists())
		{
			parent.mkdirs();
		}
		file.createNewFile();
	}
	
	public void commit(MessageRequest request)
	{
		Message m = request.getMessage();
		persistentService.commit(request);
	}
	
	public void subscribe(MessageRequest request) throws Exception
	{
		Message m = request.getMessage();
		String groupId = m.getGroupId();
		GroupConnection groupConn = groupRegistryTable.get(groupId);
		if(groupConn != null)
		{
			System.out.println("原有的集群机器加入");
			groupConn.setFilter(m.getFilter());
			groupConn.addBrokerHandler(request.getBrokerHandler());
		}
		else
		{
			
			String subFilePath = path + "subscribe/" + groupId+".csub";
			File subStateFile = new File(subFilePath);
			//进度文件存在但是群组不存在，表示需要重建消费进度
			if(subStateFile.exists())
			{
				System.out.println("查找到文件，需要重建消费进度");
				FutureTask<GroupConnection> recoveryTask = new FutureTask<>(
												new ConsumerGroupRecoveryService(m.getGroupId(),m.getFilter(),path+"msg.dat", subFilePath));
				BrokerController.executor.execute(recoveryTask);
				groupConn = recoveryTask.get();
			}
			//进度文件不存在、群组不存在，表示Group是全新的
			else
			{
				System.out.println("有全新的groupConnection加入");
				subStateFile.createNewFile();	
				groupConn = new GroupConnection(groupId,m.getFilter());	
				groupConn.setSubFilePath(subFilePath);
				groupConn.setSubFile(new RandomAccessFile(subStateFile, "rw"));
				groupConn.setCurOffset(persistentService.getPersistentOffset());
			}
			groupConn.init();
			groupConn.addBrokerHandler(request.getBrokerHandler());
			groupRegistryTable.put(groupConn.getGroupId(), groupConn);
		}
	}
	
	/*
	 * 在分发的之后MessageRequest会被重新打包成SendMessageRequest
	 */
	private void transmit(MessageRequest request)  //需要考虑对于无效的channel要如何处理
	 {
		System.out.println("groupRegistryTable size:"+groupRegistryTable.size());
		for(String key : groupRegistryTable.keySet())
		{
			GroupConnection groupConnection = groupRegistryTable.get(key);
			if(groupConnection != null && groupConnection.isActive())
			{
				if(isMatch(request.getMessage(),groupConnection.getFilter()))
				 {
					 System.out.println("消息分发给GroupConnection groupId"+groupConnection.getGroupId());
					 Message m = request.getMessage();
			
					 SendMessageRequest sendRequest = new SendMessageRequest(
							 								request.getMessage());
					 sendRequest.setOffset(request.getOffset());
					 sendRequest.setStoreId(request.getStoreId());
					 groupConnection.commitMessage(sendRequest);
				 }
			}
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
	 
	@Override
	public void onPersistent(MessageRequest request) 
	{
		Message m = request.getMessage();
		//System.out.println("消息被持久化完成，准备转发："+m.getMsgId()+"  消息类型："+m.getMessageType()+"  properties:"+m.getPropMap());
		transmit(request);	
	}

	public String getTopic() {
		return topic;
	}

	public void setTopic(String topic) {
		this.topic = topic;
	}
	
	public GroupConnection getGroupConnection(String groupId)
	{
		return groupRegistryTable.get(groupId);
	}
	 
	 
	
	 
	 
	
}
