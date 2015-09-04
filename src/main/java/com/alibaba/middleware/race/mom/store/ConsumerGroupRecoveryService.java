package com.alibaba.middleware.race.mom.store;

import java.io.RandomAccessFile;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.util.Queue;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentLinkedQueue;

import com.alibaba.middleware.race.mom.Message;
import com.alibaba.middleware.race.mom.broker.GroupConnection;
import com.alibaba.middleware.race.mom.broker.SendMessageRequest;
import com.alibaba.middleware.race.mom.consumer.ConsumerConnection;
import com.alibaba.middleware.race.mom.protobuf.ProtoMessage;
import com.alibaba.middleware.race.mom.protobuf.ProtobufUtil;
import com.google.protobuf.InvalidProtocolBufferException;

/*
 * 这个线程用于根据sub文件和topic文件恢复ConsumerGroup的消费进度
 */
public class ConsumerGroupRecoveryService implements Callable<GroupConnection>{
	
	private static final int PRIORITY = 3; //线程优先级比默认的要低，因此具备一定的削峰填谷能力
	
	private String groupId;
	
	private String filter;
	
	private String topicPath;
	
	private String subFilePath;
	
	public ConsumerGroupRecoveryService(String groupId,String filter,String topicPath,String subFilePath) 
	{
		this.groupId = groupId;
		this.filter = filter;
		this.topicPath = topicPath;
		this.subFilePath = subFilePath;
	}
	
	@Override
	public GroupConnection call() throws Exception {
		Thread.currentThread().setPriority(PRIORITY);
		RandomAccessFile subFile = new RandomAccessFile(subFilePath, "rw");
		int offset = subFile.readInt();
		ConcurrentLinkedQueue<SendMessageRequest> msgQueue =  readMessageOutByOffset(
															topicPath, offset);
		GroupConnection groupConn = new GroupConnection(groupId, filter);
		groupConn.setSubFilePath(subFilePath);
		groupConn.setSubFile(subFile);
		groupConn.setWaitingToSendQueue(msgQueue);
		groupConn.setCurOffset(offset); //还原消费进度
		groupConn.setInitCurOffset(true);
		return groupConn;
	}
	
	private ConcurrentLinkedQueue<SendMessageRequest> readMessageOutByOffset(String fileName,int offset) throws Exception
	{
		ConcurrentLinkedQueue<SendMessageRequest> queue = new ConcurrentLinkedQueue<>();
		RandomAccessFile raf = new RandomAccessFile(fileName, "r");
		MappedByteBuffer mbb = raf.getChannel().map(FileChannel.MapMode.READ_ONLY,
								offset, raf.length() - offset);
		while(true)
		{
			int curOffset = offset;
			try{
				int storeId = mbb.getInt();
				int length = mbb.getInt();
				byte[] protoMsg = new byte[length];
				mbb.get(protoMsg);
				ProtoMessage.Message pm = ProtobufUtil.decode(protoMsg);
				Message m = ProtobufUtil.createMessage(pm);
				SendMessageRequest request = new SendMessageRequest(m);
				request.setOffset(curOffset);
				request.setStoreId(storeId);
				curOffset += (length + 8);
				queue.offer(request);
			}catch(InvalidProtocolBufferException e)	
			{
				break;
			}catch(Exception e)
			{
				e.printStackTrace();
			}
		}
		System.out.println("message queue 加载完毕");
		if(!queue.isEmpty()) queue.poll();
		return queue;
	}

}
