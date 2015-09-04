package com.alibaba.middleware.race.mom.store;

import java.io.File;
import java.io.FileOutputStream;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import com.alibaba.middleware.race.mom.Message;
import com.alibaba.middleware.race.mom.MessageType;
import com.alibaba.middleware.race.mom.broker.BrokerHandler;
import com.alibaba.middleware.race.mom.broker.MessageRequest;
import com.alibaba.middleware.race.mom.protobuf.ProtobufUtil;


/*
 * 提供了同步刷盘的持久化方式
 * 使用了MBB和RAF
 * 注意：还需要实现换文件和删掉旧消息的逻辑
 */
public class MbbPersistentService implements PersistentService{
	
	private String path;
	
	private MappedByteBuffer mbb;
	
	private RandomAccessFile raf;
	
	private FileChannel fc;
	
	private int storeId = 0;
	
	private PersistentCallback callback;
	
	private long lastPersistentTime;
	
	private int cacheLength;
	
	private int curFileOffset;
	
	private static final int MAX_SIZE = 512;
	
	private static final long TIME_OUT = 500;
	
	private static final int MAX_FILE_SIZE = 1024 * 1024 * 64; 
	
	//存放的是等待进行缓存的消息
	private ConcurrentLinkedQueue<MessageRequest> cacheQueue;
	
	// 这个队列里面存放的是已经缓存到buffer中但是还没有持久化的消息
	private ConcurrentLinkedQueue<MessageRequest> ackQueue;
	
	
	public MbbPersistentService(String path,PersistentCallback callback) throws Exception
	{
		this.path = path;
		this.callback = callback;
		initFileSystem();
		initPersistentContext();
	}
	
	private void initFileSystem() throws Exception
	{
		
		preWriteFile();
		raf = new RandomAccessFile(path+"msg.dat", "rw");
		fc = raf.getChannel();
		mbb = fc.map(FileChannel.MapMode.READ_WRITE, 0, MAX_FILE_SIZE);
	}
	
	private void initPersistentContext()
	{
		lastPersistentTime = System.currentTimeMillis();
		cacheLength = 0;
		curFileOffset = 0;
		cacheQueue = new ConcurrentLinkedQueue<>();
		ackQueue = new ConcurrentLinkedQueue<>();
	}
	
	
	
	private void preWriteFile() throws Exception
	{
		 raf = new RandomAccessFile(path+"msg.dat", "rw");
		 fc = raf.getChannel();
		 ByteBuffer buffer= ByteBuffer.allocate(MAX_FILE_SIZE);
		 buffer.limit(MAX_FILE_SIZE);
	     fc.write( buffer);
	     fc.force(true);
	     fc.close();
	}
	
	public void commit(MessageRequest request) 
	{
		cacheQueue.offer(request);
	}
	
	
	/*
	 * 当前线程会不断扫描并且并且如果消息累积到一定量或者超时，则会刷新磁盘
	 */
	@Override
	public void run() 
	{
		while(true)
		{
			if(!cacheQueue.isEmpty())
			{
				cacheAllMessage();
			}
			if(isNeededPersistent())
			{
				try{
					boolean isPersistent = persistent();
					if(isPersistent){
						sendAllAck();
						lastPersistentTime = System.currentTimeMillis(); //上次持久化时间重置
						cacheLength = 0;  //持久化后缓存长度重置为0
					}
					else{
						//throw new Exception("持久化错误");
					}
				}catch(Exception e){
					Thread.currentThread().interrupt();
					//e.printStackTrace();
				}
			}
		}
	}
	
	private boolean isNeededPersistent()
	{
		boolean result = false;
		if(cacheLength > 0)
		{
			if(isOverFlow() || isTimeout())
			{
				result = true;
			}
		}
		return result;
	}
	
	private boolean isTimeout()
	{
		if(System.currentTimeMillis() - lastPersistentTime > TIME_OUT)
		{
			return true;
		}
		return false;
	}
	
	private boolean isOverFlow()
	{
		if(cacheLength > MAX_SIZE) 
		{
			//System.out.println("缓存达到，cachelength:"+cacheLength);
			return true;
		}
		return false;
	}
	
	
	private void cacheAllMessage()
	{
		while(!cacheQueue.isEmpty())
		{
			MessageRequest request = cacheQueue.poll();
			cacheMessage(request);
			ackQueue.offer(request);
		}
	}
	/*
	 * 这个方法一定只能在run方法中被当前线程调用，否则多线程访问buffer会出问题
	 */
	private void cacheMessage(MessageRequest request)
	{
		byte[] body = ProtobufUtil.encode(ProtobufUtil.createProtoMessage(request.getMessage()));
		int length = body.length;
		mbb.putInt(storeId);
		mbb.putInt(length);
		mbb.put(body);
		request.setStoreId(storeId);
		request.setOffset(curFileOffset);
		storeId++;
		cacheLength+=(length+8);
		curFileOffset += (length+8);
	}
	
	private boolean persistent() throws Exception
	{	
		try
		{	
			mbb.force();
			return true;
		}catch(Exception e)
		{
			e.printStackTrace();
			return false;
		}
	}
	
	private void sendAllAck()
	{
		while(!ackQueue.isEmpty())
		{
			MessageRequest request = ackQueue.poll();
			Message m = request.getMessage();
			BrokerHandler handler = request.getBrokerHandler();
			Message ackMsg = new Message();
			ackMsg.setMessageType(MessageType.ACK);
			ackMsg.setMsgId(m.getMsgId());
			handler.sendMessage(ackMsg);
			callback.onPersistent(request); //消息成功持久化后转发给consumer
		}
	}

	public int getCurFileOffset() {
		return curFileOffset;
	}

	public void setCurFileOffset(int curFileOffset) {
		this.curFileOffset = curFileOffset;
	}

	@Override
	public int getPersistentOffset() {
		return this.curFileOffset;
	}

	
	
}
