package com.alibaba.middleware.race.momtest;

import java.io.File;
import java.io.RandomAccessFile;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.util.HashMap;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.LinkedBlockingDeque;

import com.alibaba.middleware.race.mom.Message;
import com.alibaba.middleware.race.mom.MessageType;
import com.alibaba.middleware.race.mom.protobuf.ProtoMessage;
import com.alibaba.middleware.race.mom.protobuf.ProtobufUtil;
import com.google.protobuf.InvalidProtocolBufferException;

/*
 * 假设目前已经broker挂掉，然后收到了Consumer的一个重连请求
 */
public class TestRecovery {
	private static final int FILE_SIZE = 1024 * 1024 * 4; 
	private static int offset;
	private static final int FILE_END = -1;
	
	public static void main(String[] args) throws Exception
	{
		String rootpath = "./store/";
		String fileName = "test" + Math.random()+".dat";
		writeMessageToFile(rootpath+fileName);
		Queue<Message> queue = readMessageOutByOffset(rootpath+fileName, offset);
		while(!queue.isEmpty())
		{
			Message m = queue.poll();
			System.out.println("msgId:"+m.getMsgId()+"  body:"+new String(m.getBody(),"utf-8")+"  property:"+m.getPropMap());
		}
	}
	
	public static void writeMessageToFile(String filename) throws Exception
	{
		RandomAccessFile raf = new RandomAccessFile(filename, "rw");
		MappedByteBuffer mbb = raf.getChannel().map(FileChannel.MapMode.READ_WRITE, 
						0, FILE_SIZE);
		int totalLength = 0;
		byte[] body = "hello world".getBytes();
		for(int i=0;i<10000;i++)
		{
			Message m = new Message();
			m.setBody(body);
			m.setProperty("key1", "value1");
			m.setProperty("key2", "value2");
			//System.out.println("msgId:"+m.getMsgId());
			byte[] content = ProtobufUtil.encode(ProtobufUtil.createProtoMessage(m));
			mbb.putInt(i);
			mbb.putInt(content.length);
			mbb.put(content);
			if(i == 100) {
				//System.out.println("totalLength:"+totalLength);
				offset = totalLength;
			}
			totalLength+=(content.length + 8);
		}
		mbb.force();
		raf.close();
	}
	
	
	
	//将offset之后的message都加载出来并且放到一个queue中
	//storeId为-1表示文件结束
	public static Queue<Message> readMessageOutByOffset(String fileName,int offset) throws Exception
	{
		Queue<Message> queue = new ConcurrentLinkedQueue<Message>();
		RandomAccessFile raf = new RandomAccessFile(fileName, "rw");
		MappedByteBuffer mbb = raf.getChannel().map(FileChannel.MapMode.READ_ONLY,
								offset, raf.length() - offset);
		while(true)
		{
			try{
				int store = mbb.getInt();
				int length = mbb.getInt();
				byte[] protoMsg = new byte[length];
				mbb.get(protoMsg);
				ProtoMessage.Message pm = ProtobufUtil.decode(protoMsg);
				//System.out.println(pm);
				Message m = ProtobufUtil.createMessage(pm);
				queue.add(m);
				/*System.out.println("mbb position:"+mbb.position());
				System.out.println("total length:"+totalLength);
				System.out.println("limit:"+mbb.limit());*/
			}catch(InvalidProtocolBufferException e)	
			{
				break;
			}catch(Exception e)
			{
				e.printStackTrace();
			}
		}
		System.out.println("message 加载完毕");
		return queue;
	}
	
	
	
	
	
}
