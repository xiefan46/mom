package com.alibaba.middleware.race.mom.producer;

import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.TimeUnit;

import com.alibaba.middleware.race.mom.Message;
import com.alibaba.middleware.race.mom.MessageType;
import com.alibaba.middleware.race.mom.SendCallback;
import com.alibaba.middleware.race.mom.SendResult;
import com.alibaba.middleware.race.mom.SendStatus;
import com.alibaba.middleware.race.mom.protobuf.ProtoMessage;
import com.alibaba.middleware.race.mom.protobuf.ProtobufUtil;

import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;

public class ProducerConnection extends ChannelInboundHandlerAdapter{
	 
	 private Channel channel;
	 
	 private final BlockingQueue<SendResult> answer = new LinkedBlockingDeque<SendResult>();
	 
	 //msgId--callback映射
	 private static Map<String,SendCallback> asyncRegistry
	 									= new ConcurrentHashMap<String, SendCallback>();
	 
	 
	 @Override
	 public void channelActive(ChannelHandlerContext ctx) throws Exception 
	 {
	       this.channel = ctx.channel();
	 }   

	 @Override
	 public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception 
	 {
		 ProtoMessage.Message protoMsg = (ProtoMessage.Message)msg;
		 Message m = ProtobufUtil.createMessage(protoMsg);
		 //System.out.println("producer收到ack msgId:"+m.getMsgId());
		 SendCallback callback = asyncRegistry.get(m.getMsgId());
		 if(callback != null)  //异步
		 {
			 callback.onResult(messageResult(m));
		 }
		 else 
		 {
			 answer.offer(messageResult(m),3000,TimeUnit.MILLISECONDS); //同步
		 }
	 }      
	 
	 @Override
	 public void channelReadComplete(ChannelHandlerContext ctx) throws Exception 
	 {
	       ctx.flush();
	 }   
	 
	 @Override
	 public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause)
	            throws Exception 
	 {
	       ctx.close();
	       //cause.printStackTrace();
	 }   
	 
	 public SendResult sendMessage(Message m)
	 {
		 channel.writeAndFlush(ProtobufUtil.createProtoMessage(m));
		 SendResult s = null;
		 boolean interrupted = false;
		 for(;;)
		 {
			 try
			{
				s = answer.take();
				break;
			}catch(InterruptedException e)
			{
				interrupted = true;
			}
		 }
		 if (interrupted) 
		 {  
			Thread.currentThread().interrupt();  
		 } 
		 return s;
	 }
	 
	 public void asyncSendMessage(Message message,SendCallback callback)
	 {
		 this.asyncRegistry.put(message.getMsgId(), callback);
		 channel.writeAndFlush(ProtobufUtil.createProtoMessage(message));
	 }
	 
	 public void close() throws Exception
	 {
		 this.channel.close().sync();
	 }
	 
	 private SendResult messageResult(Message m) throws Exception
	 {
		 SendResult s = new SendResult();
		 s.setMsgId(m.getMsgId());
		 if(m.getMessageType() == MessageType.ACK)
		 {
			 //System.out.println("收到ack msgId:"+m.getMsgId());
			 s.setStatus(SendStatus.SUCCESS);
		 }
		 else if(m.getMessageType() == MessageType.NACK)
		 {
			 s.setStatus(SendStatus.FAIL);
		 }
		 else
		 {
			 throw new Exception("返回消息类型错误");
		 }
		 return s;
	 }
	
	 public boolean isActive()
	 {
		 return channel.isActive();
	 }
}
