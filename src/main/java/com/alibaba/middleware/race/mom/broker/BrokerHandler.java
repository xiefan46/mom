package com.alibaba.middleware.race.mom.broker;



import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import com.alibaba.middleware.race.mom.Message;
import com.alibaba.middleware.race.mom.MessageType;
import com.alibaba.middleware.race.mom.SendResult;
import com.alibaba.middleware.race.mom.SendStatus;
import com.alibaba.middleware.race.mom.protobuf.ProtoMessage;
import com.alibaba.middleware.race.mom.protobuf.ProtobufUtil;

import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;

/*
 * BrokerHandler负责收发消息，并且负责用protobuf对消息进行编解码
 * BrokerHandler接下来会将消息发送给BrokerController进一步处理
 */
public class BrokerHandler extends ChannelInboundHandlerAdapter{
	
	 private static BrokerController brokerController= new BrokerController();
	 
	 private Channel channel;
	 
	
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
		  brokerController.handle(new MessageRequest(m, this));
	 }  
	 
	 
	 @Override
	 public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause)
	            throws Exception 
	 {
	     //cause.printStackTrace();
	     ctx.close();
	 }  
	 
	 public boolean isActive()
	 {
		 return channel.isActive();
	 }
	
	 public void close()
	 {
		 channel.close();
	 }
	 
	 public void sendMessage(Message m)
	 {
		 //waitingToAckMap.put(m.getMsgId(), m);
		 //System.out.println("handler发送消息 msgId"+m.getMsgId());
		 System.out.println("消息将要被从broker发送到出去 msgId"+m.getMsgId()+"  消息类型："+m.getMessageType()+"  properties:"+m.getPropMap());
		 channel.writeAndFlush(ProtobufUtil.createProtoMessage(m));
	 }
	 
	
	 
}
