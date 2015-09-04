package com.alibaba.middleware.race.mom.consumer;



import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import com.alibaba.middleware.race.mom.ConsumeResult;
import com.alibaba.middleware.race.mom.ConsumeStatus;
import com.alibaba.middleware.race.mom.DefaultConsumer;
import com.alibaba.middleware.race.mom.Message;
import com.alibaba.middleware.race.mom.MessageListener;


import com.alibaba.middleware.race.mom.MessageType;
import com.alibaba.middleware.race.mom.protobuf.ProtoMessage;
import com.alibaba.middleware.race.mom.protobuf.ProtobufUtil;

import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;

public class ConsumerConnection extends ChannelInboundHandlerAdapter
{
	
	 private MessageListener listener;
	 
	 private Message subMsg;
	
	 public ConsumerConnection(MessageListener listener,Message subMsg) {
		this.listener = listener;
		this.subMsg = subMsg;
	}
	 
	 @Override
	 public void channelActive(ChannelHandlerContext ctx) throws Exception 
	 {
		   ctx.writeAndFlush(ProtobufUtil.createProtoMessage(subMsg));
	 }   

	 @Override
	 public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception 
	 {
		   System.out.println("consumer收到消息");
		   ProtoMessage.Message protoMsg = (ProtoMessage.Message)msg;
		   Message m = ProtobufUtil.createMessage(protoMsg);
		   ConsumeResult result =  listener.onMessage(m);
		   if(result.getStatus() == ConsumeStatus.SUCCESS){
				ctx.writeAndFlush(ProtobufUtil.createProtoMessage(ack(m)));
		   }
		   else{
				ctx.writeAndFlush(ProtobufUtil.createProtoMessage(nack(m)));
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
	       cause.printStackTrace();
	 }  
	 
	 private Message ack(Message m)
	 {
		 Message ackMsg = new Message();
		 ackMsg.setMessageType(MessageType.ACK);
		 ackMsg.setMsgId(m.getMsgId());
		 ackMsg.setTopic(m.getTopic());
		 ackMsg.setGroupId(subMsg.getGroupId());
		 return ackMsg;
	 }
	 
	 private Message nack(Message m)
	 {
		 Message nackMsg = new Message();
		 nackMsg.setMessageType(MessageType.NACK);
		 nackMsg.setMsgId(m.getMsgId());
		 nackMsg.setTopic(m.getTopic());
		 nackMsg.setGroupId(subMsg.getGroupId());
		 return nackMsg;
	 }
	
	 
	 
}
