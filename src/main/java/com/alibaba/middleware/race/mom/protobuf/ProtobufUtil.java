package com.alibaba.middleware.race.mom.protobuf;

import java.util.List;
import java.util.Map;

import com.alibaba.middleware.race.mom.Message;
import com.alibaba.middleware.race.mom.MessageType;
import com.alibaba.middleware.race.mom.broker.Broker;
import com.google.protobuf.InvalidProtocolBufferException;

public class ProtobufUtil {
	
	public static byte[] encode(ProtoMessage.Message protoMsg)
	{
		return protoMsg.toByteArray();
	}
	
	public static ProtoMessage.Message decode(byte[] body) throws InvalidProtocolBufferException
	{
		return ProtoMessage.Message.parseFrom(body);
	}
	
	public static ProtoMessage.Message createProtoMessage(Message m)
	{
		ProtoMessage.Message.Builder builder = ProtoMessage.Message.newBuilder();
		builder.setMsgId(m.getMsgId());
		builder.setTopic(m.getTopic());
		builder.setBody(new String(m.getBody()));
		builder.setGroupId(m.getGroupId());
		builder.setFilter(m.getFilter());
		builder.setBornTime(m.getBornTime());
		builder.setMessageType(m.getMessageType().ordinal());
		Map<String,String> map = m.getPropMap(); 
		if(map != null && !map.isEmpty())
		{
			for(String key : map.keySet())
			{
				builder.addPropertiesKey(key);
				builder.addPropertiesValue(map.get(key));
			}
		}
		return builder.build();
	}
	
	public static Message createMessage(ProtoMessage.Message protoMsg) throws Exception
	{
		Message m = new Message();
		m.setMsgId(protoMsg.getMsgId());
		m.setBody(protoMsg.getBody().getBytes());
		m.setBornTime(protoMsg.getBornTime());
		m.setFilter(protoMsg.getFilter());
		m.setGroupId(protoMsg.getGroupId());
		m.setMessageType(numToMsgType(protoMsg.getMessageType()));
		List<String> key = protoMsg.getPropertiesKeyList();
		List<String> value = protoMsg.getPropertiesValueList();
		if(key != null && !key.isEmpty())
		{
			for(int i=0;i<key.size();i++)
			{
				m.setProperty(key.get(i), value.get(i));
			}
		}
		m.setTopic(protoMsg.getTopic());
		return m;
		
	}
	
	public static MessageType numToMsgType(int num) throws Exception
	{
		switch(num)
		{
		case 0:return MessageType.PUBLISH;
		case 1:return MessageType.SUBSCRIBE;
		case 2:return MessageType.MsgRecv;
		case 3:return MessageType.ACK;
		case 4:return MessageType.NACK;
		case 5:return MessageType.NONE;
		default:throw new Exception("MessageType 错误");
		}
	}
	
	public static byte[] intToByteArray(int i) {   
		  byte[] result = new byte[4];   
		  result[0] = (byte)((i >> 24) & 0xFF);
		  result[1] = (byte)((i >> 16) & 0xFF);
		  result[2] = (byte)((i >> 8) & 0xFF); 
		  result[3] = (byte)(i & 0xFF);
		  return result;
		}
	 
	public static int byteArrayToInt(byte[] b) {
		   int offset = 0;
	       int value= 0;
	       for (int i = 0; i < 4; i++) {
	           int shift= (4 - 1 - i) * 8;
	           value +=(b[i + offset] & 0x000000FF) << shift;//往高位游
	       }
	       return value;
	 }
}
