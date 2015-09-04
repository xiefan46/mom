package com.alibaba.middleware.race.mom.protobuf;

import java.util.Collections;
import java.util.List;
import java.util.Map;

import com.alibaba.middleware.race.mom.Message;
import com.alibaba.middleware.race.mom.MessageType;
import com.google.protobuf.InvalidProtocolBufferException;

public class TestProtobuf {
	private static byte[] encode(ProtoMessage.Message protoMsg)
	{
		return protoMsg.toByteArray();
	}
	
	private static ProtoMessage.Message decode(byte[] body) throws InvalidProtocolBufferException
	{
		return ProtoMessage.Message.parseFrom(body);
	}
	
	private static ProtoMessage.Message createProtoMessage(Message m)
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
	
	private static Message createMessage(ProtoMessage.Message protoMsg) throws Exception
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
	
	private static MessageType numToMsgType(int num) throws Exception
	{
		switch(num)
		{
		case 0:return MessageType.PUBLISH;
		case 1:return MessageType.SUBSCRIBE;
		case 2:return MessageType.MsgRecv;
		case 3:return MessageType.ACK;
		case 4:return MessageType.NACK;
		default:throw new Exception("MessageType 错误");
		}
	}
	
	public static void main(String[] args) throws Exception
	{
		Message m = new Message();
		m.setBody("hello".getBytes());
		m.setBornTime(System.currentTimeMillis());
		m.setFilter("city=guangzhou");
		m.setGroupId("test");
		m.setMessageType(MessageType.PUBLISH);
		m.setProperty("key1", "value1");
		m.setProperty("key2", "value2");
		m.setTopic("topic1");
		ProtoMessage.Message protoMsg = createProtoMessage(m);
		System.out.println("before encode:"+protoMsg);
		System.out.println("after encode decode "+decode(encode(protoMsg)));
		Message m2 = createMessage(protoMsg);
		System.out.println(m2.getBornTime());
		System.out.println(m2.getFilter());
		System.out.println(m2.getGroupId());
		System.out.println(m2.getMsgId());
		System.out.println(m2.getPropMap());
		System.out.println(m2.getTopic());
		System.out.println(new String(m2.getBody()));
		System.out.println(m2.getMessageType());
	}
}
