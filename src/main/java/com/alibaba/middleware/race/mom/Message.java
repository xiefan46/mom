package com.alibaba.middleware.race.mom;

import java.io.Serializable;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

public class Message implements Serializable{
	/**
	 * 
	 */
	private String msgId=""; //全局唯一的消息id，不同消息不能重复
	private String topic="";
	private byte[] body="".getBytes();
	private String groupId="";
	private String filter="";
	private long bornTime = 0;
	private MessageType messageType = MessageType.NONE;
	private Map<String, String> properties = new HashMap<String, String>();
	
	public Message()
	{
		msgId = UniqueIDUtil.getUniqueId()+"";
	}
	
	
	
	public String getFilter() {
		return filter;
	}



	public void setFilter(String filter) {
		this.filter = filter;
	}



	public String getGroupId() {
		return groupId;
	}



	public void setGroupId(String groupId) {
		this.groupId = groupId;
	}



	public MessageType getMessageType() {
		return messageType;
	}
	public void setMessageType(MessageType messageType) {
		this.messageType = messageType;
	}
	

	public void setTopic(String topic) {
		this.topic = topic;
	}
	public String getMsgId() {
		return msgId;
	}
	public void setMsgId(String msgId) {
		this.msgId = msgId;
	}
	public String getTopic() {
		return topic;
	}

	public void setBody(byte[] body) {
		this.body = body;
	}

	public byte[] getBody() {
		return body;
	}

	public String getProperty(String key) {
		return properties.get(key);
	}
	/**
	 * 设置消息属性
	 * @param key
	 * @param value
	 */
	public void setProperty(String key, String value) {
		properties.put(key, value);
	}
	/**
	 * 删除消息属性
	 * @param key
	 */
	public void removeProperty(String key) {
		properties.remove(key);
	}
	public long getBornTime() {
		return bornTime;
	}
	public void setBornTime(long bornTime) {
		this.bornTime = bornTime;
	}
	
	public void printMap()
	{
		System.out.println(properties);
	}
	
	public Map<String,String> getPropMap()
	{
		return properties;
	}
	
	public static void main(String[] args)
	{
		UUID uuid = UUID.randomUUID();
		System.out.println(uuid);
	}
}
