package com.alibaba.middleware.race.mom.broker;

import com.alibaba.middleware.race.mom.Message;

public class MessageRequest {
	
	private Message message;
	
	private BrokerHandler brokerHandler;
	
	private int offset;
	
	private int storeId;
	
	public MessageRequest(Message message,BrokerHandler brokerHandler)
	{
		this.message = message;
		this.brokerHandler = brokerHandler;
	}

	public Message getMessage() {
		return message;
	}

	public void setMessage(Message message) {
		this.message = message;
	}

	public BrokerHandler getBrokerHandler() {
		return brokerHandler;
	}

	public void setBrokerHandler(BrokerHandler brokerHandler) {
		this.brokerHandler = brokerHandler;
	}

	public int getOffset() {
		return offset;
	}

	public void setOffset(int offset) {
		this.offset = offset;
	}

	public int getStoreId() {
		return storeId;
	}

	public void setStoreId(int storeId) {
		this.storeId = storeId;
	}
	
	
	
}
