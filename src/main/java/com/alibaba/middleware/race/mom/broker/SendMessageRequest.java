package com.alibaba.middleware.race.mom.broker;

import com.alibaba.middleware.race.mom.Message;

/**
 * @author aris
 *
 */
public class SendMessageRequest implements Comparable<SendMessageRequest>{
	
	private Message message;
	
	private int offset;
	
	private int storeId;
	
	private long startWaitingTime;
	
	private WaitingState state = WaitingState.WAITING;
	
	private int resendCount = 0;  //重发的次数
	
	private static final int MAX_RESEND_COUNT = 3; //最大重发次数
	
	private static final long TIME_OUT = 60000;  //超时时间60s
	
	public SendMessageRequest(Message message)
	{
		this.message = message;
	}
	
	public void setStartWaitingTime(long startWaitingTime) {
		this.startWaitingTime = startWaitingTime;
	}
	public Message getMessage() {
		return message;
	}
	
	public WaitingState getState() {
		return state;
	}
	
	public void setState(WaitingState state) {
		this.state = state;
	}
	
	public boolean isTimeout()
	{
		if(System.currentTimeMillis() - startWaitingTime >= TIME_OUT)
		{
			return true;
		}
		return false;
	}
	
	public boolean achiveMaxResendCount()
	{
		if(this.resendCount >= MAX_RESEND_COUNT)
		{
			return true;
		}
		return false;
	}
	
	public void resendCountIncrease()
	{
		this.resendCount++;
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

	@Override
	public int compareTo(SendMessageRequest o) {
		if(this.storeId < o.getStoreId())
			return -1;
		if(this.storeId > o.getStoreId()) 
			return 1;
		return 0;
	}
	
	
	
}
