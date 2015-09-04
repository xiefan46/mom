package com.alibaba.middleware.race.mom;

import com.alibaba.middleware.race.mom.broker.BrokerHandler;

import io.netty.channel.Channel;

public class SubscribeEntry {
	private BrokerHandler handler;
	private String filter;
	
	
	
	public BrokerHandler getHandler() {
		return handler;
	}
	public void setHandler(BrokerHandler handler) {
		this.handler = handler;
	}
	public String getFilter() {
		return filter;
	}
	public void setFilter(String filter) {
		this.filter = filter;
	}
	
	
}
