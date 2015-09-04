package com.alibaba.middleware.race.mom.store;

import com.alibaba.middleware.race.mom.broker.MessageRequest;


public interface PersistentService extends Runnable{
	void commit(MessageRequest request);
	int getPersistentOffset();
}
