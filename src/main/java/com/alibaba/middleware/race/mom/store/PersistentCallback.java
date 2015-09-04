package com.alibaba.middleware.race.mom.store;

import com.alibaba.middleware.race.mom.Message;
import com.alibaba.middleware.race.mom.broker.MessageRequest;

public interface PersistentCallback {
	void onPersistent(MessageRequest m);
}
