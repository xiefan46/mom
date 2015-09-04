package com.alibaba.middleware.race.momtest;

import java.util.Random;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.PriorityBlockingQueue;

import com.alibaba.middleware.race.mom.Message;
import com.alibaba.middleware.race.mom.broker.SendMessageRequest;

public class TestPriorityQueue {
	public static void main(String[] args)
	{
		Random rand = new Random(System.currentTimeMillis());
		BlockingQueue<SendMessageRequest> queue = new PriorityBlockingQueue<>();
		for(int i=0;i<10;i++)
		{
			Message m = new Message();
			SendMessageRequest request = new SendMessageRequest(m);
			int sid = rand.nextInt(100);
			request.setStoreId(sid);
			System.out.println(sid);
			queue.offer(request);
		}
		System.out.println("------------------");
		while(!queue.isEmpty())
		{
			SendMessageRequest request = queue.poll();
			System.out.println(request.getStoreId());
		}
	}
}
