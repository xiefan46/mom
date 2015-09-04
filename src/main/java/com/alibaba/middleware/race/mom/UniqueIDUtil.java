package com.alibaba.middleware.race.mom;

import java.util.Date;
import java.util.Random;

public class UniqueIDUtil 
{
		
		public static Integer getUniqueId() {
			long cur = new Date().getTime();
			int curInt = (int)cur;
			int random = rand.nextInt(1000000);
			return Integer.valueOf(curInt + random);
		}

		public static void main(String[] args) {
			for(int i=0;i<10;i++) System.out.println(getUniqueId());
		}
		
		private static Random rand = new Random(System.currentTimeMillis());
	
}
