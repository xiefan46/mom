package com.alibaba.middleware.race.momtest;

import java.io.FileDescriptor;
import java.io.RandomAccessFile;



public class TestOverrideFlie {
	public static void main(String[] args) throws Exception
	{
		String path = Math.random()+".dat";
		RandomAccessFile raf = new RandomAccessFile(path, "rw");
		for(int i=0;i<100;i++)
		{
			raf.seek(0);
			raf.writeInt(i);
			FileDescriptor fd = raf.getFD();
			fd.sync();
			raf.seek(0);
			System.out.println(raf.readInt());
		}
	}
}
