package com.alibaba.middleware.race.momtest;



import java.io.File;
import java.io.IOException;

public class FileTest {
	public static void main(String[] args) throws IOException {
		File file = new File("test/pp/tt/asd.txt");
		File parent = file.getParentFile();
		if(parent!=null&&!parent.exists()){
			parent.mkdirs();
		}
		file.createNewFile();
		}

} 