package com.alibaba.middleware.race.momtest;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;


public class TestPreWrite {
    public static void main(String[] args) throws IOException {
        final String bfile = "bfile";
        final String f2 = "yoo";
        trydelete(bfile);
        trydelete(f2);
        fillfile(bfile);
        testWriteFile(bfile);

        System.out.println("------yoo------");
        testWriteFile(f2);


    }

    private static void fillfile(String bfile) throws IOException {
        //预写入
        FileChannel fileChannel = new RandomAccessFile(bfile, "rw").getChannel();
        ByteBuffer inibuf = ByteBuffer.allocate(4 * 1024 * 1024);
        inibuf.limit(4 * 1024 * 1024);
        fileChannel.write(inibuf);
        fileChannel.force(true);
        fileChannel.close();
    }

    private static void testWriteFile(String bfile) throws IOException {
        FileChannel fileChannel;
        fileChannel = new RandomAccessFile(bfile, "rw").getChannel();
        int num = 32;
        for (; num< 1024 *4*4*4; num*=2){
            byte[] tmp = new byte[num];
            //fake data
            for(int i = 0; i< tmp.length; i++){
                tmp[i] = (byte)(i%128);
            }
            ByteBuffer buf = ByteBuffer.allocate(num);

            long delay =0 ;
            final int count = 10;
            int datalen = 0;
            for(int i=0;i<count;i++){
                buf.clear();
                buf.put(tmp);
                buf.flip();
                long start = System.nanoTime();
                datalen = fileChannel.write(buf);
                fileChannel.force(false);
                long end =System.nanoTime();
                delay +=end-start;
            }
            System.out.println(String.format("%8d %d", datalen, delay/count));
        }
    }

    private static void trydelete(String filename){
        File file = new File(filename);
        if(file.exists()){
            boolean flag = file.delete();
            System.out.println("delete "+filename);
        }
    }
}
