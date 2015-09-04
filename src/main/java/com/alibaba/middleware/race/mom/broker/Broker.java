package com.alibaba.middleware.race.mom.broker;

import io.netty.bootstrap.ServerBootstrap;
import io.netty.buffer.PooledByteBufAllocator;
import io.netty.channel.AdaptiveRecvByteBufAllocator;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.handler.codec.protobuf.ProtobufDecoder;
import io.netty.handler.codec.protobuf.ProtobufEncoder;
import io.netty.handler.codec.protobuf.ProtobufVarint32FrameDecoder;
import io.netty.handler.codec.protobuf.ProtobufVarint32LengthFieldPrepender;

import java.net.ServerSocket;
import java.net.Socket;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import com.alibaba.middleware.race.mom.protobuf.ProtoMessage;




public class Broker {
	
	private volatile EventLoopGroup bossGroup;
	
	private volatile EventLoopGroup workerGroup;
	
	private volatile ServerBootstrap bootstrap;
	
	private volatile boolean closed = false;
	
	private int port;
	
	public void start(int port)
	{
		this.port = port;
		bossGroup = new NioEventLoopGroup();
		workerGroup = new NioEventLoopGroup();
		bootstrap = new ServerBootstrap();
		bootstrap.group(bossGroup, workerGroup)
		 .channel(NioServerSocketChannel.class)
		 .option(ChannelOption.SO_BACKLOG,200)
		 .option(ChannelOption.TCP_NODELAY, false)
		 .option(ChannelOption.RCVBUF_ALLOCATOR, new AdaptiveRecvByteBufAllocator(64,80,65535))
		 .option(ChannelOption.ALLOCATOR, PooledByteBufAllocator.DEFAULT)
		 .childOption(ChannelOption.RCVBUF_ALLOCATOR, new AdaptiveRecvByteBufAllocator(64,80,65535))
		 .childOption(ChannelOption.ALLOCATOR, PooledByteBufAllocator.DEFAULT)
		 .childOption(ChannelOption.SO_SNDBUF, 1024)
		 .childOption(ChannelOption.SO_RCVBUF,1024)
		 .childHandler(new ChannelInitializer<SocketChannel>() {

			@Override
			protected void initChannel(SocketChannel ch) throws Exception {
				ch.pipeline().addLast(new ProtobufVarint32FrameDecoder());
				ch.pipeline().addLast(new ProtobufDecoder(ProtoMessage.Message.getDefaultInstance()));
				ch.pipeline().addLast(new ProtobufVarint32LengthFieldPrepender());
				ch.pipeline().addLast(new ProtobufEncoder());
				ch.pipeline().addLast(new BrokerHandler());
			}
		});
		doBind();
	}
	
	protected void doBind()
	{
		 if (closed) 
		 {
	        return;
	     }
		 bootstrap.bind(port).addListener(new ChannelFutureListener() {
	            @Override
	            public void operationComplete(ChannelFuture f) throws Exception {
	                if (f.isSuccess()) {
	                    System.out.println("Broker 启动: " + port);
	                } else {
	                    System.out.println("Broker 启动失败,每10秒尝试重新连接: " + port);
	                    
	                    /*
	                     * 如果监听端口失败，每10秒重试一次
	                     */
	                    f.channel().eventLoop().schedule(new Runnable() {					
							@Override
							public void run() {
								doBind();
							}
						}, 10, TimeUnit.SECONDS);
	                }
	            }
	        });
	       
	}
	
	public void close() {
        closed = true;
        bossGroup.shutdownGracefully();
        workerGroup.shutdownGracefully();
        System.out.println("Stopped Tcp Server: " + port);
    }
	
	public static void main(String[] args)
	{
		Broker broker = new Broker();
		broker.start(9999);
	}
}
