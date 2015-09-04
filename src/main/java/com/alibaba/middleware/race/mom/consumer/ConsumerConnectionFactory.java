package com.alibaba.middleware.race.mom.consumer;

import java.util.concurrent.TimeUnit;

import io.netty.bootstrap.Bootstrap;
import io.netty.buffer.PooledByteBufAllocator;
import io.netty.channel.AdaptiveRecvByteBufAllocator;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.handler.codec.protobuf.ProtobufDecoder;
import io.netty.handler.codec.protobuf.ProtobufEncoder;
import io.netty.handler.codec.protobuf.ProtobufVarint32FrameDecoder;
import io.netty.handler.codec.protobuf.ProtobufVarint32LengthFieldPrepender;

import org.apache.commons.pool.impl.GenericObjectPool;

import com.alibaba.middleware.race.mom.Message;
import com.alibaba.middleware.race.mom.MessageListener;
import com.alibaba.middleware.race.mom.broker.SendMessageRequest;
import com.alibaba.middleware.race.mom.protobuf.ProtoMessage;

public class ConsumerConnectionFactory {
	
	private static final int RECONNECT_TIME = 3;
	
	private final String host;
	
	private final int port;
	
	private volatile Bootstrap bootstrap;
	
	private volatile EventLoopGroup group;
	
	private volatile boolean closed = false;
	
	private MessageListener listener;
	
	private Message subMsg;
	
	public ConsumerConnectionFactory(String host,int port,MessageListener listener,Message subMsg) throws Exception
	{
		this.host = host;
		this.port = port;
		this.listener = listener;
		this.subMsg = subMsg;
		init();
	}
	
	public void init() throws Exception
	{
		group = new NioEventLoopGroup();
		bootstrap = new Bootstrap();
		bootstrap.group(group).channel(NioSocketChannel.class)
		.option(ChannelOption.TCP_NODELAY, true)
		.option(ChannelOption.SO_KEEPALIVE, true)
		.option(ChannelOption.RCVBUF_ALLOCATOR, new AdaptiveRecvByteBufAllocator(64,80,65535))
		.option(ChannelOption.ALLOCATOR, PooledByteBufAllocator.DEFAULT)
		.handler(new ChannelInitializer<SocketChannel>() {
			@Override
			protected void initChannel(SocketChannel ch) throws Exception {
				ch.pipeline().addFirst(new ChannelInboundHandlerAdapter(){
					@Override
					public void channelInactive(ChannelHandlerContext ctx) throws Exception{
						super.channelInactive(ctx);
						ctx.channel().eventLoop().schedule(new Runnable() {					
							@Override
							public void run() {
								doConnect();		
							}
						}, RECONNECT_TIME, TimeUnit.SECONDS);
					}
				});
				ch.pipeline().addLast(new ProtobufVarint32FrameDecoder());
				ch.pipeline().addLast(new ProtobufDecoder(ProtoMessage.Message.getDefaultInstance()));
				ch.pipeline().addLast(new ProtobufVarint32LengthFieldPrepender());
				ch.pipeline().addLast(new ProtobufEncoder());
				ch.pipeline().addLast(new ConsumerConnection(listener,subMsg));
				
			}
		});
		doConnect();
	}
	
	private void doConnect()
	{
		try{
			if(closed){
				return;
			}
			ChannelFuture future = bootstrap.connect(this.host,this.port).sync();
			future.addListener(new ChannelFutureListener() {
				
				@Override
				public void operationComplete(ChannelFuture future) throws Exception {
					if(future.isSuccess()){			
						System.out.println("consumer connect success！ host : " + host + " port : " + port);
					}
					else{
						System.out.println("consumer connect fail! host : " + host + " port : " + port);
	                    future.channel().eventLoop().schedule(new Runnable() {
							@Override
							public void run() {
								doConnect();
							}
						}, RECONNECT_TIME, TimeUnit.SECONDS);
					}
				}
			});
		}catch(Exception e){
			e.printStackTrace();
		}
	}
	
	public void close()
	{
		this.closed = true;
		System.out.println("factory close");
		group.shutdownGracefully();
	}
	
	
}
