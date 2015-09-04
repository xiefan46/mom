package com.alibaba.middleware.race.mom.producer;

import java.util.concurrent.TimeUnit;

import io.netty.bootstrap.Bootstrap;
import io.netty.buffer.PooledByteBufAllocator;
import io.netty.channel.AdaptiveRecvByteBufAllocator;
import io.netty.channel.Channel;
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
import io.netty.util.concurrent.GenericFutureListener;

import org.apache.commons.pool.PoolableObjectFactory;
import org.apache.commons.pool.impl.GenericObjectPool;

import com.alibaba.middleware.race.mom.protobuf.ProtoMessage;





public class PoolableProducerConnectionFactory implements PoolableObjectFactory<ProducerConnection>{
	
	private static final int RECONNECT_TIME = 10;
	
	private final String host;
	
	private final int port;
	
	private GenericObjectPool<ProducerConnection> pool;
	
	private volatile Bootstrap bootstrap;
	
	private volatile EventLoopGroup group;
	
	private volatile boolean closed = false;
	
	
	public PoolableProducerConnectionFactory(String host,int port) throws Exception
	{
		this.host = host;
		this.port = port;
	}
	
	public void init() throws Exception
	{
		pool = new GenericObjectPool<ProducerConnection>(this);
		group = new NioEventLoopGroup();
		bootstrap = new Bootstrap();
		bootstrap.group(group).channel(NioSocketChannel.class)
		.option(ChannelOption.TCP_NODELAY, false)
		.option(ChannelOption.SO_KEEPALIVE, true)
		.option(ChannelOption.RCVBUF_ALLOCATOR, new AdaptiveRecvByteBufAllocator(64,80,65535))
		.option(ChannelOption.ALLOCATOR, PooledByteBufAllocator.DEFAULT)
		.handler(new ChannelInitializer<SocketChannel>() {
			@Override
			protected void initChannel(SocketChannel ch) throws Exception {
				/*
				 * 如果连接失败，则定时重连,暂时先默认10秒重连一次
				 */
				ch.pipeline().addFirst(new ChannelInboundHandlerAdapter(){
					@Override
                    public void channelInactive(ChannelHandlerContext ctx) throws Exception {
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
				ch.pipeline().addLast(new ProducerConnection());
				
			}
		});
		pool.setMaxIdle(200);
		pool.setMaxActive(200);
		pool.setTestOnBorrow(true);
		//pool.setMinIdle(40);
		for(int i=0;i<200;i++)
			pool.addObject();
	}
	
	private void doConnect() {
        if (closed) {
            return;
        }

        ChannelFuture future = bootstrap.connect(this.host,this.port);

        future.addListener(new ChannelFutureListener() {
            public void operationComplete(ChannelFuture f) throws Exception {
                if (f.isSuccess()) {
                    //System.out.println("producer connect success！ host : " + host + " port : " + port);
                } else {
                    System.out.println("producer connect fail! host : " + host + " port : " + port);
                    f.channel().eventLoop().schedule(new Runnable() {
						@Override
						public void run() {
							doConnect();
						}
					}, 10, TimeUnit.SECONDS);
                }
            }
        });
    }
	
	public ProducerConnection getConnection() throws Exception
	{
		return pool.borrowObject();
	}
	
	public void recycleConnection(ProducerConnection h) throws Exception
	{
		if(h != null) pool.returnObject(h);
	}
	
	@Override
	public ProducerConnection makeObject() throws Exception 
	{
		ChannelFuture future = bootstrap.connect(host, port).sync();
		future.addListener(new ChannelFutureListener() {
            public void operationComplete(ChannelFuture f) throws Exception {
                if (f.isSuccess()) {
                    //System.out.println("producer connect success！ host : " + host + " port : " + port);
                } else {
                    System.out.println("producer connect fail! host : " + host + " port : " + port);
                    f.channel().eventLoop().schedule(new Runnable() {
						@Override
						public void run() {
							doConnect();
						}
					}, 10, TimeUnit.SECONDS);
                }
            }
        });
		return future.channel().pipeline().get(ProducerConnection.class);
	}

	@Override
	public void destroyObject(ProducerConnection obj) throws Exception 
	{
		//System.out.println("一个Handler被销毁");
		//obj.close();
	}

	@Override
	public boolean validateObject(ProducerConnection obj) 
	{
		if(obj != null && obj.isActive()) return true;
		return false;
	}

	@Override
	public void activateObject(ProducerConnection obj) throws Exception 
	{		
		
	}

	@Override
	public void passivateObject(ProducerConnection obj) throws Exception 
	{		
	}
	
	public void clear()
	{
		pool.clear();
		this.closed = true;
	}
}
