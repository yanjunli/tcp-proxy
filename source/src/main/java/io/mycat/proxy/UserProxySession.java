package io.mycat.proxy;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.ClosedChannelException;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.SocketChannel;

/**
 * 代表用户的会话，存放用户会话数据，如前端连接，后端连接，状态等数据
 * 
 * @author wuzhihui
 *
 */
public class UserProxySession extends AbstractSession {
	public ProxyBuffer frontBuffer;

	// 后端连接
	public String backendAddr;
	public SocketChannel backendChannel;
	public SelectionKey backendKey;

	public UserProxySession(BufferPool bufferPool, Selector selector, SocketChannel channel) throws IOException {
		super(bufferPool, selector, channel);
		frontBuffer = new ProxyBuffer(bufPool.allocByteBuffer());
	}

	/**
	 * 从SocketChannel中读取数据并写入到内部Buffer中,writeState里记录了写入的位置指针
	 * 第一次调用之前需要确保Buffer状态为Write状态，并指定要写入的位置，
	 * 
	 * @param channel
	 * @return 读取了多少数据
	 */
	public boolean readFromChannel(ProxyBuffer proxyBuf, SocketChannel channel) throws IOException {

		ByteBuffer buffer = proxyBuf.getBuffer();
		proxyBuf.compact();
		int readed = channel.read(buffer);
		logger.debug(" readed {} total bytes ", readed);
		if (readed == -1) {
			logger.warn("Read EOF ,socket closed ");
			throw new ClosedChannelException();
		} else if (readed == 0) {

			logger.warn("readed zero bytes ,Maybe a bug ,please fix it !!!!");
		}
		proxyBuf.writeIndex = buffer.position();
		return readed > 0;
	}

	/**
	 * 从内部Buffer数据写入到SocketChannel中发送出去，readState里记录了写到Socket中的数据指针位置 方法，
	 * 
	 * @param channel
	 */
	public void writeToChannel(ProxyBuffer proxyBuf, SocketChannel channel) throws IOException {
		ByteBuffer buffer = proxyBuf.getBuffer();
		buffer.position(0);
		buffer.limit(proxyBuf.readIndex);
		int writed = channel.write(buffer);
		if(writed <=0||buffer.hasRemaining()){
			/**
			 * 1. writed <0 时,尝试注册可写事件,注册失败时，需要关闭连接。
			 *    也可能发生在写的过程中 ，连接断开的情况.
			 *    这种情况下会报异常
			 * 2. writed==0 或者  buffer 中数据没有写完时,注册可写事件
			 *    通常发生在网络阻塞或者 客户端  COM_STMT_FETCH 命令可能会 出现没有写完或者 writed == 0 的情况
			 */
			keepWrite(channel);
			logger.info("register OP_WRITE  selectkey .write  {} bytes. current channel is {}",writed,channel);
			int readindex = proxyBuf.readIndex - writed;
			proxyBuf.readIndex = writed;
			proxyBuf.compact();
			proxyBuf.readIndex = readindex;
			return;
		}else{
			logger.info("writeToChannel write  {} bytes ",writed);
			/*
			 * 数据写完时,切换NIO 事件
			 */
			//从写状态切换到读状态时,需要检查对端 是否有注册可读事件
			modifySelectKey();
//			inReadState();  //TODO bug 持续写入完成后,需要给对端注册读状态
		}
	}
	
	/**
	 * 没有写完.或socket buffer 满了。。。注册可写事件.取消对端可读事件
	 * @param channel
	 * @throws IOException
	 */
	private void keepWrite(SocketChannel channel)throws IOException{
		SelectionKey theKey = channel.equals(frontChannel) ? frontKey : backendKey;
		SelectionKey otherKey = channel.equals(frontChannel) ? backendKey : frontKey;
		logger.info("otherKey  interestOps is {} bytes ",otherKey.interestOps());
		
		theKey.interestOps(theKey.interestOps() | SelectionKey.OP_WRITE);
		otherKey.interestOps(otherKey.interestOps() & ~SelectionKey.OP_READ);
	}

	/**
	 * 手动创建的ProxyBuffer需要手动释放，recycleAllocedBuffer()
	 * 
	 * @return ProxyBuffer
	 */
	public ProxyBuffer allocNewProxyBuffer() {
		logger.info("alloc new ProxyBuffer ");
		return new ProxyBuffer(bufPool.allocByteBuffer());
	}

	/**
	 * 释放手动分配的ProxyBuffer
	 * 
	 * @param curFrontBuffer
	 */
	public void recycleAllocedBuffer(ProxyBuffer curFrontBuffer) {
		logger.info("recycle alloced ProxyBuffer ");

		if (curFrontBuffer != null) {
			this.bufPool.recycleBuf(curFrontBuffer.getBuffer());
		}
	}

	public boolean isBackendOpen() {
		return backendChannel != null && backendChannel.isConnected();
	}

	public String sessionInfo() {
		return " [" + this.frontAddr + "->" + this.backendAddr + ']';
	}

	@SuppressWarnings("rawtypes")
	public void lazyCloseSession(final boolean normal,final String reason) {
		if (isClosed()) {
			return;
		}

		ProxyRuntime.INSTANCE.addDelayedNIOJob(() -> {
			if (!isClosed()) {
				close(normal,reason);
			}
		}, 10, (ProxyReactorThread) Thread.currentThread());
	}

	public void close(boolean normal,String hint){
		if (!this.isClosed()) {
			bufPool.recycleBuf(frontBuffer.getBuffer());
			// 关闭后端连接
			closeSocket(backendChannel,normal,hint);
			super.close(normal,hint);
		} else {
			super.close(normal,hint);
		}

	}

	public boolean hasDataTrans2Backend() {
		return frontBuffer.backendUsing() && frontBuffer.isInReading();
	}

	@SuppressWarnings({ "unchecked", "rawtypes" })
	public void closeSocket(SocketChannel channel, boolean normal, String msg) {
		super.closeSocket(channel, normal, msg);
		if (channel == frontChannel) {
			((FrontIOHandler) getCurNIOHandler()).onFrontSocketClosed(this, normal);
			frontChannel = null;
		} else if (channel == backendChannel) {
			((BackendIOHandler) getCurNIOHandler()).onBackendSocketClosed(this, normal);
			backendChannel = null;
		}
	}

	public void modifySelectKey() throws ClosedChannelException {
		SelectionKey theKey = this.frontBuffer.frontUsing() ? frontKey : backendKey;
		if (theKey != null && theKey.isValid()) {
			int clientOps = SelectionKey.OP_READ;
			if (frontBuffer.isInWriting() == false) {
				clientOps = SelectionKey.OP_WRITE;
			}
			int oldOps = theKey.interestOps();
			if (oldOps != clientOps) {
				theKey.interestOps(clientOps);
			}
			logger.info("current selectkey is {} ",clientOps);
		}
	}
	
	/**
	 * 从对端读数据到本端
	 * 向对端注册可读事件
	 * @param channel
	 * @throws IOException
	 */
	private void inReadState()throws ClosedChannelException{
		SelectionKey otherKey = this.frontBuffer.frontUsing() ? backendKey : frontKey;
		if (otherKey != null && otherKey.isValid()) {
			int oldOps = otherKey.interestOps();
			if ( ( oldOps & SelectionKey.OP_READ ) == 0) {
				otherKey.interestOps(oldOps|SelectionKey.OP_READ);
			}
		}
	}
}
