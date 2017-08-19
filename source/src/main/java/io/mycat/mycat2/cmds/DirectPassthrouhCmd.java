package io.mycat.mycat2.cmds;

import java.io.IOException;
import java.nio.channels.SocketChannel;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.mycat.mycat2.MySQLSession;
import io.mycat.mycat2.SQLCommand;
import io.mycat.mycat2.beans.MySQLPackageInf;
import io.mycat.mysql.packet.MySQLPacket;
import io.mycat.proxy.ProxyBuffer;

/**
 * 直接透传命令报文
 * 
 * @author wuzhihui
 *
 */
public class DirectPassthrouhCmd implements SQLCommand {
	
	private static final Logger logger = LoggerFactory.getLogger(DirectPassthrouhCmd.class);
	
	public static final DirectPassthrouhCmd INSTANCE = new DirectPassthrouhCmd();
	
	@Override
	public boolean procssSQL(MySQLSession session, boolean backresReceived) throws IOException {
		if(backresReceived){
			return backendProcessor(session,session.curFrontMSQLPackgInf,session.frontChannel,session.frontBuffer);
		}else{
			return frontProcessor(session);
		}
	}
	
	/**
	 * 前端报文处理
	 * @param session
	 * @return
	 * @throws IOException
	 */
	private boolean frontProcessor(MySQLSession session) throws IOException{
		ProxyBuffer curBuffer = session.frontBuffer;
		SocketChannel curChannel = session.backendChannel;
		// 直接透传报文
		curBuffer.changeOwner(!curBuffer.frontUsing());
		curBuffer.readIndex = curBuffer.writeIndex;
		session.writeToChannel(curBuffer, curChannel);
		session.modifySelectKey();
		return false;
	}
	
	/**
	 * 后端报文处理
	 * @param session
	 * @return
	 * @throws IOException
	 */
	private boolean backendProcessor(MySQLSession session,MySQLPackageInf curMSQLPackgInf,
			SocketChannel curChannel,ProxyBuffer curBuffer)throws IOException{
		
		if(!session.readFromChannel(session.frontBuffer, session.backendChannel)){
			return false;
		}

		boolean isContinue = true;
		do{
			switch(session.resolveMySQLPackage(curBuffer, curMSQLPackgInf,true)){
				case Full:
					if(curBuffer.readIndex < curBuffer.writeIndex){  //还有数据报文没有处理完,继续处理下一个数据报文
						isContinue = true;
					}else if(curBuffer.readIndex == curBuffer.writeIndex){  // 最后一个整包,一定出现在这里。出现在这里的 也有可能是当前buffer的最后一个包
						curMSQLPackgInf.crossBuffer = true;
						isContinue = false;
					}
					break;
				case LongHalfPacket:			
					if(curMSQLPackgInf.crossBuffer){
						//发生过透传的半包,往往包的长度超过了buffer 的长度.
						logger.debug(" readed crossBuffer LongHalfPacket ,curMSQLPackgInf is {}", curMSQLPackgInf);
					}else if(!isfinishPackage(curMSQLPackgInf)){
						//不需要整包解析的长半包透传. result set  .这种半包直接透传
						curMSQLPackgInf.crossBuffer=true;
						curBuffer.readIndex = curMSQLPackgInf.endPos;
						curMSQLPackgInf.remainsBytes = curMSQLPackgInf.pkgLength-(curMSQLPackgInf.endPos - curMSQLPackgInf.startPos);
						logger.debug(" readed LongHalfPacket ,curMSQLPackgInf is {}", curMSQLPackgInf);
						logger.debug(" curBuffer {}", curBuffer);
					}else{
						// 读取到了EOF/OK/ERROR 类型长半包  是需要保证是整包的.
						logger.debug(" readed finished LongHalfPacket ,curMSQLPackgInf is {}", curMSQLPackgInf);
						// TODO  保证整包的机制
					}
					isContinue = false;
					break;
				case ShortHalfPacket:
					logger.debug(" readed ShortHalfPacket ,curMSQLPackgInf is {}", curMSQLPackgInf);
					isContinue = false;
					break;
			}
		}while(isContinue);
		
		// 直接透传报文
		session.writeToChannel(curBuffer, curChannel);
		/**
		* 当前命令处理是否全部结束,全部结束时需要清理资源
		*/
		return false;
	}
	
	private boolean isfinishPackage(MySQLPackageInf curMSQLPackgInf)throws IOException{
		switch(curMSQLPackgInf.pkgType){
		case MySQLPacket.OK_PACKET:
		case MySQLPacket.ERROR_PACKET:
		case MySQLPacket.EOF_PACKET:
			return true;
		default:
			return false;
		}
	}

	@Override
	public void clearResouces(boolean sessionCLosed) {
		// TODO Auto-generated method stub
		
	}

}
