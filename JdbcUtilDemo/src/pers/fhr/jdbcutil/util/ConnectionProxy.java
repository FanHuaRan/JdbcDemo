package pers.fhr.jdbcutil.util;

import java.sql.Connection;

/**
 * 简易版的代理 
 * 没有按照经典模式而实现connection接口
 * 只含有connection引用
 * @author fhr
 *
 */
public class ConnectionProxy  {
	/**
	 * Fields
	 */
	private Connection connection=null;
	private boolean isBusy;
	/**
	 * getters setters
	 * @return
	 */
	public Connection getConnection() {
		return connection;
	}
	public boolean isBusy() {
		return isBusy;
	}
	public void setBusy(boolean isBusy) {
		this.isBusy = isBusy;
	}
	/**
	 * constructors
	 */
	public ConnectionProxy(Connection connection2){
		this(connection2, false);
	}
	public ConnectionProxy(Connection connection,boolean isBusy){
		this.connection=connection;
		this.isBusy=isBusy;
	}
}
