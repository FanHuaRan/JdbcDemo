package pers.fhr.jdbcutil.util;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Optional;
import java.util.Vector;
/**
 * 连接池实现 线程安全 只是学习 
 * @author fhr
 *
 */
public class JdbcConnectionPool implements IConnectionPool {
	// 初始化池大小，即一开始池中就会有10个连接对象
	private static final int INITIALSIZE = 10;
	// 最大连接数
	private static final int MAXSIZE = 50;
	// 最大空闲连接数
	private static final int MAXFREESIZE = 20;
	/// 数据库用户名
	private static final String USERNAME = "root";
	// 用户密码
	private static final String PASSWORD = "123";
	// 驱动名
	private static final String DRIVER = "com.mysql.jdbc.Driver";
	// 连接字符串
	private static final String URL = "jdbc:mysql://localhost:3306/musicstore";
	// 连接池容器
	private final Vector<ConnectionProxy> connectionProxys = new Vector<>();
	//利用静态代码块进行驱动加载
	static{
		try {
			Class.forName(DRIVER);
		} catch (Exception e) {
			System.out.println("驱动加载失败");
		}
	}
	//初始化连接池
	@Override
	public void initialPool() throws SQLException {
		for(int i=0;i<INITIALSIZE;i++){
			createConnectionProxy();
		}
	}
	/**
	 * 创建连接及其代理
	 * @return
	 * @throws SQLException
	 */
	private ConnectionProxy createConnectionProxy() throws SQLException {
		//同步
		synchronized (connectionProxys) {
			ConnectionProxy connectionProxy = new ConnectionProxy(DriverManager.getConnection(URL, USERNAME, PASSWORD));
			connectionProxys.addElement(createConnectionProxy());
			return connectionProxy;
		}
	}
	/**
	 * 获取连接
	 */
	@Override
	public Connection getConnection() throws SQLException {
			//这儿同步肯定死锁！！
			ConnectionProxy proxy = getCouldUseConnectionProxy();
			return proxy.getConnection();
	}
	/**
	 * 获取可以使用的连接代理
	 */
	@Override
	public ConnectionProxy getCouldUseConnectionProxy() throws SQLException {
			while (true) {
			{
				//利用connectionProxys实现同步策略
				synchronized (connectionProxys) {
					Optional<ConnectionProxy> proxyOptional = connectionProxys.stream().filter(p -> !p.isBusy())
							.findFirst();
					if (proxyOptional.isPresent()) {
						ConnectionProxy proxy=proxyOptional.get();
						proxy.setBusy(true);
						return proxyOptional.get();
					}
					if (connectionProxys.size() < MAXSIZE - 1) {
						return createConnectionProxy();
					}
				}
				//不能把下面区域放入同步块 否则会死锁！！
				try {
					Thread.sleep(250);
				} catch (InterruptedException e) {
					return null;
				}
			}
		}
	}
	/**
	 * 释放连接
	 */
	@Override
	public void releaseConnection(Connection connection) {
		//同步
		synchronized (connectionProxys) {
			Optional<ConnectionProxy> proxyOptional = connectionProxys
					.stream()
					.filter(p -> p.getConnection().equals(connection))
					.findFirst();
			if (proxyOptional != null) {
				ConnectionProxy proxy = proxyOptional.get();
				proxy.setBusy(false);
				if (staticsFreeConnectionNum() > MAXFREESIZE) {
					connectionProxys.remove(proxy);
				}
			}
		}
	}
	/**
	 * 统计空闲连接数
	 * @return
	 */
	private int staticsFreeConnectionNum() {
		//同步
		synchronized (connectionProxys) {
			return (int) connectionProxys.stream().filter(p -> !p.isBusy()).count();
		}
	}
}
