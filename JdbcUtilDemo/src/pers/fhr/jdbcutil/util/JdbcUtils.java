package pers.fhr.jdbcutil.util;

import java.lang.reflect.Field;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import pers.fhr.jdbcutil.model.DataColumn;
import pers.fhr.jdbcutil.model.DataRow;
import pers.fhr.jdbcutil.model.DataTable;
/**
 * 方法较多 很多地方可以考虑使用模板方法进行优化扩展
 * @author FHR
 * @date 2017/3/5
 */
public class JdbcUtils {
	/// 数据库用户名
	private static final String USERNAME = "root";
	// 用户密码
	private static final String PASSWORD = "123";
	// 驱动名
	private static final String DRIVER = "com.mysql.jdbc.Driver";
	// 连接字符串
	private static final String URL = "jdbc:mysql://localhost:3306/musicstore";
	
	//利用静态代码块进行驱动加载
	static {
		try {
			Class.forName(DRIVER);
		} catch (ClassNotFoundException e) {
			System.out.println("驱动加载失败");
			e.printStackTrace();
		}
	}
	/**
	 * 更新数据库记录
	 * @param sql
	 * @param params
	 * @return
	 * @throws SQLException
	 */
	public static boolean updateByPreparedStatement(String sql, List<Object> params) throws SQLException {
		Connection connection = getConnection();
		PreparedStatement pstmt = createNormalPreparedStatement(sql, params, connection);
		int result = pstmt.executeUpdate();
		releaseResources(connection, null, pstmt);
		boolean flag = result > 0 ? true : false;
		return flag;
	}
	/**
	 * 查找单个记录
	 * @param sql
	 * @param params
	 * @return
	 * @throws SQLException
	 */
	public static Map<String, Object> findSingleResultByMap(String sql, List<Object> params) throws SQLException {
		Connection connection = getConnection();
		PreparedStatement pstmt = createNormalPreparedStatement(sql, params, connection);
		ResultSet resultSet = pstmt.executeQuery();
		Map<String, Object> map = null;
		ResultSetMetaData metaData = resultSet.getMetaData();
		int col_len = metaData.getColumnCount();
		while (resultSet.next()) {
			map = getSingleHashMap(resultSet, metaData, col_len);
		}
		releaseResources(connection, resultSet, pstmt);
		return map;
	}
	/**
	 * 查找多个记录
	 * @param sql
	 * @param params
	 * @return
	 * @throws SQLException
	 */
	public static List<Map<String, Object>> findMoreResultByMap(String sql, List<Object> params) throws SQLException {
		Connection connection = getConnection();
		PreparedStatement pstmt = createNormalPreparedStatement(sql, params, connection);
		ResultSet resultSet = pstmt.executeQuery();
		ResultSetMetaData metaData = resultSet.getMetaData();
		int cols_len = metaData.getColumnCount();
		List<Map<String, Object>> list = new ArrayList<>();
		while (resultSet.next()) {
			list.add(getSingleHashMap(resultSet, metaData, cols_len));
		}
		releaseResources(connection, resultSet, pstmt);
		return list;
	}
	/**
	 * 更新记录
	 * @param tableName
	 * @param rowValues
	 * @param primaryKey
	 * @param id
	 * @return
	 * @throws SQLException
	 */
	public static boolean updateRecordByMap(String tableName, Map<String, Object> rowValues, String primaryKey, int id)
			throws SQLException {
		Object[] keys = rowValues.keySet().toArray();
		String sql = createMapUpdateSql(tableName, primaryKey, keys);
		Connection connection = getConnection();
		PreparedStatement pstmt = connection.prepareStatement(sql);
		for (int i = 0; i < keys.length; i++) {
			pstmt.setObject(i++, rowValues.get(keys[i]));
		}
		pstmt.setObject(keys.length + 1, id);
		int resultCount = pstmt.executeUpdate();
		releaseResources(connection, null, pstmt);
		return resultCount > 0 ? true : false;
	}

	/**
	 * 插入记录
	 * @param tableName
	 * @param rowValues
	 * @return
	 * @throws SQLException
	 */
	public static boolean insertRecordByMap(String tableName, Map<String, Object> rowValues) throws SQLException {
		Object[] keys = rowValues.keySet().toArray();
		String sql = createMapInsertSql(tableName, keys);
		Connection connection = getConnection();
		PreparedStatement pstmt = connection.prepareStatement(sql);
		for (int i = 0; i < rowValues.size(); i++) {
			pstmt.setObject(i + 1, rowValues.get(keys[i]));
		}
		int resultCount = pstmt.executeUpdate();
		releaseResources(connection, null, pstmt);
		return resultCount > 0 ? true : false;
	}
	/**
	 * 删除记录
	 * @param tableName
	 * @param primaryKey
	 * @param id
	 * @return
	 * @throws SQLException
	 */
	public static boolean deleteRecord(String tableName, String primaryKey, int id) throws SQLException {
		Connection connection = getConnection();
		String sql = createNormalDeleteSql(tableName, primaryKey);
		PreparedStatement pstmt = connection.prepareStatement(sql);
		pstmt.setObject(1, id);
		int resultCount = pstmt.executeUpdate();
		releaseResources(connection, null, pstmt);
		return resultCount > 0 ? true : false;
	}
	/**
	 * 通过反射查找单个记录
	 * @param sql
	 * @param params
	 * @param cls
	 * @return
	 * @throws Exception
	 */
	public static <T> T findSingleResultByObject(String sql, List<Object> params, Class<T> cls) throws Exception {
		Connection connection = getConnection();
		PreparedStatement pstmt = createNormalPreparedStatement(sql, params, connection);
		ResultSet resultSet = pstmt.executeQuery();
		resultSet = pstmt.executeQuery();
		T resultObject = null;
		ResultSetMetaData metaData = resultSet.getMetaData();
		int cols_len = metaData.getColumnCount();
		while (resultSet.next()) {
			resultObject = getSingleObject(cls, resultSet, metaData, cols_len);
		}
		releaseResources(connection, resultSet, pstmt);
		return resultObject;
	}
	/**
	 * 通过反射查找多个记录
	 * @param sql
	 * @param params
	 * @param cls
	 * @return
	 * @throws Exception
	 */
	public static <T> List<T> findMoreResultByObject(String sql, List<Object> params, Class<T> cls) throws Exception {
		Connection connection = getConnection();
		PreparedStatement pstmt = createNormalPreparedStatement(sql, params, connection);
		ResultSet resultSet = pstmt.executeQuery();
		resultSet = pstmt.executeQuery();
		List<T> list = new ArrayList<>();
		ResultSetMetaData metaData = resultSet.getMetaData();
		int cols_len = metaData.getColumnCount();
		while (resultSet.next()) {
			list.add(getSingleObject(cls, resultSet, metaData, cols_len));
		}
		releaseResources(connection, resultSet, pstmt);
		return list;
	}
	/**
	 * 通过反射更新记录
	 * @param object
	 * @param primaryKeyName
	 * @return
	 * @throws IllegalArgumentException
	 * @throws IllegalAccessException
	 * @throws SQLException
	 * @throws NoSuchFieldException
	 * @throws SecurityException
	 */
	public static <T> boolean updateRecordByObject(T object, String primaryKeyName) throws IllegalArgumentException,
			IllegalAccessException, SQLException, NoSuchFieldException, SecurityException {
		@SuppressWarnings("unchecked")
		Class<T> cls = (Class<T>) object.getClass();
		Field[] fileds = cls.getDeclaredFields();
		Field primaryField = cls.getField(primaryKeyName);
		String sql = createObjectUpdateSql(primaryKeyName, cls, fileds);
		Connection connection = getConnection();
		PreparedStatement pstmt = connection.prepareStatement(sql);
		for (int i = 0; i < fileds.length; i++) {
			pstmt.setObject(i + 1, fileds[i].get(object));
		}
		pstmt.setObject(fileds.length + 1, primaryField.get(object));
		int resultCount = pstmt.executeUpdate();
		releaseResources(connection, null, pstmt);
		return resultCount > 0 ? true : false;
	}
	/**
	 * 通过反射插入记录
	 * @param object
	 * @return
	 * @throws IllegalArgumentException
	 * @throws IllegalAccessException
	 * @throws SQLException
	 */
	public static <T> boolean insertRecordByObject(T object)
			throws IllegalArgumentException, IllegalAccessException, SQLException {
		@SuppressWarnings("unchecked")
		Class<T> cls = (Class<T>) object.getClass();
		Field[] fileds = cls.getDeclaredFields();
		String sql = createObjectInsertSql(object, fileds);
		Connection connection = getConnection();
		PreparedStatement pstmt = connection.prepareStatement(sql);
		for (int i = 0; i < fileds.length; i++) {
			pstmt.setObject(i + 1, fileds[i].get(object));
		}
		int resultCount = pstmt.executeUpdate();
		releaseResources(connection, null, pstmt);
		return resultCount > 0 ? true : false;
	}
	/**
	 * 通过自定义datatable查找记录
	 * @param sql
	 * @param params
	 * @return
	 * @throws SQLException
	 */
	public static DataTable findResultByDataTable(String sql, List<Object> params) throws SQLException {
		Connection connection = getConnection();
		PreparedStatement pstmt = createNormalPreparedStatement(sql, params, connection);
		ResultSet resultSet = pstmt.executeQuery();
		ResultSetMetaData metaData = resultSet.getMetaData();
		DataTable dataTable = new DataTable(metaData.getColumnLabel(1));
		int cols_len = metaData.getColumnCount();
		getColumns(metaData, dataTable, cols_len);
		while (resultSet.next()) {
			getDataRow(resultSet, metaData, dataTable, cols_len);
		}
		releaseResources(connection, resultSet, pstmt);
		return dataTable;
	}
	/*****************************私有方法*******************************/
	/**
	 * 获取连接对象
	 * @return
	 * @throws SQLException 
	 */
	public static Connection getConnection() throws SQLException {
		return DriverManager.getConnection(URL, USERNAME, PASSWORD);
	}
	/**
	 *  释放相关资源：数据库连接、数据集和处理对象
	 * @param connection
	 * @param resultSet
	 * @param pstmt
	 */
	private static void releaseResources(Connection connection, ResultSet resultSet, PreparedStatement pstmt) {
		releaseConnection(connection);
		releasePrepardStatement(pstmt);
		releaseResultSet(resultSet);
	}
   /**
    * 释放连接
    * @param connection
    */
	private static void releaseConnection(Connection connection) {
		try {
			if (connection != null && !connection.isClosed()) {
				connection.close();
			}
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}
   /**
    * 释放resultset
    * @param resultSet
    */
	private static void releaseResultSet(ResultSet resultSet) {
		try {
			if (resultSet != null && !resultSet.isClosed()) {
				resultSet.close();
			}
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}
  /**
   * 释放PreparedStatement
   * @param pstmt
   */
	private static void releasePrepardStatement(PreparedStatement pstmt) {
		try {
			if (pstmt != null && !pstmt.isClosed()) {
				pstmt.close();
			}
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}
	/**
	 * 根据对象创建insert sql语句
	 * @param object
	 * @param fileds
	 * @return
	 */
	private static <T> String createObjectInsertSql(T object, Field[] fileds) {
		StringBuilder builder = new StringBuilder("insert into  " + object.getClass().getSimpleName() + "(");
		StringBuilder valueBuilder = new StringBuilder("(");
		for (int i = 0; i < fileds.length; i++) {
			Field filed = fileds[i];
			String collumName = filed.getName();
			if (i != fileds.length - 1) {
				builder.append(collumName + ",");
				valueBuilder.append("?,");
			} else {
				builder.append(collumName + ") values ");
				valueBuilder.append("?)");
			}
		}
		builder.append(valueBuilder);
		return builder.toString();
	}
   /**
    * 获取一行数据 datarow
    * @param resultSet
    * @param metaData
    * @param dataTable
    * @param cols_len
    * @throws SQLException
    */
	private static void getDataRow(ResultSet resultSet, ResultSetMetaData metaData, DataTable dataTable, int cols_len)
			throws SQLException {
		for (int i = 0; i < cols_len; i++) {
			String cols_name = metaData.getColumnName(i + 1);
			Object cols_value = resultSet.getObject(cols_name);
			DataRow row = dataTable.newRow();
			row.setValue(cols_name, cols_value);
		}
	}
	/**
	 * 获取数据列集合
	 * @param metaData
	 * @param dataTable
	 * @param cols_len
	 * @throws SQLException
	 */
	private static void getColumns(ResultSetMetaData metaData, DataTable dataTable, int cols_len) throws SQLException {
		for (int j = 0; j < cols_len; j++) {
			String name = metaData.getColumnName(j + 1);
			// String typeName = metaData.getColumnTypeName(j+1);
			int typeId = metaData.getColumnType(j + 1);
			DataColumn column = new DataColumn(name, typeId);
			dataTable.addColumn(column);
		}
	}
	/**
	 * 根据sql和参数数组创建PreparedStatement
	 * @param sql
	 * @param params
	 * @param connection
	 * @return
	 * @throws SQLException
	 */
	private static PreparedStatement createNormalPreparedStatement(String sql, List<Object> params,
			Connection connection) throws SQLException {
		PreparedStatement pstmt = connection.prepareStatement(sql);
		if (params != null && !params.isEmpty()) {
			for (int i = 0; i < params.size(); i++) {
				pstmt.setObject(i++, params.get(i));
			}
		}
		return pstmt;
	}
	/**
	 * 根据keys创建insert sql语句
	 * @param tableName
	 * @param keys
	 * @return
	 */
	private static String createMapInsertSql(String tableName, Object[] keys) {
		StringBuilder builder = new StringBuilder();
		builder.append("insert  into " + tableName + " ( ");
		StringBuilder valueBuilder = new StringBuilder("(");
		for (int i = 0; i < keys.length; i++) {
			if (i != keys.length - 1) {
				builder.append(keys[i] + ",");
				valueBuilder.append("?,");
			} else {
				builder.append(keys[i] + ") values ");
				valueBuilder.append("?)");
			}
		}
		builder.append(valueBuilder);
		String sql = builder.toString();
		return sql;
	}
	/**
	 * 根据tablename、主键字段名和keys创建修改 sql
	 * @param tableName
	 * @param primaryKey
	 * @param keys
	 * @return
	 */
	private static String createMapUpdateSql(String tableName, String primaryKey, Object[] keys) {
		StringBuilder builder = new StringBuilder();
		builder.append("update  " + tableName + " set ");
		for (int i = 0; i < keys.length; i++) {
			builder.append(keys[i] + "=?");
			if (i != keys.length - 1) {
				builder.append(",");
			}
		}
		builder.append(" where " + primaryKey + "=?");
		String sql = builder.toString();
		return sql;
	}
	/**
	 * 创建删除sql
	 * @param tableName
	 * @param primaryKey
	 * @return
	 */
	private static String createNormalDeleteSql(String tableName, String primaryKey) {
		StringBuilder builder = new StringBuilder("delete from " + tableName + " ");
		builder.append(" where " + primaryKey + "=?");
		String sql = builder.toString();
		return sql;
	}
	/**
	 * 根据对象创建修改sql
	 * @param primaryKeyName
	 * @param cls
	 * @param fileds
	 * @return
	 */
	private static <T> String createObjectUpdateSql(String primaryKeyName, Class<T> cls, Field[] fileds) {
		StringBuilder builder = new StringBuilder("update " + cls.getSimpleName() + " set ");
		for (int i = 0; i < fileds.length; i++) {
			Field filed = fileds[i];
			String name = filed.getName();
			builder.append(name + "=?");
			if (i != fileds.length - 1) {
				builder.append(",");
			}
		}
		builder.append(" where " + primaryKeyName + "=?");
		return builder.toString();
	}
	/**
	 * 获取一个map 也就是一行
	 * @param resultSet
	 * @param metaData
	 * @param col_len
	 * @return
	 * @throws SQLException
	 */
	private static Map<String, Object> getSingleHashMap(ResultSet resultSet, ResultSetMetaData metaData, int col_len)
			throws SQLException {
		Map<String, Object> map = new HashMap<>();
		for (int i = 0; i < col_len; i++) {
			String cols_name = metaData.getColumnName(i + 1);
			Object cols_value = resultSet.getObject(cols_name);
			map.put(cols_name, cols_value);
		}
		return map;
	}
	/**
	 * 应用反射创建一个对象 一行
	 * @param cls
	 * @param resultSet
	 * @param metaData
	 * @param cols_len
	 * @return
	 * @throws InstantiationException
	 * @throws IllegalAccessException
	 * @throws SQLException
	 * @throws NoSuchFieldException
	 */
	private static <T> T getSingleObject(Class<T> cls, ResultSet resultSet, ResultSetMetaData metaData, int cols_len)
			throws InstantiationException, IllegalAccessException, SQLException, NoSuchFieldException {
		T resultObject = cls.newInstance();
		for (int i = 0; i < cols_len; i++) {
			String cols_name = metaData.getColumnName(i + 1);
			Object cols_value = resultSet.getObject(cols_name);
			Field field = cls.getDeclaredField(cols_name);
			field.setAccessible(true);
			field.set(resultObject, cols_value);
		}
		return resultObject;
	}
}
