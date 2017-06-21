package pers.fhr.jdbcutil.util;

import java.lang.reflect.Field;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import pers.fhr.jdbcutil.model.DataColumn;
import pers.fhr.jdbcutil.model.DataRow;
import pers.fhr.jdbcutil.model.DataTable;
/**
 * jdbc封装，
 * 对资源的释放大量采用try_with-resources,少部分采用finally，不统一是个代码坏味，暂时不改了
 * 还没写多：
 * 1.数据集的封装方法
 * 2.同时支持返回和不返回的方法
 * @author fhr
 * @date 2017/03/05
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
	 * 获取连接对象
	 * @return
	 * @throws SQLException 
	 */
	public static Connection getConnection() throws SQLException {
		return DriverManager.getConnection(URL, USERNAME, PASSWORD);
	}
	/**
	 * 查找单个记录
	 * @param sql
	 * @param params
	 * @return
	 * @throws SQLException
	 */
	public static Map<String, Object> findSingleResultByMap(String sql, List<Object> params) throws SQLException {
		try (Connection connection = getConnection();
				PreparedStatement pstmt = createNormalPreparedStatement(sql, params, connection);
				ResultSet resultSet = pstmt.executeQuery()) {
			Map<String, Object> map = null;
			ResultSetMetaData metaData = resultSet.getMetaData();
			int col_len = metaData.getColumnCount();
			while (resultSet.next()) {
				map = getSingleHashMap(resultSet, metaData, col_len);
				break;
			}
			return map;
		}
	}
	/**
	 * 查找多个记录
	 * @param sql
	 * @param params
	 * @return
	 * @throws SQLException
	 */
	public static List<Map<String, Object>> findMoreResultByMap(String sql, List<Object> params) throws SQLException {
		try (Connection connection = getConnection();
				PreparedStatement pstmt = createNormalPreparedStatement(sql, params, connection);
				ResultSet resultSet = pstmt.executeQuery()) {
			ResultSetMetaData metaData = resultSet.getMetaData();
			int cols_len = metaData.getColumnCount();
			List<Map<String, Object>> list = new ArrayList<>();
			while (resultSet.next()) {
				list.add(getSingleHashMap(resultSet, metaData, cols_len));
			}
			return list;
		}
	}
	/**
	 * 通过map插入或者更新记录 主键存在则更新记录，否则插入记录返回自增主键
	 * @param tableName
	 * @param fields
	 * @param pkName
	 * @param id
	 * @return 返回主键
	 * @throws SQLException
	 */
	public static Object insertOrUpdateRecordByMap(String tableName,Map<String,Object> fields,String pkName,Object id) throws SQLException{
		//id不为null且通过id查询记录存在则更新记录
		if(id!=null&&recordExist(tableName, pkName, id)){
			updateRecordByMap(tableName, fields, pkName, id);
			return id;
		}else{//插入记录
			return insertRecordByMap(tableName, fields);
		}
	}
	
	/**
	 * 通过map和主键修改单个记录 返回是否成功标识
	 * @param tableName
	 * @param fields
	 * @param pkName
	 * @param id
	 * @return
	 * @throws SQLException
	 */
	public static boolean updateRecordByMap(String tableName, Map<String, Object> fields, String pkName, Object id)
			throws SQLException {
		// 字段名集合
		Set<String> filedNames = fields.keySet();
		// 更新记录的sql语句
		String sql = createUpdateSqlByMap(tableName, pkName, filedNames);
		try (Connection connection = getConnection(); PreparedStatement pstmt = connection.prepareStatement(sql)) {
			// 设置参数
			int index = 1;
			for (String fieldName : filedNames) {
				pstmt.setObject(index++, fields.get(fieldName));
			}
			pstmt.setObject(index, id);
			int resultCount = pstmt.executeUpdate();
			// 受影响行数>0返回true
			return resultCount > 0 ? true : false;
		}
	}

	/**
	 * 通过map插入记录 返回主键
	 * @param tableName
	 * @param rowValues
	 * @return
	 * @throws SQLException
	 */
	public static Object insertRecordByMap(String tableName, Map<String, Object> fields) throws SQLException {
		// 字段名集合
		Set<String> filedNames = fields.keySet();
		// 插入记录的sql语句
		String sql = createInsertSqlByMap(tableName, filedNames);
		ResultSet resultSet = null;
		try (Connection connection = getConnection();
				PreparedStatement pstmt = connection.prepareStatement(sql, Statement.RETURN_GENERATED_KEYS);) {
			// 设置参数
			int index = 1;
			for (String fieldName : filedNames) {
				pstmt.setObject(index++, fields.get(fieldName));
			}
			pstmt.execute();
			// 获取主键
			resultSet = pstmt.getGeneratedKeys();
			Object key = null;
			if (resultSet.next()) {
				key = resultSet.getObject(1);
			}
			return key;
		} finally {
			releaseResultSet(resultSet);
		}
	}
	/**
	 * 检查主键是否存在
	 * @param connection
	 * @param tableName
	 * @param pkName
	 * @param id
	 * @return 
	 * @throws SQLException
	 */
	public static boolean recordExist(String tableName,String pkName,Object id) throws SQLException{
		ResultSet resultSet = null;
		try (Connection connection = getConnection();
				PreparedStatement pstmt = connection
						.prepareStatement(String.format("select %s from %s where %s =? ", pkName, tableName, pkName))) {
			pstmt.setObject(1, id);
			resultSet = pstmt.executeQuery();
			return resultSet.next();
		} finally {
			releaseResultSet(resultSet);
		}
	}
	/**
	 * 删除记录
	 * @param tableName
	 * @param pkName
	 * @param id
	 * @return
	 * @throws SQLException
	 */
	public static boolean deleteRecord(String tableName, String pkName, int id) throws SQLException {
		String sql = createNormalDeleteSql(tableName, pkName);
		try (Connection connection = getConnection(); PreparedStatement pstmt = connection.prepareStatement(sql)) {
			pstmt.setObject(1, id);
			int resultCount = pstmt.executeUpdate();
			return resultCount > 0 ? true : false;
		}
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
		ResultSet resultSet = null;
		try (Connection connection = getConnection();
				PreparedStatement pstmt = createNormalPreparedStatement(sql, params, connection)) {
			resultSet = pstmt.executeQuery();
			T resultObject = null;
			ResultSetMetaData metaData = resultSet.getMetaData();
			int cols_len = metaData.getColumnCount();
			while (resultSet.next()) {
				resultObject = getSingleObject(cls, resultSet, metaData, cols_len);
				break;
			}
			return resultObject;
		} finally {
			releaseResultSet(resultSet);
		}
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
		ResultSet resultSet = null;
		try (Connection connection = getConnection();
				PreparedStatement pstmt = createNormalPreparedStatement(sql, params, connection)) {
			resultSet = pstmt.executeQuery();
			List<T> list = new ArrayList<>();
			ResultSetMetaData metaData = resultSet.getMetaData();
			int cols_len = metaData.getColumnCount();
			while (resultSet.next()) {
				list.add(getSingleObject(cls, resultSet, metaData, cols_len));
			}
			return list;
		} finally {
			releaseResultSet(resultSet);
		}
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
		try (Connection connection = getConnection(); PreparedStatement pstmt = connection.prepareStatement(sql)) {
			for (int i = 0; i < fileds.length; i++) {
				pstmt.setObject(i + 1, fileds[i].get(object));
			}
			pstmt.setObject(fileds.length + 1, primaryField.get(object));
			int resultCount = pstmt.executeUpdate();
			return resultCount > 0 ? true : false;
		}
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
		try (Connection connection = getConnection(); PreparedStatement pstmt = connection.prepareStatement(sql)) {
			for (int i = 0; i < fileds.length; i++) {
				pstmt.setObject(i + 1, fileds[i].get(object));
			}
			int resultCount = pstmt.executeUpdate();
			return resultCount > 0 ? true : false;
		}
	}
	/**
	 * 通过自定义datatable查找记录
	 * @param sql
	 * @param params
	 * @return
	 * @throws SQLException
	 */
	public static DataTable findResultByDataTable(String sql, List<Object> params) throws SQLException {
		try (Connection connection = getConnection();
				PreparedStatement pstmt = createNormalPreparedStatement(sql, params, connection);
				ResultSet resultSet = pstmt.executeQuery()) {
			ResultSetMetaData metaData = resultSet.getMetaData();
			DataTable dataTable = new DataTable(metaData.getColumnLabel(1));
			int cols_len = metaData.getColumnCount();
			getColumns(metaData, dataTable, cols_len);
			while (resultSet.next()) {
				getDataRow(resultSet, metaData, dataTable, cols_len);
			}
			return dataTable;
		}
	}
	
	/**
	 * 判断数据库是否存在表
	 * @param tableName
	 * @return
	 * @throws SQLException
	 */
	public static boolean tableExist(String tableName) throws SQLException{
		try (Connection connection = getConnection();
				ResultSet resultSet = connection.getMetaData().getTables(null, null, tableName, null)) {
			return resultSet.next();
		}
	}
	
	/**
	 * 判断数据库是否支持批处理 
	 * @param con
	 * @return
	 */
    public static boolean supportBatch(Connection connection) {
		try {
			// 得到数据库的元数据
			DatabaseMetaData md = connection.getMetaData();
			return md.supportsBatchUpdates();
		} catch (SQLException e) {
			e.printStackTrace();
			return false;
		}
    }
	
	/**
	 * 执行批处理
	 * @param sqls
	 * @return
	 * @throws SQLException
	 */
	public static int[] executeBatch(List<String> sqls) throws SQLException{
		try (Connection connection = getConnection(); Statement statement = connection.createStatement()) {
			for (String sql : sqls) {
				statement.addBatch(sql);
			}
			// 一次执行多条SQL语句
			int[] count = statement.executeBatch();
			return count;
		}
	}
	
	/**
	 * 执行带参数的sql语句
	 * @param sql
	 * @param params
	 * @return
	 * @throws SQLException
	 */
	public static boolean executeSQL(String sql, List<Object> params) throws SQLException {
		try (Connection connection = getConnection();
				PreparedStatement pstmt = createNormalPreparedStatement(sql, params, connection)) {
			return pstmt.execute();
		}
	}
	/**
	 * 执行不带参数的sql语句
	 * @param sql
	 * @return
	 * @throws SQLException 
	 */
	public static boolean executeSQL(String sql) throws SQLException{
		try (Connection connection = getConnection(); Statement statement = connection.createStatement()) {
			return statement.execute(sql);
		}
	}
	/*************************************私有方法*****************************************/
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
	private static void releaseStatement(PreparedStatement pstmt) {
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
	 * 根据filedNames创建插入sql
	 * @param tableName
	 * @param filedNames
	 * @return
	 */
	private static String createInsertSqlByMap(String tableName, Set<String> filedNames) {
		StringBuilder builder = new StringBuilder("insert  into " + tableName);
		// 使用lambda表达式拼接:(字段名1，字段名2)
		builder.append(filedNames.stream().collect(Collectors.joining(",", " ( ", " ) values")));
		// 使用lambda表达式拼接:(?，?)
		builder.append(filedNames.stream().map(p -> "?").collect(Collectors.joining(",", "(", ")")));
		return builder.toString();
	}
	/**
	 * 根据tablename、主键字段名和filedNames创建修改sql
	 * @param tableName
	 * @param primaryKey
	 * @param keys
	 * @return
	 */
	private static String createUpdateSqlByMap(String tableName, String pkName, Set<String> fieldNames) {
		StringBuilder builder = new StringBuilder();
		builder.append("update  " + tableName + " set  ");
		// 使用lambda表达式拼接：字段名=？
		builder.append(fieldNames.stream().collect(Collectors.joining("=?,", " ", "=?")));
		builder.append(" where " + pkName + "=?");
		return builder.toString();
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
