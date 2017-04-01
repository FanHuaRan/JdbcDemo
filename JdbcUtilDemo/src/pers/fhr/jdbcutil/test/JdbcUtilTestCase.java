package pers.fhr.jdbcutil.test;

import java.sql.SQLException;
import java.util.ArrayList;

import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import pers.fhr.jdbcutil.util.JdbcUtils;

public class JdbcUtilTestCase {

	@BeforeClass
	public static void setUpBeforeClass() throws Exception {
	}

	@Before
	public void setUp() throws Exception {
	}
	@Test
	public void testFindResultByDataTable() throws SQLException {
		String sql="select * from album";
		JdbcUtils.findResultByDataTable(sql, new ArrayList<>());
	}
	/*
	@Test
	public void testfindMoreRefResult(){
		String sql="select * from album";
		try {
			JdbcUtils.findMoreResultByObject(sql, new ArrayList<>(),Album.class);
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	*/
	@Test
	public void testFindMoreResultByMap() throws SQLException {
		String sql="select * from album";
		JdbcUtils.findMoreResultByMap(sql, new ArrayList<>());
	}
}
