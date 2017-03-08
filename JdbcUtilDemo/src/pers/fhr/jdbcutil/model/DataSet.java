package pers.fhr.jdbcutil.model;

import java.util.HashMap;
import java.util.Map;

public class DataSet {
	private Map<String, DataTable> tables;
	
	public DataSet() {
		tables=new HashMap<>();
	}
	public DataSet(DataTable dataTable){
		tables=new HashMap<>();
		tables.put(dataTable.getTableName(), dataTable);
	}
	public void addTable(DataTable dataTable){
		tables.put(dataTable.getTableName(), dataTable);
	}
	public DataTable getTable(String name){
		return tables.get(name);
	}
	public void removeTable(String name){
		tables.remove(name);
	}
}
