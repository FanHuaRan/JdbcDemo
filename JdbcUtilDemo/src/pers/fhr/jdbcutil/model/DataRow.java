package pers.fhr.jdbcutil.model;

import java.util.HashMap;
import java.util.Map;

public class DataRow {
	//Fields
	private DataTable dataTable;
	private Map<String, Object> values;
	
	//Constrctor
	protected DataRow(){
		values=new HashMap<>();
	}
	
	//getters setters
	public DataTable getDataTable() {
		return dataTable;
	}
	
	//methods
	public Object getValue(String name){
		return values.get(name);
	}
	public void setValue(String name,Object value){/* throws Exception{
		if(!values.containsKey(name)){
			throw new IllegalStateException("不存在"+name+"字段");
		}
		*/
		values.put(name, value);
	}
	protected void removeFiled(String name){
		if(values.containsKey(name)){
			values.remove(name);
		}
	}
	protected void addField(String name){
		values.put(name, null);
	}
	protected void addField(String name,Object value){
		values.put(name, value);
	}
	protected void updateFieldName(String oldName,String newName){
		Object value=values.get(oldName);
		values.remove(oldName);
		values.put(newName, value);
	}
}
