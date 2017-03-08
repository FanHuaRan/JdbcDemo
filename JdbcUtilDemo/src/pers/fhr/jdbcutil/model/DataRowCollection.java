package pers.fhr.jdbcutil.model;

import java.util.ArrayList;
import java.util.List;

public class DataRowCollection extends ArrayList<DataRow>{
	
	/**
	 * 
	 */
	private static final long serialVersionUID = 3206611314966467900L;
	
	protected void addFiled(String name){
		for(DataRow row:this){
			row.addField(name);
		}
	}
	protected void removeFiled(String name){
		for(DataRow row:this){
			row.removeFiled(name);
		}
	}
	protected void updateFiledName(String oldName,String newName){
		for(DataRow row :this){
			row.updateFieldName(oldName, newName);
		}
	}
}
