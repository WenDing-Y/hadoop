/**
 * @author DELL_pc
 *  @date 2017年6月27日
 * 
 */
package com.beifeng.test;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.lib.db.DBWritable;

/**
 * @author DELL_pc
 *  @date 2017年6月27日
 */
public class StudentRecord implements Writable,DBWritable{
     int id;
     String name;
	
	/**
	 * @return the id
	 */
	public int getId() {
		return id;
	}


	/**
	 * @param id the id to set
	 */
	public void setId(int id) {
		this.id = id;
	}


	/**
	 * @return the name
	 */
	public String getName() {
		return name;
	}


	/**
	 * @param name the name to set
	 */
	public void setName(String name) {
		this.name = name;
	}


	@Override
	public void write(PreparedStatement statement) throws SQLException {
		// TODO Auto-generated method stub
		statement.setInt(1, this.id);
		statement.setString(2, this.name);
	}

	
	@Override
	public void readFields(ResultSet resultSet) throws SQLException {
		// TODO Auto-generated method stub
		this.id=resultSet.getInt(1);
		this.name=resultSet.getString(2);
	}

	
	@Override
	public void write(DataOutput out) throws IOException {
		// TODO Auto-generated method stub
		out.writeInt(this.id);
		out.writeUTF(this.name);
	}

	
	@Override
	public void readFields(DataInput in) throws IOException {
		// TODO Auto-generated method stub
		this.id=in.readInt();
		this.name=in.readUTF();
	}


	
	@Override
	public String toString() {
		return "StudentRecord [id=" + id + ", name=" + name + "]";
	}
	

}
