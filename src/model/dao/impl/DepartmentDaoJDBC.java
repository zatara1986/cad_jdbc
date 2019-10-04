package model.dao.impl;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

import db.Conexao;
import db.DbException;
import model.dao.DepartmentDao;
import model.entities.Department;

public class DepartmentDaoJDBC implements DepartmentDao{
	
	private Connection conn;
	
	public DepartmentDaoJDBC(Connection conn) {
		this.conn = conn;
	}

	@Override
	public void insert(Department obj) {
		StringBuilder sbSQL = new StringBuilder();
		sbSQL.append(" INSERT INTO department (Name) VALUES (?) ");
		
		PreparedStatement ps = null;
		try{
			ps = conn.prepareStatement(sbSQL.toString(), Statement.RETURN_GENERATED_KEYS);
			ps.setString(1, obj.getName());
			
			int rowsAffected = ps.executeUpdate();
			
			if (rowsAffected > 0) {
				ResultSet rs = ps.getGeneratedKeys();
				if (rs.next()) {
					int id = rs.getInt(1);
					obj.setId(id);
				}
				Conexao.closeResultSet(rs);
			}else{
				throw new DbException("Unexpected error! No rows affected!");
			}
			
		}catch(SQLException e){
			throw new DbException(e.getMessage());
		}finally{
			Conexao.closePreparedStatement(ps);
		}
	}

	@Override
	public void update(Department obj) {
		StringBuilder sbSQL = new StringBuilder();
		sbSQL.append(" UPDATE department SET Name = ? WHERE Id = ? ");
		
		PreparedStatement ps = null;
		try{
			ps = conn.prepareStatement(sbSQL.toString());
			ps.setString(1, obj.getName());
			ps.setInt(2, obj.getId());
			ps.executeUpdate();
		}catch(SQLException e){
			throw new DbException(e.getMessage());
		}finally{
			Conexao.closePreparedStatement(ps);
		}
	}

	@Override
	public void deleteById(Integer id) {
		StringBuilder sbSQL = new StringBuilder();
		sbSQL.append(" DELETE FROM department WHERE Id = ? ");
		PreparedStatement ps = null;
		try{
			ps = conn.prepareStatement(sbSQL.toString());
			ps.setInt(1, id);
			ps.executeUpdate();
		}catch(SQLException e){
			throw new DbException(e.getMessage());
		}finally{
			Conexao.closePreparedStatement(ps);
		}
	}

	@Override
	public Department findById(Integer id) {
		Department department = null;
		
		StringBuilder sbSQL = new StringBuilder();
		sbSQL.append(" SELECT ");
		sbSQL.append("  d.id, d.name ");
		sbSQL.append(" FROM ");
		sbSQL.append("  department d ");
		sbSQL.append(" WHERE d.Id = ? ");
		
		PreparedStatement ps = null;
		ResultSet rs = null;
		
		try{
			ps = conn.prepareStatement(sbSQL.toString());
			ps.setInt(1, id);
			rs = ps.executeQuery();
			
			if(rs.next()){
				department = instatiateDepartment(rs);
			}
		}catch(SQLException e){
			throw new DbException(e.getMessage());
		}finally{
			Conexao.closePreparedStatement(ps);
			Conexao.closeResultSet(rs);
		}
		return department;
	}

	@Override
	public List<Department> findAll() {
		List<Department> list = new ArrayList<Department>();
		
		StringBuilder sbSQL = new StringBuilder();
		sbSQL.append(" SELECT ");
		sbSQL.append("  d.id, d.name "); 
		sbSQL.append(" FROM department d ");
		sbSQL.append(" ORDER BY d.name ");
		
		PreparedStatement ps = null;
		ResultSet rs = null;
		
		try{
			ps = conn.prepareStatement(sbSQL.toString());
			rs = ps.executeQuery();
			
			while(rs.next()){
				list.add(instatiateDepartment(rs));
			}
		}catch(SQLException e){
			throw new DbException(e.getMessage());
		}finally{
			Conexao.closePreparedStatement(ps);
			Conexao.closeResultSet(rs);
		}
		
		return list;
	}
	
	private Department instatiateDepartment(ResultSet rs) throws SQLException{
		return new Department(rs.getInt("id"), rs.getString("name"));
	}
}