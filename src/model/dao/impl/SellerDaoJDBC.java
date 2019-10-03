package model.dao.impl;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import db.Conexao;
import db.DbException;
import model.dao.SellerDao;
import model.entities.Department;
import model.entities.Seller;

public class SellerDaoJDBC implements SellerDao{
	
	private Connection conn;
	
	public SellerDaoJDBC(Connection conn) {
		this.conn = conn;
	}

	@Override
	public void insert(Seller obj) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void update(Seller obj) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void deleteById(Integer id) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public Seller findById(Integer id) {
		Seller seller = null;
		
		StringBuilder sbSQL = new StringBuilder();
		sbSQL.append(" SELECT ");
		sbSQL.append("  s.id, s.name, s.email, s.birthdate,");
		sbSQL.append("  s.basesalary,  s.departmentid, d.Name as DepName");
		sbSQL.append(" FROM ");
		sbSQL.append("  seller s ");
		sbSQL.append("  INNER JOIN department d ON s.DepartmentId = d.Id ");
		sbSQL.append(" WHERE s.Id = ? ");
		
		PreparedStatement ps = null;
		ResultSet rs = null;
		
		try{
			ps = conn.prepareStatement(sbSQL.toString());
			ps.setInt(1, id);
			rs = ps.executeQuery();
			
			if(rs.next()){
				seller = instatiateSeller(rs);
			}
		}catch(SQLException e){
			throw new DbException(e.getMessage());
		}finally{
			Conexao.closePreparedStatement(ps);
			Conexao.closeResultSet(rs);
		}
		
		return seller;
	}

	@Override
	public List<Seller> findAll() {
		List<Seller> list = new ArrayList<Seller>();
		
		StringBuilder sbSQL = new StringBuilder();
		sbSQL.append(" SELECT ");
		sbSQL.append("  s.id, s.name, s.email, s.birthdate,");
		sbSQL.append("  s.basesalary,  s.departmentid, d.Name as DepName");
		sbSQL.append(" FROM ");
		sbSQL.append("  seller s ");
		sbSQL.append("  INNER JOIN department d ON s.DepartmentId = d.Id ");
		sbSQL.append(" ORDER BY s.name ");
		
		PreparedStatement ps = null;
		ResultSet rs = null;
		
		try{
			ps = conn.prepareStatement(sbSQL.toString());
			rs = ps.executeQuery();
			
			while(rs.next()){
				list.add(instatiateSeller(rs));
			}
		}catch(SQLException e){
			throw new DbException(e.getMessage());
		}finally{
			Conexao.closePreparedStatement(ps);
			Conexao.closeResultSet(rs);
		}
		
		return list;
	}
	
	@Override
	public List<Seller> findByDepartment(Department department) {
		
		List<Seller> list = new ArrayList<Seller>();
		
		StringBuilder sbSQL = new StringBuilder();
		sbSQL.append(" SELECT ");
		sbSQL.append("  s.id, s.name, s.email, s.birthdate,");
		sbSQL.append("  s.basesalary,  s.departmentid, d.Name as DepName");
		sbSQL.append(" FROM ");
		sbSQL.append("  seller s ");
		sbSQL.append("  INNER JOIN department d ON s.DepartmentId = d.Id ");
		sbSQL.append(" WHERE s.departmentid = ? ");
		sbSQL.append(" ORDER BY s.name ");
		
		PreparedStatement ps = null;
		ResultSet rs = null;
		
		try{
			ps = conn.prepareStatement(sbSQL.toString());
			ps.setInt(1, department.getId());
			rs = ps.executeQuery();
			
			while(rs.next()){
				list.add(instatiateSeller(rs));
			}
		}catch(SQLException e){
			throw new DbException(e.getMessage());
		}finally{
			Conexao.closePreparedStatement(ps);
			Conexao.closeResultSet(rs);
		}
		
		return list;
	}
	
	private Seller instatiateSeller(ResultSet rs) throws SQLException{
		return new Seller(rs.getInt("id"), 
						  rs.getString("name"), 
						  rs.getString("email"), 
						  rs.getTimestamp("birthdate"), 
						  rs.getDouble("basesalary"), 
						  new Department(rs.getInt("departmentid"), rs.getString("DepName")));
	}

}