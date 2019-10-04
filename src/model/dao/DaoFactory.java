package model.dao;

import db.Conexao;
import model.dao.impl.DepartmentDaoJDBC;
import model.dao.impl.SellerDaoJDBC;

public class DaoFactory {
	public static SellerDao createSellerDao(){
		return new SellerDaoJDBC(Conexao.getConnection());
	}
	public static DepartmentDao createDepartmentDao(){
		return new DepartmentDaoJDBC(Conexao.getConnection());
	}
}