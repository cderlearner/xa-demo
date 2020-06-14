package com.xa.demo;

import com.atomikos.icatch.jta.UserTransactionImp;
import com.atomikos.jdbc.AtomikosDataSourceBean;
import javax.transaction.UserTransaction;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.Statement;
import java.util.Properties;

public class TestAtomikosJTA {
    public static void main(String[] args) throws Exception{
        AtomikosDataSourceBean ds1 = create("db_user");
        AtomikosDataSourceBean ds2 = create("db_account");

        Connection conn1 = null;
        Connection conn2 = null;
        PreparedStatement ps1 = null;
        PreparedStatement ps2 = null;

        UserTransaction userTransaction = new UserTransactionImp();
        try {
            userTransaction.begin();

            // 执行db1的sql
            conn1 = ds1.getConnection();
            ps1 = conn1.prepareStatement("insert into user(name) values (?)", Statement.RETURN_GENERATED_KEYS);
            ps1.setString(1, "test1");
            ps1.executeUpdate();

            // 模拟异常
            //int i = 1/0;

            conn2 = ds2.getConnection();
            ps2 = conn2.prepareStatement("insert into account(name) values (?)", Statement.RETURN_GENERATED_KEYS);
            ps2.setString(1, "test2");
            ps2.executeUpdate();

            // 两阶段提交
            userTransaction.commit();
        } catch (Exception ex) {
            ex.printStackTrace();
            userTransaction.rollback();
        } finally {
            try {
                conn1.close();
                conn2.close();
                ps1.close();
                ps2.close();
                ds1.close();
                ds2.close();
            } catch (Exception ex2) {}
        }
    }

    private static AtomikosDataSourceBean create(String dbName) {
        Properties p = new Properties();
        p.setProperty("url", "jdbc:mysql://localhost:3306/" + dbName);
        p.setProperty("user", "root");
        p.setProperty("password", "root");

        // 使用AtomikosDataSourceBean封装MysqlXADataSource
        // 代理mysql数据源，实现二阶段提交协议
        AtomikosDataSourceBean bean = new AtomikosDataSourceBean();
        bean.setUniqueResourceName(dbName);
        bean.setXaDataSourceClassName("com.mysql.jdbc.jdbc2.optional.MysqlXADataSource");
        bean.setXaProperties(p);
        return bean;
    }
}
