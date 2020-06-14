package com.xa.demo;

import com.mysql.jdbc.jdbc2.optional.MysqlXAConnection;
import com.mysql.jdbc.jdbc2.optional.MysqlXid;

import javax.sql.XAConnection;
import javax.transaction.xa.XAException;
import javax.transaction.xa.XAResource;
import javax.transaction.xa.Xid;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;

public class TestXA {
    public static void main(String[] args) throws Exception {
        boolean xaCommondsLog = true;  // true打印调试日志
        // 获得资源管理器操作接口实例rm1
        Connection conn1 = DriverManager.getConnection("jdbc:mysql://localhost:3306/db_user", "root", "root");
        XAConnection xaConn1 = new MysqlXAConnection((com.mysql.jdbc.Connection) conn1, xaCommondsLog);
        XAResource rm1 = xaConn1.getXAResource();
        // 获得资源管理器操作接口实例rm2
        Connection conn2 = DriverManager.getConnection("jdbc:mysql://localhost:3306/db_account", "root", "root");
        XAConnection xaConn2 = new MysqlXAConnection((com.mysql.jdbc.Connection) conn2, xaCommondsLog);
        XAResource rm2 = xaConn2.getXAResource();

        // AP请求TM执行一个分布式事务，TM生成全局事务id
        byte[] globalTId = "g111".getBytes();
        int formatId = 1;
        try {
            // 分别执行rm1和rm2的事务分支

            // tm生成rm1的事务分支id
            byte[] b1 = "b00001".getBytes();
            Xid xid1 = new MysqlXid(globalTId, b1, formatId);
            // 执行rm1的事务分支
            rm1.start(xid1, XAResource.TMNOFLAGS);

            PreparedStatement ps1 = conn1.prepareStatement("insert into user(name) values('test3')");
            ps1.execute();
            rm1.end(xid1, XAResource.TMSUCCESS);

            // tm生成rm2的事务分支id
            byte[] b2 = "b00002".getBytes();
            Xid xid2 = new MysqlXid(globalTId, b2, formatId);
            // 执行rm2的事务分支
            rm2.start(xid2, XAResource.TMNOFLAGS);

            PreparedStatement ps2 = conn2.prepareStatement("insert into account(name) values('test4')");
            ps2.execute();
            rm2.end(xid2, XAResource.TMSUCCESS);

            // 两阶段提交
            // 阶段1：询问所有的rm，准备提交事务分支
            int rm1_prepare = rm1.prepare(xid1);
            int rm2_prepare = rm2.prepare(xid2);
            // 阶段2：提交所有事务分支
            boolean onePhase = false;
            if (rm1_prepare == XAResource.XA_OK && rm2_prepare == XAResource.XA_OK) { // 所有事务分支都prepare成功，提交所有事务分支
                rm1.commit(xid1, onePhase);
                rm2.commit(xid2, onePhase);

                // 这个地方可以做重试
                // TODO 重试
            } else { // 如果有事务分支没有成功，则回滚
                rm1.rollback(xid1);
                rm2.rollback(xid2);
            }
        } catch (XAException ex) {
            // 如果出现异常，也要进行回滚
            ex.printStackTrace();
        }
    }
}
