package com.usaccidents.operators;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;

public class HIVEInputOperator {
    public static void main(String[] args) throws Exception {
        Class.forName("org.apache.hive.jdbc.HiveDriver");
        Connection con = DriverManager.getConnection("jdbc:hive2://localhost:10000/default", "root", "");
        Statement stmt = con.createStatement();

        ResultSet res = stmt.executeQuery("SELECT * FROM raw_accidents LIMIT 10");
        while (res.next()) {
            System.out.println(res.getString(1));
        }
        stmt.close();
        con.close();
    }
}