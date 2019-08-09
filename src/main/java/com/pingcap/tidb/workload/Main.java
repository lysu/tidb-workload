package com.pingcap.tidb.workload;


public class Main {

    public static void main(String[] args) throws Exception{
        DbUtil.getInstance().initConnectionPool("jdbc:mysql://0.0.0.0:4000/test?useunicode=true&characterEncoding=utf8", "root", "");
        new Test().test();
    }

}
