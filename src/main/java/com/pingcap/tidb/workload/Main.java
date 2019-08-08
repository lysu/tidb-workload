package com.pingcap.tidb.workload;


public class Main {

    public static void main(String[] args) {
        DbUtil.getInstance().initConnectionPool("", "", "");
    }

}
