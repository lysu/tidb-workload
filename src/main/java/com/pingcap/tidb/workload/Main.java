package com.pingcap.tidb.workload;


public class Main {

    //java -cp tidb-workload-all.jar com.pingcap.tidb.workload.Main 2234 insert 100 4000
    public static void main(String[] args) throws Exception{
        DbUtil.getInstance().initConnectionPool("jdbc:mysql://aa7e48fbcb9a811e9bc3e0e05a91079b-ed3b031e6d68faca.elb.ap-northeast-1.amazonaws.com:4000/test?useunicode=true&characterEncoding=utf8", "root", "");
        int workId=Integer.parseInt(args[0]);
        boolean update = "update".equalsIgnoreCase(args[1]);
        int concurrency = Integer.parseInt(args[2]);
        int repeat = Integer.parseInt(args[3]);
        new Test().test(workId, concurrency, repeat, update);
    }

}
