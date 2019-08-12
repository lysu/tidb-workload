package com.pingcap.tidb.workload;

import com.pingcap.tidb.workload.utils.UidGenerator;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import org.json.JSONArray;
import org.json.JSONObject;


public class RegionUpdateWorkload {

    private static String dbName = null;
    private static String tableName = null;
    private static String keyName = null;

    public static void main(String[] args) throws Exception {
//        dbName = args[3];
//        DbUtil.getInstance().initConnectionPool(
//            String.format("jdbc:mysql://aa7e48fbcb9a811e9bc3e0e05a91079b-ed3b031e6d68faca.elb.ap-northeast-1.amazonaws.com:4000/%s?useunicode=true&characterEncoding=utf8&rewriteBatchedStatements=true&useLocalSessionState=true", dbName),
//            "root", "");
//        int workId = Integer.parseInt(args[0]);
//        int concurrency = Integer.parseInt(args[1]);
//        int repeat = Integer.parseInt(args[2]);
//        tableName = args[4];
//        keyName = args[5];
//        new RegionUpdateWorkload().updateTest(workId, concurrency, repeat, tableName);
//        System.out.println(new JSONObject(readFile("/Users/jiangjianyuan/work/git/tidb-workload/txn_history.json")).get("id"));


//        printRegionInfo(args);
    }

    private static void printRegionInfo(String[] args) throws Exception{

        String dbName = args[0];
        String tblName = args[1];
        JSONObject json = new JSONObject(Utils.get(String.format("http://aa7e48fbcb9a811e9bc3e0e05a91079b-ed3b031e6d68faca.elb.ap-northeast-1.amazonaws.com:10080/tables/%s/%s/regions", dbName, tblName)));
        JSONArray regions = json.getJSONArray("record_regions");

        JSONArray detail = new JSONArray();
        for(int i=0;i<regions.length();i++) {
            JSONObject region = regions.getJSONObject(i);
            int regionId = region.getInt("region_id");
            JSONObject regionInfo = new JSONObject(Utils.get(String.format("http://aa7e48fbcb9a811e9bc3e0e05a91079b-ed3b031e6d68faca.elb.ap-northeast-1.amazonaws.com:10080/regions/%s", regionId)));
            detail.put(regionInfo);
        }
        String str = detail.toString();
        System.out.println("\n\n");
        System.out.println(str);
    }


    private static final String updateSQL = "update  %s set "
        + "txn_id=?, "
        + "user_id=?,"
        + "txn_type=?, "
        + "txn_state=?, "
        + "txn_order_amount=?, "
        + "txn_order_currency=?, "
        + "txn_charge_amount=?, " +
        "txn_charge_currency=?, "
        + "txn_exchange_amount=?, "
        + "txn_exchange_currency=?, "
        + "txn_promo_amount=?, "
        + "txn_promo_currency=?, " +
        "disabled=?, "
        + "version=?, "
        + "order_id=?,"
        + " order_state=?, "
        + "order_error_code=?, "
        + "order_type=?, "
        + "order_created_at=now(),"
        + " order_updated_at=now(), "
        + "order_version=?, "
        + "order_items=?, " +
        "payment_id=?, "
        + "payment_created_at=now(), "
        + "payment_updated_at=now(), "
        + "payment_paid_at=now(), "
        + "payment_version=?, "
        + "payment_state=?, "
        + "payment_error_code=?, "
        + "comments=?, "
        + "sub_payments=?, " +
        "cb_amount=?, "
        + "cb_state=?, "
        + "cb_release_date=now(), "
        + "cb_created_at=now(), "
        + "cb_updated_at=now(), "
        + "cb_version=?,"
        + " merchant_id=?, "
        + "merchant_name=?, "
        + "merchant_cat=?, "
        + "merchant_sub_cat=?, "
        + "created_at=now(), "
        + "updated_at=now()," +
        "store_id=?, "
        + "store_name=?, "
        + "pos_id=?, "
        + "biller_id=?, "
        + "logo_url=?, "
        + "peer_id=?, peer_name=?, device_id=?, "
        + "extra_info=? where %s =? ";

    private final static int updateSize = 10000;
    private List<Long> userIds = new ArrayList<>(10000);
    private List<Long> orderIds = new ArrayList<>(10000);
    private List<Long> txnIds = new ArrayList<>(10000);
    private List<Long> paymentIds = new ArrayList<>(10000);
    private List<Long> keyIds = new ArrayList<>(10000);

    private static String selectSQL = "select  txn_id, user_id, order_id, payment_id, %s from % where %s >= ? and %s < ?  limit 2";



    private void updateTest(int workId, int concurrency, int repeat, String tableName)
        throws Exception {
        final UidGenerator uidGenerator = new UidGenerator(30, 20, 13);
        uidGenerator.setWorkerId(workId);
        long start = System.currentTimeMillis();
        System.out.println("start to insert base data");
        Connection conn = null;

        try {


            conn = getConnection();

            final PreparedStatement seStmt = conn
                .prepareStatement(String.format(selectSQL, keyName, tableName, keyName, keyName));



            ResultSet rs = seStmt.executeQuery();
            while (rs.next()) {
                txnIds.add(rs.getLong(1));
                userIds.add(rs.getLong(2));
                orderIds.add(rs.getLong(3));
                paymentIds.add(rs.getLong(4));
                keyIds.add(rs.getLong(5));
            }
            rs.close();
            seStmt.clearParameters();

        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            closeConnection(conn);
        }

        System.out.println("insert data done: " + (System.currentTimeMillis() - start) + "ms");

//        CountDownLatch tmpwg = new CountDownLatch(concurrency);
//        for (
//            int i = 0;
//            i < concurrency; i++) {
//            new Thread(new Runnable() {
//                @Override
//                public void run() {
//                    Connection conn = null;
//                    try {
//                        Random random = new Random();
//                        conn = getConnection();
//                        final PreparedStatement updateStmt = conn
//                            .prepareStatement(String.format(updateSQL, tableName, keyName));
//                        for (int i = 0; i < repeat; i++) {
//                            try {
//                                conn.setAutoCommit(false);
//                                int index = random.nextInt(userIds.size());
//                                long txnId = txnIds.get(index);
//                                long userId = userIds.get(index);
//                                long orderId = orderIds.get(index);
//                                long paymentId = paymentIds.get(index);
//
//                                setUpdateParam(updateStmt, txnId, userId, orderId, paymentId);
//                                updateStmt.execute();
//                                updateStmt.clearParameters();
//                                conn.commit();
//                            } catch (Exception e) {
//                                e.printStackTrace();
//                            }
//                        }
//                    } catch (Exception e) {
//                        e.printStackTrace();
//                    } finally {
//                        closeConnection(conn);
//                        tmpwg.countDown();
//                    }
//                }
//            }).start();
//        }
//        tmpwg.await();
//        System.out.println("All done, use " + (System.currentTimeMillis() - start) + "ms");
    }

    private void setUpdateParam(PreparedStatement updateStmt, long txnId, long userId, long orderId,
        long paymentId)
        throws SQLException {
        updateStmt.setLong(1, txnId); // txn_id
        updateStmt.setLong(2, userId); //user_id
        updateStmt.setString(3, "txn_type");
        updateStmt.setString(4, "txn_state");
        updateStmt.setLong(5, 100); // txn_order_amount
        updateStmt.setString(6, "txn_order_currency");
        updateStmt.setLong(7, 1000); // txn_charge_amount
        updateStmt.setString(8, "JPY"); // txn_charge_currency
        updateStmt.setLong(9, 10);// txn_exchange_amount
        updateStmt.setString(10, "JPY"); // txn_exchange_currency
        updateStmt.setLong(11, 9);// txn_promo_amount
        updateStmt.setString(12, "JPY"); // txn_promo_currency
        updateStmt.setInt(13, 0); // disabled
        updateStmt.setInt(14, 1); // version
        updateStmt.setLong(15, orderId); // order_id
        updateStmt.setString(16, "started"); // order_state
        updateStmt.setString(17, "code1"); // order_error_code
        updateStmt.setString(18, "type1"); // order_type
        updateStmt.setLong(19, 4); // order_version
        updateStmt.setString(20, "{}");// order_items
        updateStmt.setLong(21, paymentId); // payment_id
        updateStmt.setLong(22, 5); // payment_version
        updateStmt.setString(23, "paid");// payment_state
        updateStmt.setString(24, "c1");// payment_error_code
        updateStmt.setString(25, "{}");// comments
        updateStmt.setString(26, "{}");// sub_payments
        updateStmt.setLong(27, 1); // cd_amount
        updateStmt.setString(28, "done"); // cb_state
        updateStmt.setInt(29, 1); // cb_version
        updateStmt.setInt(30, 1000); // merchant_id
        updateStmt.setString(31, "mname"); // merchant_name
        updateStmt.setLong(32, 1); // merchant_cat
        updateStmt.setLong(33, 2); // merchant_cat_sub
        updateStmt.setString(34, "s1"); // store_id
        updateStmt.setString(35, "store x"); // store_name
        updateStmt.setString(36, "pos1");// pos_id
        updateStmt.setLong(37, 1);// biller_id
        updateStmt.setString(38, "http://11.com/1");// logo_url
        updateStmt.setLong(39, 2);// peer_id
        updateStmt.setString(40, "p1");// peer_name
        updateStmt.setLong(41, 99);// device_id
        updateStmt.setString(42, "{}");// extra_info
        updateStmt.setLong(43, userId); //key
    }

    private Connection getConnection() throws SQLException {
        return DbUtil.getInstance().getConnection();
    }

    private void closeConnection(Connection conn) {
        if (conn != null) {
            try {
                DbUtil.getInstance().closeConnection(conn);
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
    }
}