package com.pingcap.tidb.workload;

import com.pingcap.tidb.workload.utils.UidGenerator;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Date;
import java.util.concurrent.CountDownLatch;

public class InsertWorkload {

    private static final String insertSQL =
        "insert into txn_history(txn_id, user_id, txn_type, txn_state, txn_order_amount, txn_order_currency, txn_charge_amount, "
            +
            "txn_charge_currency, txn_exchange_amount, txn_exchange_currency, txn_promo_amount, txn_promo_currency, "
            +
            "disabled, version, order_id, order_state, order_error_code, order_type, order_created_at, order_updated_at, order_version, order_items, "
            +
            "payment_id, payment_created_at, payment_updated_at, payment_paid_at, payment_version, payment_state, payment_error_code, comments, sub_payments, "
            +
            "cb_amount, cb_state, cb_release_date, cb_created_at, cb_updated_at, cb_version, merchant_id, merchant_name, merchant_cat, merchant_sub_cat, created_at, updated_at,"
            +
            "store_id, store_name, pos_id, biller_id, logo_url, peer_id, peer_name, device_id, extra_info) "
            +
            "values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, now(), now(), ?, ?, ?, now(), now(), now(), ?, ?, ?, ?, ?, "
            +
            "?, ?, now(), now(), now(), ?, ?, ?, ?, ?, now(), now(), ?, ?, ?, ?, ?, ?, ?, ?, ?)";


    public static void workload(int workId, int concurrency) {
        CountDownLatch tmpwg = new CountDownLatch(concurrency);
        final UidGenerator uidGenerator = new UidGenerator(30, 20, 13);
        uidGenerator.setWorkerId(workId);
        for (int i = 0; i < concurrency; i++) {
            final int threadID = i;
            new Thread(() -> {
                Connection conn = null;
                try {
                    conn = DbUtil.getInstance().getConnection();
                    final PreparedStatement inPstmt = conn.prepareStatement(insertSQL);
                    long repeat = 0;
                    while (true) {
                        try {
                            insert(inPstmt, uidGenerator);
                        }catch (Exception e) {
                            e.printStackTrace();
                        }
                        if (repeat % 2000 ==0) {
                            System.out.println(Thread.currentThread().getId() +"  " +new Date() + "  add batch done" );
                        }
                        repeat ++;
                    }
                } catch (Exception e) {
                    e.printStackTrace();
                } finally {
                    tmpwg.countDown();
                    try {
                        if (conn != null) {
                            DbUtil.getInstance().closeConnection(conn);
                        }
                    } catch (SQLException e) {
                        e.printStackTrace();
                    }
                }
            }).start();
        }
        try {
            tmpwg.await();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    public static int BATCH_SIZE = 200;
    public static  void insert(PreparedStatement inPstmt, UidGenerator uidGenerator)
        throws SQLException {
        for (int i = 0; i < BATCH_SIZE; i++) {
            long txnId = uidGenerator.getUID();
            long userId = txnId;
            long orderId = txnId;
//            long paymentId = uidGenerator.getUID();

            inPstmt.setLong(1, txnId); // txn_id
            inPstmt.setLong(2, userId); //user_id
            inPstmt.setString(3, "txn_type");
            inPstmt.setString(4, "txn_state");
            inPstmt.setLong(5, 100); // txn_order_amount
            inPstmt.setString(6, "txn_order_currency");
            inPstmt.setLong(7, 1000); // txn_charge_amount
            inPstmt.setString(8, "JPY"); // txn_charge_currency
            inPstmt.setLong(9, 10);// txn_exchange_amount
            inPstmt.setString(10, "JPY"); // txn_exchange_currency
            inPstmt.setLong(11, 9);// txn_promo_amount
            inPstmt.setString(12, "JPY"); // txn_promo_currency
            inPstmt.setInt(13, 0); // disabled
            inPstmt.setInt(14, 1); // version
            inPstmt.setLong(15, orderId); // order_id
            inPstmt.setString(16, "started"); // order_state
            inPstmt.setString(17, "code1"); // order_error_code
            inPstmt.setString(18, "type1"); // order_type
            inPstmt.setLong(19, 4); // order_version
            inPstmt.setString(20, "{}");// order_items
            inPstmt.setLong(21, userId); // payment_id
            inPstmt.setLong(22, 5); // payment_version
            inPstmt.setString(23, "paid");// payment_state
            inPstmt.setString(24, "c1");// payment_error_code
            inPstmt.setString(25, "{}");// comments
            inPstmt.setString(26, "{}");// sub_payments
            inPstmt.setLong(27, 1); // cd_amount
            inPstmt.setString(28, "done"); // cb_state
            inPstmt.setInt(29, 1); // cb_version
            inPstmt.setInt(30, 1000); // merchant_id
            inPstmt.setString(31, "Thomas Jefferson and James Madison met in 1776. Could it have been any other year? They worked together starting then to further American Revolution and later to shape the new scheme of government. From the work sprang a friendship perhaps incomparable in intimacy1 and the trustfulness of collaboration2 and induration. It lasted 50 years. It included pleasure and utility but over and above them, there were shared purpose, a common end and "); // merchant_name
            inPstmt.setLong(32, 1); // merchant_cat
            inPstmt.setLong(33, 2); // merchant_cat_sub
            inPstmt.setString(34, "s1"); // store_id
            inPstmt.setString(35, "Thomas Jefferson and James Madison met in 1776. Could it have been any other year? They worked together starting then to further American Revolution and later to shape the new scheme of government. From the work sprang a friendship perhaps incomparable in intimacy1 and the trustfulness of collaboration2 and induration. It lasted 50 years. It included pleasure and utility but over and above them, there were shared purpose, a common end and "); // store_name
            inPstmt.setString(36, "pos1");// pos_id
            inPstmt.setLong(37, 1);// biller_id
            inPstmt.setString(38, "Thomas Jefferson and James Madison met in 1776. Could it have been any other year? They worked together starting then to further American Revolution and later to shape the new scheme of government. From the work sprang a friendship perhaps incomparable in intimacy1 and the trustfulness of collaboration2 and induration. It lasted 50 years. It included pleasure and utility but over and above them, there were shared purpose, a common end and an enduring goodness on both sides. Four and a half months before he died, when he was ailing3, debt-ridden, and worried about his impoverished4 family, Jefferson wrote to his longtime friend. His words and Madison's reply remind us that friends are friends until death. They also remind us that sometimes a friendship has a bearing on things larger than the friendship itself, for has there ever been a friendship of greater public consequence than this one?Thomas Jefferson and James Madison met in 1776. Could it have been any other year? They worked together starting then to further American Revolution and later to shape the new scheme of government. From the work sprang a friendship perhaps incomparable in intimacy1 and the trustfulness of collaboration2 and induration. It lasted 50 years. It included pleasure and utility but over and above them, there were shared purpose, a common end and an enduring goodness on both sides. Four and a half months before he died, when he was ailing3, debt-ridden, and worried about his impoverished4 family, Jefferson wrote to his longtime friend. His words and Madison's reply remind us that friends are friends until death. They also remind us that sometimes a friendship has a bearing on things larger than the friendship itself, for has there ever been a friendship of greater public consequence than this one? ");// logo_url
            inPstmt.setLong(39, 2);// peer_id
            inPstmt.setString(40, "Thomas Jefferson and James Madison met in 1776. Could it have been any other year? They worked together starting then to further American Revolution and later to shape the new scheme of government. From the work sprang a friendship perhaps incomparable in intimacy1 and the trustfulness of collaboration2 and induration. It lasted 50 years. It included pleasure and utility but over and above them, there were shared purpose, a common end and ");// peer_name
            inPstmt.setString(41, "Thomas Jefferson and James Madison met in 1776. Could it have been any other year? They worked together starting then to further American Revolution and later to shape the new scheme of government. From the work sprang a friendship perhaps incomparable in intimacy1 and the trustfulness of collaboration2 and induration. It lasted 50 years. It included pleasure and utility but over and above them, there were shared purpose, a common end and ");// device_id
            inPstmt.setString(42, "{}");// extra_info

            inPstmt.addBatch();
        }
        inPstmt.executeBatch();
        inPstmt.clearBatch();
    }
}
