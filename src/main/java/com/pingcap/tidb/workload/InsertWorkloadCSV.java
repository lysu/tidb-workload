package com.pingcap.tidb.workload;

import java.io.*;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Date;
import java.util.Random;
import java.util.concurrent.CountDownLatch;

public class InsertWorkloadCSV {
    private static final String TITLES =
            "txn_id, user_id, txn_type, txn_state, txn_order_amount, txn_order_currency, txn_charge_amount, "
                    +
                    "txn_charge_currency, txn_exchange_amount, txn_exchange_currency, txn_promo_amount, txn_promo_currency, "
                    +
                    "disabled, version, order_id, order_state, order_error_code, order_type, order_created_at, order_updated_at, order_version, order_items, "
                    +
                    "payment_id, payment_created_at, payment_updated_at, payment_paid_at, payment_version, payment_state, payment_error_code, comments, sub_payments, "
                    +
                    "cb_amount, cb_state, cb_release_date, cb_created_at, cb_updated_at, cb_version, merchant_id, merchant_name, merchant_cat, merchant_sub_cat, created_at, updated_at,"
                    +
                    "store_id, store_name, pos_id, biller_id, logo_url, peer_id, peer_name, device_id, extra_info ";



    public static void workload(int concurrency, final String basePath) {
        CountDownLatch tmpwg = new CountDownLatch(concurrency);
//        final UidGenerator uidGenerator = new UidGenerator(30, 20, 13);
//        uidGenerator.setWorkerId(workId);
        for (int i = 0; i < concurrency; i++) {
            final int threadID = i;
            new Thread(() -> {
                FileOutputStream out = null;
                OutputStreamWriter osw = null;
                BufferedWriter bw = null;
                try {
                    File finalCSVFile = new File(String.format("%s/csv%d", basePath, threadID));
                    out = new FileOutputStream(finalCSVFile);
                    osw = new OutputStreamWriter(out, "UTF-8");
                    // 手动加上BOM标识
                    osw.write(new String(new byte[] { (byte) 0xEF, (byte) 0xBB, (byte) 0xBF }));
                    bw = new BufferedWriter(osw);

                    bw.append(TITLES).append("\r");

                    long repeat = 0;
                    while (true) {
                        try {
                            insert(bw);
                        }catch (Exception e) {
                            e.printStackTrace();
                        }
                        if (repeat % 1000 ==0) {
                            bw.flush();
                            System.out.println(Thread.currentThread().getId() +"  " +new Date() + "  add batch done" );
                        }
                        repeat ++;
                    }
                } catch (Exception e) {
                    e.printStackTrace();
                } finally {
                    if (bw != null) {
                        try {
                            bw.close();
                            bw = null;
                        } catch (IOException e) {
                            e.printStackTrace();
                        }
                    }
                    if (osw != null) {
                        try {
                            osw.close();
                            osw = null;
                        } catch (IOException e) {
                            e.printStackTrace();
                        }
                    }
                    if (out != null) {
                        try {
                            out.close();
                            out = null;
                        } catch (IOException e) {
                            e.printStackTrace();
                        }
                    }
                    tmpwg.countDown();
                }
            }).start();
        }
        try {
            tmpwg.await();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    private static Random r = new Random();
    private static int BATCH_SIZE = 200;
    private static  void insert(BufferedWriter bw)
            throws SQLException, IOException {
//        for (int i = 0; i < BATCH_SIZE; i++) {
            long txnId = r.nextLong();
            long userId = txnId;
            long orderId = txnId;
//            long paymentId = uidGenerator.getUID();

            DBStruct struct = new DBStruct();
         bw.append(String.format("%s,", txnId)); // txn_id
        bw.append(String.format("%s,", userId)); //user_id
        bw.append(String.format("%s,", "txn_type"));
        bw.append(String.format("%s,", "txn_state"));
        bw.append(String.format("%s,", 100)); // txn_order_amount
        bw.append(String.format("%s,",  "txn_order_currency"));
        bw.append(String.format("%s,", 1000)); // txn_charge_amount
        bw.append(String.format("%s,", "JPY")); // txn_charge_currency
        bw.append(String.format("%s,",  10));// txn_exchange_amount
        bw.append(String.format("%s,",  "JPY")); // txn_exchange_currency
        bw.append(String.format("%s,", 9));// txn_promo_amount
        bw.append(String.format("%s,",  "JPY")); // txn_promo_currency
        bw.append(String.format("%s,",  0)); // disabled
        bw.append(String.format("%s,",  1)); // version
        bw.append(String.format("%s,", orderId)); // order_id
        bw.append(String.format("%s,", "started")); // order_state
        bw.append(String.format("%s,",  "code1")); // order_error_code
        bw.append(String.format("%s,", "type1")); // order_type
        bw.append(String.format("%s,",  4)); // order_version
        bw.append(String.format("%s,",  "{}"));// order_items
        bw.append(String.format("%s,",  userId)); // payment_id
        bw.append(String.format("%s,",  5)); // payment_version
        bw.append(String.format("%s,",  "paid"));// payment_state
        bw.append(String.format("%s,",  "c1"));// payment_error_code
        bw.append(String.format("%s,", "{}"));// comments
        bw.append(String.format("%s,",  "{}"));// sub_payments
        bw.append(String.format("%s,", 1)); // cd_amount
        bw.append(String.format("%s,",  "done")); // cb_state
        bw.append(String.format("%s,",  1)); // cb_version
        bw.append(String.format("%s,",  1000)); // merchant_id
        bw.append(String.format("%s,",  "Thomas Jefferson and James Madison met in 1776. Could it have been any other year? They worked together starting then to further American Revolution and later to shape the new scheme of government. From the work sprang a friendship perhaps incomparable in intimacy1 and the trustfulness of collaboration2 and induration. It lasted 50 years. It included pleasure and utility but over and above them, there were shared purpose, a common end and ")); // merchant_name
        bw.append(String.format("%s,",   1)); // merchant_cat
        bw.append(String.format("%s,",  2)); // merchant_cat_sub
        bw.append(String.format("%s,",  "s1")); // store_id
        bw.append(String.format("%s,",  "Thomas Jefferson and James Madison met in 1776. Could it have been any other year? They worked together starting then to further American Revolution and later to shape the new scheme of government. From the work sprang a friendship perhaps incomparable in intimacy1 and the trustfulness of collaboration2 and induration. It lasted 50 years. It included pleasure and utility but over and above them, there were shared purpose, a common end and ")); // store_name
        bw.append(String.format("%s,",  "pos1"));// pos_id
        bw.append(String.format("%s,",  1));// biller_id
        bw.append(String.format("%s,",  "Thomas Jefferson and James Madison met in 1776. Could it have been any other year? They worked together starting then to further American Revolution and later to shape the new scheme of government. From the work sprang a friendship perhaps incomparable in intimacy1 and the trustfulness of collaboration2 and induration. It lasted 50 years. It included pleasure and utility but over and above them, there were shared purpose, a common end and an enduring goodness on both sides. Four and a half months before he died, when he was ailing3, debt-ridden, and worried about his impoverished4 family, Jefferson wrote to his longtime friend. His words and Madison's reply remind us that friends are friends until death. They also remind us that sometimes a friendship has a bearing on things larger than the friendship itself, for has there ever been a friendship of greater public consequence than this one?Thomas Jefferson and James Madison met in 1776. Could it have been any other year? They worked together starting then to further American Revolution and later to shape the new scheme of government. From the work sprang a friendship perhaps incomparable in intimacy1 and the trustfulness of collaboration2 and induration. It lasted 50 years. It included pleasure and utility but over and above them, there were shared purpose, a common end and an enduring goodness on both sides. Four and a half months before he died, when he was ailing3, debt-ridden, and worried about his impoverished4 family, Jefferson wrote to his longtime friend. His words and Madison's reply remind us that friends are friends until death. They also remind us that sometimes a friendship has a bearing on things larger than the friendship itself, for has there ever been a friendship of greater public consequence than this one? "));// logo_url
        bw.append(String.format("%s,",  2));// peer_id
        bw.append(String.format("%s,",  "Thomas Jefferson and James Madison met in 1776. Could it have been any other year? They worked together starting then to further American Revolution and later to shape the new scheme of government. From the work sprang a friendship perhaps incomparable in intimacy1 and the trustfulness of collaboration2 and induration. It lasted 50 years. It included pleasure and utility but over and above them, there were shared purpose, a common end and "));// peer_name
        bw.append(String.format("%s,",  "Thomas Jefferson and James Madison met in 1776. Could it have been any other year? They worked together starting then to further American Revolution and later to shape the new scheme of government. From the work sprang a friendship perhaps incomparable in intimacy1 and the trustfulness of collaboration2 and induration. It lasted 50 years. It included pleasure and utility but over and above them, there were shared purpose, a common end and "));// device_id
        bw.append(String.format("%s",  "{}"));// extra_info

        bw.append("\r");

        }
//        inPstmt.executeBatch();
//        inPstmt.clearBatch();
//    }
}

class DBStruct {
    long txnId;
    long userId;
    String txnType;
    String txnState;
    long txn_order_amount;
    String txn_order_currency;
    long txn_charge_amount;
    String txn_charge_currency;
    long txn_exchange_amount;
    String txn_exchange_currency;
    long txn_promo_amount;
    String txn_promo_currency;
    int disabled;
    int version;
    long order_id;
    String order_state;
    String order_error_code;
    String order_type;
    long order_version;
    String order_items;
    long payment_id;
    long payment_version;
    String payment_state;

    String payment_error_code;
    String comments;
    String sub_payments;
    long cd_amount;

    String cb_state;
    int cb_version;
    int merchant_id;
    String merchant_name;

    long merchant_cat;
    long merchant_cat_sub;
    String store_id;
    String store_name;

    String pos_id;
    long biller_id;
    String logo_url;
    long peer_id;

    String peer_name;

    String device_id;
    String extra_info;
}
