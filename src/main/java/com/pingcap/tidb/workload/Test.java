package com.pingcap.tidb.workload;

import com.pingcap.tidb.workload.utils.UidGenerator;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Date;
import java.util.Random;
import java.util.concurrent.CountDownLatch;
import java.util.stream.IntStream;


public class Test {

    public void test(int workId, int concurrency, int repeat, boolean isUpdate) throws Exception {
        if (isUpdate) {
            updateTest(workId, concurrency, repeat);
        } else {
            long start = System.currentTimeMillis();
            CountDownLatch wg = new CountDownLatch(concurrency);
            IDGenerator[] generators = new IDGenerator[concurrency];
            CountDownLatch tmpwg = new CountDownLatch(concurrency);
            System.out.println("start insert id");
            for (int i = 0; i < concurrency; i++) {
                final int threadID = i;
                new Thread(() -> {
                    final UidGenerator uidGenerator = new UidGenerator(30, 20, 13);
                    uidGenerator.setWorkerId(workId + 100 + threadID);
                    try {
                        generators[threadID] = new IDGenerator(uidGenerator, repeat);
                    } catch (Exception e) {
                        e.printStackTrace();
                    } finally {
                        tmpwg.countDown();
                    }
                }).start();
            }
            tmpwg.await();
            System.out.println("id generation done");

            for (int i = 0; i < concurrency; i++) {
                final int threadID = i;
                new Thread(new Runnable() {
                    PrimaryIDCache idCache = new PrimaryIDCache();
                    IDGenerator idGenerator = generators[threadID];
                    final UidGenerator uidGenerator = new UidGenerator(30, 20, 13);

                    @Override
                    public void run() {
                        uidGenerator.setWorkerId(workId + 100 + threadID);
                        System.out.println("thread " + threadID + " start");
                        try {
                            System.out.println(
                                "thread " + threadID + " done" + ", repeat=" + repeat + ",insert="
                                    + mixSelectAfterInsertTest(repeat, idCache, idGenerator));
                        } catch (Exception e) {
                            e.printStackTrace();
                        } finally {
                            wg.countDown();
                        }

                    }
                }).start();
            }
            wg.await();
            System.out.println("All done, use " + (System.currentTimeMillis() - start) + "ms");
        }
    }

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
    private static final String selectSQL = "select * from txn_history where user_id = ? and order_id = ?";
    private static final String updateSQL = "update  txn_history set "
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
        + "extra_info=? where user_id =? and order_id =? ";

    public int mixSelectAfterInsertTest(int repeat, PrimaryIDCache idCache, IDGenerator idGenerator)
        throws Exception {
        int insertCount = 0;
        Connection conn = null;
        try {
            conn = getConnection();
            final PreparedStatement inPstmt = conn.prepareStatement(insertSQL);
            for (int i = 0; i < repeat; i++) {
                try {
//                conn.setAutoCommit(false);
//                final PreparedStatement selPstmt = conn.prepareStatement(selectSQL);
//                    final PreparedStatement updateStmt = conn.prepareStatement(updateSQL);

                    PrimaryID pid = getRowIds(i, idCache, idGenerator);
                    insert(inPstmt, idGenerator, pid);
                    insertCount++;
                    if (repeat % 500 ==0) {
                        System.out.println(Thread.currentThread().getId() +"  " +new Date() + "  add batch done" );
                    }
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            closeConnection(conn);
        }

        return insertCount;
    }

    public static int BATCH_SIZE = 10;
    public void insert(PreparedStatement inPstmt, IDGenerator idGenerator, PrimaryID pid)
        throws SQLException {
        for (int i = 0; i < BATCH_SIZE; i++) {
            inPstmt.setLong(1, idGenerator.getTxnId()); // txn_id
            inPstmt.setLong(2, pid.userId); //user_id
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
            inPstmt.setLong(15, pid.orderId); // order_id
            inPstmt.setString(16, "started"); // order_state
            inPstmt.setString(17, "code1"); // order_error_code
            inPstmt.setString(18, "type1"); // order_type
            inPstmt.setLong(19, 4); // order_version
            inPstmt.setString(20, "{}");// order_items
            inPstmt.setLong(21, idGenerator.getPaymentId()); // payment_id
            inPstmt.setLong(22, 5); // payment_version
            inPstmt.setString(23, "paid");// payment_state
            inPstmt.setString(24, "c1");// payment_error_code
            inPstmt.setString(25, "{}");// comments
            inPstmt.setString(26, "{}");// sub_payments
            inPstmt.setLong(27, 1); // cd_amount
            inPstmt.setString(28, "done"); // cb_state
            inPstmt.setInt(29, 1); // cb_version
            inPstmt.setInt(30, 1000); // merchant_id
            inPstmt.setString(31, "mname"); // merchant_name
            inPstmt.setLong(32, 1); // merchant_cat
            inPstmt.setLong(33, 2); // merchant_cat_sub
            inPstmt.setString(34, "s1"); // store_id
            inPstmt.setString(35, "store x"); // store_name
            inPstmt.setString(36, "pos1");// pos_id
            inPstmt.setLong(37, 1);// biller_id
            inPstmt.setString(38, "http://11.com/1");// logo_url
            inPstmt.setLong(39, 2);// peer_id
            inPstmt.setString(40, "p1");// peer_name
            inPstmt.setLong(41, 99);// device_id
            inPstmt.setString(42, "{}");// extra_info

            inPstmt.addBatch();
        }

        inPstmt.execute();

        inPstmt.clearParameters();
    }


    public PrimaryID getRowIds(int cycleIndex, PrimaryIDCache idCache, IDGenerator idGenerator) {
//        if (cycleIndex % 2 == 0) {
            return idGenerator.getPrimaryID();
//        } else {
//        return idCache.getRandom();
//        }
    }



    static class PrimaryIDCache {

        ArrayList<Long> userids = new ArrayList<>();
        ArrayList<Long> orderids = new ArrayList<>();
        Random random;

        public PrimaryIDCache() {
            random = new Random();
        }

        public void Add(long userId, long orderId) {
            userids.add(userId);
            orderids.add(orderId);
        }

        public PrimaryID getRandom() {
            int index = random.nextInt(userids.size());
            return new PrimaryID(userids.get(index), orderids.get(index));
        }
    }

    static class IDGenerator {

        ArrayList<PrimaryID> primaryIDS = new ArrayList<>();
        ArrayList<Long> txnIds = new ArrayList<>();
        ArrayList<Long> paymentIds = new ArrayList<>();

        public IDGenerator(UidGenerator uidGenerator, int repeat) {
            IntStream.range(0, repeat * BATCH_SIZE + 10).mapToLong(id -> uidGenerator.getUID())
                .peek(lid -> {
                    txnIds.add(lid);
                }).peek(lid -> {
                paymentIds.add(lid + +new Random().nextInt(10000000));
            }).forEach(lid -> {
                PrimaryID pid = new PrimaryID(lid + +new Random().nextInt(10000000),
                    lid + +new Random().nextInt(10000000));
                primaryIDS.add(pid);
            });

//                    .mapToObj(id -> {
//                return new PrimaryID(uidGenerator);
//            }).peek((id) -> {
//                txnIds.add(id.userId + + new Random().nextInt(10000000));
//            }).peek((id) -> {
//                paymentIds.add(id.orderId + + new Random().nextInt(10000000));
//            }).forEach(pid -> {
////                PrimaryID pid = new PrimaryID(uidGenerator);
//                primaryIDS.add(pid);
//            });
        }

        public PrimaryID getPrimaryID() {
            if (!primaryIDS.isEmpty()) {
                return primaryIDS.remove(0);
            }
            throw new RuntimeException("orderId and userId use up");
        }

        public long getTxnId() {
            if (!txnIds.isEmpty()) {
                return txnIds.remove(0);
            }

            throw new RuntimeException("txn id use up");
        }

        public long getPaymentId() {
            if (!paymentIds.isEmpty()) {
                return paymentIds.remove(0);
            }

            throw new RuntimeException("payment id use up");
        }
    }

    static class PrimaryID {

        long userId;
        long orderId;

//        public PrimaryID(UidGenerator uidGenerator) {
//            userId = uidGenerator.getUID();
//            orderId = userId + new Random().nextInt(10000000);
//        }

        public PrimaryID(long userId, long orderId) {
            this.userId = userId;
            this.orderId = orderId;
        }
    }

    private long[] userIds = new long[100000];
    private long[] orderIds = new long[100000];
    private long[] txnIds = new long[100000];
    private long[] paymentIds = new long[100000];

    private void updateTest(int workId, int concurrency, int repeat) throws Exception {
        final UidGenerator uidGenerator = new UidGenerator(30, 20, 13);
        uidGenerator.setWorkerId(workId);
        long start = System.currentTimeMillis();
        System.out.println("start to insert base data");

        for (int i = 0; i < userIds.length; i++) {
            userIds[i] = uidGenerator.getUID();
            orderIds[i] = uidGenerator.getUID();
            txnIds[i] = uidGenerator.getUID();
            paymentIds[i] = uidGenerator.getUID();
        }
        System.out.println("ids  generation done");

        int threadPoolSize = 100;
        CountDownLatch insertGroup = new CountDownLatch(threadPoolSize);
        for (int c = 0; c < threadPoolSize; c++) {
            final int threadId = c;
            new Thread(() -> {
                Connection conn = null;
                try {
                    conn = getConnection();
                    final PreparedStatement inPstmt = conn.prepareStatement(insertSQL);
                    System.out.println(threadId);
                    for (int i = 0; i < userIds.length / threadPoolSize; i++) {
                        try {
                            int index = threadId * (userIds.length / threadPoolSize) + i;
                            long txnId = txnIds[index];
                            long userId = userIds[index];
                            long orderId = orderIds[index];
                            long paymentId = paymentIds[index];
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
                            inPstmt.setLong(21, paymentId); // payment_id
                            inPstmt.setLong(22, 5); // payment_version
                            inPstmt.setString(23, "paid");// payment_state
                            inPstmt.setString(24, "c1");// payment_error_code
                            inPstmt.setString(25, "{}");// comments
                            inPstmt.setString(26, "{}");// sub_payments
                            inPstmt.setLong(27, 1); // cd_amount
                            inPstmt.setString(28, "done"); // cb_state
                            inPstmt.setInt(29, 1); // cb_version
                            inPstmt.setInt(30, 1000); // merchant_id
                            inPstmt.setString(31, "mname"); // merchant_name
                            inPstmt.setLong(32, 1); // merchant_cat
                            inPstmt.setLong(33, 2); // merchant_cat_sub
                            inPstmt.setString(34, "s1"); // store_id
                            inPstmt.setString(35, "store x"); // store_name
                            inPstmt.setString(36, "pos1");// pos_id
                            inPstmt.setLong(37, 1);// biller_id
                            inPstmt.setString(38, "http://11.com/1");// logo_url
                            inPstmt.setLong(39, 2);// peer_id
                            inPstmt.setString(40, "p1");// peer_name
                            inPstmt.setLong(41, 99);// device_id
                            inPstmt.setString(42, "{}");// extra_info
                            inPstmt.execute();
                            inPstmt.clearParameters();

                        } catch (Exception e) {
                            e.printStackTrace();
                        }
                    }
                } catch (Exception e) {
                    e.printStackTrace();
                } finally {
                    closeConnection(conn);
                    insertGroup.countDown();
                }
            }).start();
        }
        insertGroup.await();

        System.out.println("insert data done: " + (System.currentTimeMillis() - start) + "ms");

        CountDownLatch tmpwg = new CountDownLatch(concurrency);
        for (int i = 0; i < concurrency; i++) {
            new Thread(new Runnable() {
                @Override
                public void run() {
                    Connection conn = null;
                    try {
                        Random random = new Random();
                        conn = getConnection();
                        for (int i = 0; i < repeat; i++) {
                            try {

                                final PreparedStatement updateStmt = conn
                                    .prepareStatement(updateSQL);
                                conn.setAutoCommit(false);
                                int index = random.nextInt(userIds.length);
                                long txnId = txnIds[index];
                                long userId = userIds[index];
                                long orderId = orderIds[index];
                                long paymentId = paymentIds[index];

                                setUpdateParam(updateStmt, txnId, userId, orderId, paymentId);
                                updateStmt.execute();
                                updateStmt.clearParameters();
                                conn.commit();
                            } catch (Exception e) {
                                e.printStackTrace();
                            }
                        }
                    } catch (Exception e) {
                        e.printStackTrace();
                    } finally {
                        closeConnection(conn);
                        tmpwg.countDown();
                    }
                }
            }).start();
        }
        tmpwg.await();
        System.out.println("All done, use " + (System.currentTimeMillis() - start) + "ms");
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
        updateStmt.setLong(43, userId); //user_id
        updateStmt.setLong(44, orderId); // order_id
    }

    private Connection getConnection() throws SQLException {
        return DbUtil.getInstance().getConnection();
    }

    private void closeConnection(Connection conn) {
        if (conn != null) {
            try {
                DbUtil.getInstance().closeConnection(conn);
//                conn.close();
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
    }
}