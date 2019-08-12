package com.pingcap.tidb.workload;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.Random;

public class Utils {

    private static Random random = new Random();
    private static char[] a = "ABCDEFGHIJKLMNOPSRSTUVWXYZabcdefghigklmnopsrstuvwxyz0123456789"
        .toCharArray();
    private static int byteLen = a.length;

    static String genRandStr(int size) {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < size; i++) {
            sb.append(a[random.nextInt(byteLen)]);
        }
        return sb.toString();
    }

    static String readFile(String path) {
        try {
            StringBuilder sb = new StringBuilder();
            char[] buf = new char[4096];
            BufferedReader br = new BufferedReader(new FileReader(path));
            int i = br.read(buf);
            while (i > 0) {
                sb.append(buf, 0, i);
                i = br.read(buf);
            }
            return sb.toString();
        } catch (Exception e) {
            e.printStackTrace();
            System.exit(0);
        }
        return null;
    }

    static String get(String httpurl) {
        HttpURLConnection connection = null;
        InputStream is = null;
        BufferedReader br = null;
        String result = null;//
        try {
            URL url = new URL(httpurl);
            connection = (HttpURLConnection) url.openConnection();
            connection.setRequestMethod("GET");
            connection.setConnectTimeout(15000);
            connection.setReadTimeout(60000);
            connection.connect();
            if (connection.getResponseCode() == 200) {
                is = connection.getInputStream();
                br = new BufferedReader(new InputStreamReader(is, "UTF-8"));
                StringBuilder sbf = new StringBuilder();
                String temp = null;
                while ((temp = br.readLine()) != null) {
                    sbf.append(temp);
                    sbf.append("\r\n");
                }
                result = sbf.toString();
            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            if (null != br) {
                try {
                    br.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
            if (null != is) {
                try {
                    is.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
            connection.disconnect();// 关闭远程连接
        }

        return result;
    }
}
