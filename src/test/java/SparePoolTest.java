import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import wong.spance.sparePool.SparePool;

import java.io.IOException;
import java.io.InputStream;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicInteger;

public class SparePoolTest {

    private List<String> result = Collections.synchronizedList(new LinkedList<String>());

    @Before
    public void before() throws Exception {
        Logger.getLogger("SparePool.main").setLevel(Level.DEBUG);
        Logger.getLogger("SparePool.detail").setLevel(Level.DEBUG);
        Properties config = new Properties();
        try {
            InputStream in = Thread.currentThread().getContextClassLoader().getResourceAsStream("db-config.properties");
            config.load(in);
        } catch (IOException e) {
            e.printStackTrace();
        }
        SparePool.init(config);
    }

    @After
    public void after() throws Exception {
        System.out.println("==================================");
        System.out.println("notify to shutdown.");
        SparePool.shutdown();
        System.out.println("==================================");
        for (String item : result) {
            System.out.println(item);
        }
    }

    @Test
    public void testAll() throws Exception {
        final SparePool ssp = SparePool.getInstance();
        final AtomicInteger counter = new AtomicInteger(0);
        final int max = 50;
        final byte[] isOver = new byte[0];
        for (int i = 0; i < max; i++) {
            new Thread(new Runnable() {

                public void run() {
                    try {
                        Connection conn = ssp.getConnection();
                        Thread.sleep(1000);
                        String sql = "select to_char(SYSTIMESTAMP, 'DD-MON-YYYY HH24:MI:SSxFF') from dual";
                        Statement stm = conn.createStatement();
                        stm.execute(sql);
                        ResultSet rs = stm.getResultSet();
                        rs.next();
                        String data = rs.getString(1);
                        rs.close();
                        stm.close();
                        result.add(String.format("%s query=%s", Thread.currentThread().toString(), data));
                        sql = "select ? from dual";
                        PreparedStatement psm = conn.prepareStatement(sql);
                        psm.setString(1, Thread.currentThread().toString());
                        rs = psm.executeQuery();
                        rs.next();
                        data = rs.getString(1);
                        rs.close();
                        stm.close();
                        result.add(String.format("%s query=%s", Thread.currentThread().toString(), data));
                    } catch (Exception e) {
                        e.printStackTrace();
                    } finally {
                        ssp.free();
                    }
                    if (counter.incrementAndGet() >= max) {
                        synchronized (isOver) {
                            isOver.notifyAll();
                        }
                    }
                }
            }).start();
        }

        synchronized (isOver) {
            isOver.wait();
        }
    }
}