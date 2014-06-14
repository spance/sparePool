package wong.spance.sparePool;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.sql.*;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

/**
 * 定位于短生命周期的连接池，一种典型情况为主连接池之外需要其它策略的任务执行等。当然也适用简单应用长生命周期等。<br/>
 * 池提供了一致简单的资源释放方法，记住free就够了！<br/>
 * 用户可以不必关闭Statement和ResultSet，在获得Connection的线程内调用free()即可释放所有相关资源。<br/>
 * 池不负责连接超时和泄露管理，池在关闭时将释放所有资源-Connection,Statement,ResultSet.<br/>
 * 一系列任务完成并其它线程不再资源时可以关闭池SparePool.shutdown()
 *
 * @author Spance Wong
 * @version 0.1
 * @since 2013/10/1
 */
public class SparePool {

    private static SparePool INSTANCE;
    private static Logger log = LoggerFactory.getLogger("SparePool.main");
    private ConcurrentLinkedQueue<Connection> freePool;
    private ConcurrentLinkedQueue<Connection> activePool;
    private ConcurrentLinkedQueue<Thread> activeThread;
    private Map<Connection, ConnectionDetail> detailMap;
    private ThreadLocal<Connection> localConn;
    private ReentrantLock lock;
    private Condition isLack;
    private volatile boolean isReady;
    private AtomicInteger activeCount;
    private AtomicInteger freeCount;
    private AtomicInteger waitingCount;
    private String url;
    private String user;
    private String password;
    private int maxWait;
    private int maxSize;
    private int initSize;

    private SparePool(Properties config) {
        this.url = config.getProperty("url");
        this.user = config.getProperty("username");
        this.password = config.getProperty("password");
        this.maxWait = toInt(config.getProperty("maxWait"), 30000) / 1000;
        this.maxSize = 20;
        this.initSize = 6;
        this.freePool = new ConcurrentLinkedQueue<Connection>();
        this.activePool = new ConcurrentLinkedQueue<Connection>();
        this.activeThread = new ConcurrentLinkedQueue<Thread>();
        this.detailMap = Collections.synchronizedMap(new HashMap<Connection, ConnectionDetail>());
        this.localConn = new ThreadLocal<Connection>();
        this.activeCount = new AtomicInteger(0);
        this.freeCount = new AtomicInteger(0);
        this.waitingCount = new AtomicInteger(0);
        this.lock = new ReentrantLock();
        this.isLack = this.lock.newCondition();
        this.isReady = true;
    }

    /**
     * 开池
     */
    public static synchronized void init(Properties config) {
        if (INSTANCE != null) {
            throw new IllegalStateException("already init");
        }
        INSTANCE = new SparePool(config);
        try {
            for (int i = 0; i < INSTANCE.initSize; i++) {
                SparePool.getInstance().createConnection();
            }
            log.info("SparePool 初始化 size={}", INSTANCE.initSize);
        } catch (Exception e) {
            log.error("SparePool 初始化错误", e);
        }
    }

    /**
     * 关池
     */
    public static synchronized void shutdown() {
        INSTANCE.isReady = false;
        log.info("开始销毁 SparePool{}", INSTANCE.statistics());
        while (!INSTANCE.freePool.isEmpty()) {
            Connection _conn = INSTANCE.freePool.poll();
            INSTANCE.release(_conn);
        }
        while (!INSTANCE.activeThread.isEmpty()) {
            Thread thread = INSTANCE.activeThread.poll();
            thread.interrupt();
        }
        while (!INSTANCE.activePool.isEmpty()) {
            Connection _conn = INSTANCE.activePool.poll();
            INSTANCE.release(_conn);
        }
        INSTANCE = null;
        log.debug("OK. Shutdown!");
        /** System.gc(); */
    }

    /**
     * 取得实例
     *
     * @return
     */
    public static SparePool getInstance() {
        if (INSTANCE == null || !INSTANCE.isReady)
            throw new IllegalStateException("pool is shutdown.");
        return INSTANCE;
    }

    /**
     * 主要的方法，取得线程内连接
     *
     * @return
     */
    public Connection getConnection() throws SQLException {
        if (!isReady)
            throw new IllegalStateException("pool is shutdown.");
        activeThread.offer(Thread.currentThread());
        try {
            lock.lockInterruptibly();
        } catch (InterruptedException e) {
            throw new DBException(e);
        }
        try {
            Connection conn = localConn.get();
            if (conn == null || conn.isClosed()) {
                conn = getConnectionDirectly();
                localConn.set(conn);
            }
            return conn;
        } finally {
            lock.unlock();
        }
    }

    private Connection getConnectionDirectly() throws SQLException {
        for (; ; ) {
            if (activeCount.get() < maxSize || freeCount.get() > 0) {
                Connection conn;
                byte i = 1;
                for (; ((conn = freePool.poll()) == null || conn.isClosed()) && i > 0; --i) {
                    this.createConnection();
                }
                if (conn != null) {
                    freeCount.decrementAndGet();
                    activePool.offer(conn);
                    activeCount.incrementAndGet();
                    ConnectionDetail cd = detailMap.get(conn);
                    assert cd != null;
                    cd.setInPool(false);
                    cd.setLastGetTime();
                    if (log.isDebugEnabled())
                        log.debug("(-) {}{}", i == 0 ? "reuse-old" : "create-new", statistics());
                    return conn;
                }
                throw new DBException("Can't create new Connection !!!");
            } else {
                waitingCount.incrementAndGet();
                if (log.isDebugEnabled())
                    log.debug("before-await {}{}", Thread.currentThread(), statistics());
                boolean await = false;
                try {
                    await = isLack.await(maxWait, TimeUnit.SECONDS);
                } catch (InterruptedException e) {
                    throw new DBException(e);
                }
                if (await) {
                    waitingCount.decrementAndGet();
                } else
                    throw new DBException("达到最大连接数%d在等待%ds无可用连接!%s", maxSize, maxWait, statistics());
            }
        }
    }

    private void createConnection() throws SQLException {
        Connection conn = DriverManager.getConnection(url, user, password);
        if (conn != null) {
            ConnectionDetail cd = new ConnectionDetail();
            freePool.offer(new ConnectionWrapper(conn, cd).wrapper());
            freeCount.incrementAndGet();
            detailMap.put(conn, cd);
        }
    }

    /**
     * 释放本线程资源，最便捷的方式，推荐！
     */
    public static void freeLocal() {
        INSTANCE.free();
    }

    /**
     * 释放本线程资源，最便捷的方式，推荐！
     */
    public void free() {
        Connection _conn = localConn.get();
        if (_conn != null) {
            localConn.remove();
            release(_conn);
            activeThread.remove(Thread.currentThread());
        }
    }

    /**
     * 跨线程释放那就调用吧释放吧。
     *
     * @param conn
     */
    public void release(Connection conn) {
        if (null == conn) return;
        boolean available = isReady;
        if (available) {
            ConnectionDetail detail = detailMap.get(conn);
            detail.closeAllStatement();
            detail.setLastFreeTime();
            detail.setInPool(true);
            try {
                if (!conn.isClosed())
                    conn.setAutoCommit(true);
                else {
                    available = false;
                    detailMap.remove(conn);
                    log.warn("the connection closed in the external");
                }
            } catch (SQLException e) {
                log.error("", e);
            }
            if (activePool.remove(conn))
                activeCount.decrementAndGet();
            if (available && freePool.offer(conn))
                freeCount.incrementAndGet();
            if (log.isDebugEnabled())
                log.debug("(+) release into freePool{}", statistics());
            try {
                lock.lockInterruptibly();
            } catch (InterruptedException e) {
                throw new DBException(e);
            }
            try {
                isLack.signal();
            } finally {
                lock.unlock();
            }
        } else {        // kill connection
            try {
                detailMap.remove(conn);
                conn.close();
            } catch (SQLException e) {
                log.warn("", e);
            }
        }
    }

    private String statistics() {
        return String.format("; Stat{A/F/W=%d/%d/%d}",
                activeCount.get(), /** permits.availablePermits(), */freeCount.get(), waitingCount.get());
    }

    /**
     * 对Connection代理 拦截close操作 并对产生的Statement进行代理
     */
    static class ConnectionWrapper implements InvocationHandler {

        private final Connection conn;
        private final ConnectionDetail connectionDetail;

        private ConnectionWrapper(Connection conn, ConnectionDetail connectionDetail) {
            this.conn = conn;
            this.connectionDetail = connectionDetail;
        }

        private Connection wrapper() {
            return (Connection) Proxy.newProxyInstance(
                    conn.getClass().getClassLoader(),
                    new Class[]{Connection.class},/** conn.getClass().getInterfaces(), */
                    this);
        }

        @Override
        public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {
            String methodName = method.getName();
            if (methodName.equals("close") && INSTANCE.isReady) {
                INSTANCE.release(conn);
                return null;
            }
            if (methodName.equals("equals")) {
                Object arg = args[0];
                return Proxy.isProxyClass(arg.getClass()) ? proxy == arg : conn == arg || conn.equals(arg);
            }
            if (connectionDetail.isInPool() && !methodName.matches("(?:isClosed|close|hashCode|setAutoCommit)"))
                throw new DBException("Can't call method[%s] when the Connection has been recovered!", methodName);
            Class<?> returnType = method.getReturnType();
            Object obj = null;
            try {
                obj = method.invoke(conn, args);
                if (returnType != null && Statement.class.isAssignableFrom(returnType) && obj != null) {
                    connectionDetail.markStatement((Statement) obj);
                    return new StatementWrapper((Statement) obj, connectionDetail).wrapper();
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
            return obj;
        }
    }

    /**
     * 代理Statement 拦截close操作 并对操作产生的ResultSet记录 应对统一资源释放
     */
    static class StatementWrapper implements InvocationHandler {

        private final ConnectionDetail connectionDetail;
        private final Statement statement;

        private StatementWrapper(Statement statement, ConnectionDetail connectionDetail) {
            this.statement = statement;
            this.connectionDetail = connectionDetail;
        }


        private Statement wrapper() {
            return (Statement) Proxy.newProxyInstance(
                    statement.getClass().getClassLoader(),
                    statement.getClass().getInterfaces(),
                    this);
        }

        @Override
        public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {
            String methodName = method.getName();
            if (methodName.equals("close")) {
                connectionDetail.closeStatement(statement);
                return null;
            }
            Class<?> returnType = method.getReturnType();
            Object obj = null;
            try {
                obj = method.invoke(statement, args);
                if (returnType != null && ResultSet.class.isAssignableFrom(returnType) && obj != null) {
                    connectionDetail.markResultSet(statement, (ResultSet) obj);
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
            return obj;
        }
    }

    public static int toInt(String property, int defaultValue) {
        try {
            if (property != null)
                defaultValue = Integer.parseInt(property);
        } catch (Exception ignored) {
        }
        return defaultValue;
    }
}
