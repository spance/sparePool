package wong.spance.sparePool;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.*;

public class ConnectionDetail {

    private final static Logger log = LoggerFactory.getLogger("SparePool.detail");

    private boolean inPool;

    private Date lastGetTime;

    private Date lastFreeTime;

    private long lastTimeUsed;

    private int usedCount;

    private Map<Statement, Deque<ResultSet>> resource;

    public ConnectionDetail() {
        this.inPool = true;
        this.usedCount = 0;
        this.resource = new HashMap<Statement, Deque<ResultSet>>();
    }

    public boolean isInPool() {
        return inPool;
    }

    public void setInPool(boolean inPool) {
        this.inPool = inPool;
    }

    public Date getLastGetTime() {
        return lastGetTime;
    }

    public void setLastGetTime() {
        this.lastGetTime = new Date();
        this.usedCount++;
    }

    public Date getLastFreeTime() {
        return lastFreeTime;
    }

    public void setLastFreeTime() {
        this.lastFreeTime = new Date();
    }

    public long getLastTimeUsed() {
        return lastTimeUsed;
    }

    public void setLastTimeUsed(long lastTimeUsed) {
        this.lastTimeUsed = lastTimeUsed;
    }

    public int getUsedCount() {
        return usedCount;
    }

    public void markStatement(Statement statement) {
        this.resource.put(statement, new LinkedList<ResultSet>());
    }

    public void markResultSet(Statement statement, ResultSet resultSet) {
        Deque<ResultSet> stm = this.resource.get(statement);
        if (stm != null && resultSet != null) {
            stm.push(resultSet);
        } else
            throw new IllegalArgumentException("Statement | ResultSet is null");
    }

    public void closeStatement(Statement statement) {
        try {
            if (!statement.isClosed()) {
                closeAllResultSet(statement);
                statement.close();
                log.debug("statement closed.");
            }
            resource.remove(statement);
        } catch (SQLException e) {
            log.error("", e);
        }
    }

    public void closeAllStatement() {
        while (!resource.isEmpty()) {
            closeStatement(resource.keySet().iterator().next());
        }
    }

    public void closeResultSet(ResultSet resultSet) {
        try {
            if (!resultSet.isClosed()) {
                resultSet.close();
                log.debug("resultSet closed.");
            }
        } catch (SQLException e) {
            log.error("", e);
        }
    }

    public void closeAllResultSet(Statement statement) {
        Deque<ResultSet> stm = this.resource.get(statement);
        while (!stm.isEmpty()) {
            closeResultSet(stm.pop());
        }
    }
}

