package com.commercial.tablename;

import java.util.Map;

/**
 * packageName com.commercial.tablename
 *
 * @author yanxuechao
 * @version JDK 8
 * @className user_coin_wallet_log
 * @date 2024/7/2
 * @description TODO
 */
public class user_coin_wallet_log {
    public String database;
    public String table;
    public String type;
    public long ts;
    public long xid;
    public boolean commit;
    public Map<String, Object> data; // 可以是更具体的类，这里为了简化使用 Map
    public Map<String, Object> old; // 同样可以是更具体的类

    public user_coin_wallet_log() {

    }

    public user_coin_wallet_log(String database, String table, String type, long ts, long xid, boolean commit, Map<String, Object> data, Map<String, Object> old) {
        this.database = database;
        this.table = table;
        this.type = type;
        this.ts = ts;
        this.xid = xid;
        this.commit = commit;
        this.data = data;
        this.old = old;
    }

    public String getDatabase() {
        return database;
    }

    public void setDatabase(String database) {
        this.database = database;
    }

    public String getTable() {
        return table;
    }

    public void setTable(String table) {
        this.table = table;
    }

    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }

    public long getTs() {
        return ts;
    }

    public void setTs(long ts) {
        this.ts = ts;
    }

    public long getXid() {
        return xid;
    }

    public void setXid(long xid) {
        this.xid = xid;
    }

    public boolean isCommit() {
        return commit;
    }

    public void setCommit(boolean commit) {
        this.commit = commit;
    }

    public Map<String, Object> getData() {
        return data;
    }

    public void setData(Map<String, Object> data) {
        this.data = data;
    }

    public Map<String, Object> getOld() {
        return old;
    }

    public void setOld(Map<String, Object> old) {
        this.old = old;
    }

    @Override
    public String toString() {
        return "user_coin_wallet_log{" +
                "database='" + database + '\'' +
                ", table='" + table + '\'' +
                ", type='" + type + '\'' +
                ", ts=" + ts +
                ", xid=" + xid +
                ", commit=" + commit +
                ", data=" + data +
                ", old=" + old +
                '}';
    }
}
