package com.flink.apache.state;

import org.rocksdb.Options;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;

public class RocksDbTest {
    static {
        //默认这个方法会加载一个共享库到java.io.tempdir
        RocksDB.loadLibrary();
    }
    public static void main(String[] args) throws RocksDBException {

        // 1. 打开数据库
        // 1.1 创建数据库配置
        Options dbOpt = new Options();
        // 1.2 配置当数据库不存在时自动创建
        dbOpt.setCreateIfMissing(true);
        // 1.3 打开数据库, 因为RocksDb默认是保存在本地磁盘, 所以需要指定位置
        RocksDB rdb = RocksDB.open(dbOpt, "a_data/rocksdb");

        // 2. 写入数据库
        // 2.1 RocksDB都是以字节流方式写入数据库中, 所以我们需要将字符串转换成字节流再写入, 这点类似HBase
        byte[] key = "阿里巴巴".getBytes();
        byte[] value = "100亿".getBytes();
        // 2.2 调用put方法写入数据
        rdb.put(key, value);
        System.out.println("写入数据到RocksDB完成！");

        // 3. 调用get读取数据
        System.out.println("从RocksDB读取key = " + new String(key) + " 的value = " + new String(rdb.get(key)));

        // 4. 移除数据
        rdb.delete(key);

        //5. 关闭资源
        rdb.close();
        dbOpt.close();
    }
}
