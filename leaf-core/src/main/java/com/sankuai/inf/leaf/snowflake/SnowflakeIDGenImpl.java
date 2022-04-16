package com.sankuai.inf.leaf.snowflake;

import com.google.common.base.Preconditions;
import com.sankuai.inf.leaf.IDGen;
import com.sankuai.inf.leaf.common.Result;
import com.sankuai.inf.leaf.common.Status;
import com.sankuai.inf.leaf.common.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Random;

/**
 * 使用位运算拼接一个long类型的id出来，主要是利用时间做高41位的内容，中间是10bit的机器id，也基本够用了，一个服务的id生成理论上不会超过1023个服务节点
 * 最后的12bit用来做递增
 */
public class SnowflakeIDGenImpl implements IDGen {

    @Override
    public boolean init() {
        return true;
    }

    private static final Logger LOGGER = LoggerFactory.getLogger(SnowflakeIDGenImpl.class);
    //开始时间戳
    //个数字可以指定为任意小于当前时间的数字，这样就能让竞争对手无法知道我们的id信息了，
    // 这就解决了基于mysql的segement方案造成的容易被竞争对手监控的问题了，
    // 因为有时间维度的参与，对手不知道我们每时每刻的id发放信息
    private final long twepoch;
    //wokerid占用的位数
    private final long workerIdBits = 10L;
    //worker最大的id1023
    private final long maxWorkerId = ~(-1L << workerIdBits);//最大能够分配的workerid =1023
    //每毫秒的id数字最大值
    private final long sequenceBits = 12L;
    //workerid的偏移量
    private final long workerIdShift = sequenceBits;
    //时间戳位移数
    private final long timestampLeftShift = sequenceBits + workerIdBits;
    //每个毫秒生成id数的掩码，用来进行与运算提高运算效率，他让自己的高位是1，
    // 其他都是0那么在与的时候如果没满则是sequence 如果是0说明与的那个数字二进制后12位全是0了，
    // 也就是满了，因此会休息一个死循环的时间然后继续生成id
    private final long sequenceMask = ~(-1L << sequenceBits);
    //工作节点的id
    private long workerId;
    //每个毫秒自增id数字
    private long sequence = 0L;
    //保存上次生成id时候的时间戳
    private long lastTimestamp = -1L;
    //new一个随机函数对象，多线程公用，并发性问题交给了synchronized关键字，并且公用对象后降低了new的成本
    private static final Random RANDOM = new Random();

    public SnowflakeIDGenImpl(String zkAddress, int port) {
        //Thu Nov 04 2010 09:42:54 GMT+0800 (中国标准时间)
        this(zkAddress, port, 1288834974657L);
    }

    /**
     *  初始化应用，主要是SnowflakeZookeeperHolder 里面的初始化，包括节点的创建，或者数据的同步，还有主要是完成时间的检查，方式工作节点始终回拨
     *  并且将workerid进行赋值，方便生成id时候使用
     * @param zkAddress zk地址
     * @param port      snowflake监听端口
     * @param twepoch   起始的时间戳
     */
    public SnowflakeIDGenImpl(String zkAddress, int port, long twepoch) {
        this.twepoch = twepoch;
        Preconditions.checkArgument(timeGen() > twepoch, "Snowflake not support twepoch gt currentTime");
        final String ip = Utils.getIp();
        SnowflakeZookeeperHolder holder = new SnowflakeZookeeperHolder(ip, String.valueOf(port), zkAddress);
        LOGGER.info("twepoch:{} ,ip:{} ,zkAddress:{} port:{}", twepoch, ip, zkAddress, port);
        boolean initFlag = holder.init();
        if (initFlag) {
            workerId = holder.getWorkerID();
            LOGGER.info("START SUCCESS USE ZK WORKERID-{}", workerId);
        } else {
            Preconditions.checkArgument(initFlag, "Snowflake Id Gen is not init ok");
        }
        Preconditions.checkArgument(workerId >= 0 && workerId <= maxWorkerId, "workerID must gte 0 and lte 1023");
    }

    /**
     * 核心方法这个就是获得雪花id的方法，因为是按照时间轴进行发布的，因此不存在不同的业务key的隔离，因为所有的业务的id都不会重复，（就是这么的任性）
     * 先取到系统时间戳，然后跟对象中的 lastTimestamp比较如果系统时间比对象时间回拨了5毫秒那么久稍作休息wait一下，也就是等待两倍的毫秒数，因为左移动1二进制翻一倍，
     * 如果线程醒过来后还是有偏移量那么就返回错误。如果偏移量超过5毫秒，那么代表着偏移量太大，那么就返回错误，
     * 如果对象中的 lastTimestamp 与当前机器中系统时间一样，这里面说明一下，这种情况下肯定是比较高的并发情况下的必然了，因为每次发放id后对象时间都会被置为当时取的系统时间
     * 也就是一个毫秒中会发憷多个id，那么处理逻辑就是给 sequence不停的加一，这里面的与其实就是2的12次方-1，也就是整了个sequence的最大值，这样出现0代表 sequence + 1变成了
     * 2的12次方-1了，那么也就是意味着并发真的很大，一毫秒中的id被打光了，那么系统就调用 tilNextMillis 进行死循环的等待，因为这种等待是毫秒级的，所以使用了while循环
     * 如果是新的毫秒是就生成一个随机数，作为sequence的新值，紧接着对lastTimestamp赋值，然后利用位运算生成一个long的id进行返回
     */
    @Override
    public synchronized Result get(String key) {
        long timestamp = timeGen();
        if (timestamp < lastTimestamp) {
            long offset = lastTimestamp - timestamp;
            if (offset <= 5) {
                try {
                    wait(offset << 1);
                    timestamp = timeGen();
                    if (timestamp < lastTimestamp) {
                        return new Result(-1, Status.EXCEPTION);
                    }
                } catch (InterruptedException e) {
                    LOGGER.error("wait interrupted");
                    return new Result(-2, Status.EXCEPTION);
                }
            } else {
                return new Result(-3, Status.EXCEPTION);
            }
        }
        if (lastTimestamp == timestamp) {
            sequence = (sequence + 1) & sequenceMask;
            if (sequence == 0) {
                //seq 为0的时候表示是下一毫秒时间开始对seq做随机
                sequence = RANDOM.nextInt(100);
                timestamp = tilNextMillis(lastTimestamp);
            }
        } else {
            //如果是新的ms开始
            sequence = RANDOM.nextInt(100);
        }
        lastTimestamp = timestamp;
        long id = ((timestamp - twepoch) << timestampLeftShift) | (workerId << workerIdShift) | sequence;
        return new Result(id, Status.SUCCESS);

    }

    /**
     * 通过死循环的形式确保时间进行了后移，因为最多也就是停留一毫秒，所以使用死循环的形式代价更低
     */
    protected long tilNextMillis(long lastTimestamp) {
        long timestamp = timeGen();
        while (timestamp <= lastTimestamp) {
            timestamp = timeGen();
        }
        return timestamp;
    }

    protected long timeGen() {
        return System.currentTimeMillis();
    }

    public long getWorkerId() {
        return workerId;
    }

}
