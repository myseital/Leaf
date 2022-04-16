package com.sankuai.inf.leaf.segment;

import com.sankuai.inf.leaf.IDGen;
import com.sankuai.inf.leaf.common.Result;
import com.sankuai.inf.leaf.common.Status;
import com.sankuai.inf.leaf.segment.dao.IDAllocDao;
import com.sankuai.inf.leaf.segment.model.*;
import org.perf4j.StopWatch;
import org.perf4j.slf4j.Slf4JStopWatch;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;

public class SegmentIDGenImpl implements IDGen {
    private static final Logger logger = LoggerFactory.getLogger(SegmentIDGenImpl.class);

    /**
     * IDCache未初始化成功时的异常码
     */
    private static final long EXCEPTION_ID_IDCACHE_INIT_FALSE = -1;
    /**
     * key不存在时的异常码
     */
    private static final long EXCEPTION_ID_KEY_NOT_EXISTS = -2;
    /**
     * SegmentBuffer中的两个Segment均未从DB中装载时的异常码
     */
    private static final long EXCEPTION_ID_TWO_SEGMENTS_ARE_NULL = -3;
    /**
     * 最大步长不超过100,0000
     */
    private static final int MAX_STEP = 1000000;
    /**
     * 一个Segment维持时间为15分钟
     * 这里指的是一个系统默认认为的合理时间，主要用于调整buffer里面步长的大小，如果当前次更新距离上次更新时间超过15分钟的话
     * 那么步长就会动态调整为二分之一之前的长度，如果说当次更新时间距离上次更新时间未超过15分钟那么说明系统压力大，那么就适当调整步长到2倍直到最大步长
     * MAX_STEP
     */
    private static final long SEGMENT_DURATION = 15 * 60 * 1000L;
    /**
     * 用来更新本地的buffer的线程池，用来更新每个tag对应的segmentBufferr里面备用的segment
     */
    private ExecutorService service = new ThreadPoolExecutor(5, Integer.MAX_VALUE, 60L, TimeUnit.SECONDS, new SynchronousQueue<Runnable>(), new UpdateThreadFactory());
    /**
     * 初始化状态，主要用来标记mysql中tag是否初次被同步进入内存中
     */
    private volatile boolean initOK = false;
    /**
     * 用来保存每个tag对应的segmentBuffer，业务通过tag进行隔离，并且此处使用了并发安全的容器，主要是防止在刷新tag的时候出现线程不安全的问题
     */
    private Map<String, SegmentBuffer> cache = new ConcurrentHashMap<String, SegmentBuffer>();
    /**
     * 主要是用来与mysql打交道，加载tag，加载step，更新maxid
     */
    private IDAllocDao dao;

    /**
     * 作者比较优秀，为了刷新线程起一个比较好听的名字特意写了个一个工厂，哈哈并且内部做了一个线程计数的变量
     */
    public static class UpdateThreadFactory implements ThreadFactory {

        private static int threadInitNumber = 0;

        private static synchronized int nextThreadNum() {
            return threadInitNumber++;
        }

        @Override
        public Thread newThread(Runnable r) {
            return new Thread(r, "Thread-Segment-Update-" + nextThreadNum());
        }
    }

    /**
     * 暴露给外部调用用来初始化分段id生成器的功能，主要包括更新所有的tag进入到内存中，并且启动一个单线程的守护线程去做定时刷新这些tag的操作，
     * 间隔60秒，这里之所以用单线程的线程池我个人的判断是为了充分利用阻塞的特性，因为在极端的情况下60秒加载不完那么就阻塞着在哪里，当然，绝大多数业务
     * 一分钟肯定是能够加载完的。
     */
    @Override
    public boolean init() {
        logger.info("Init ...");
        // 确保加载到kv后才初始化成功
        updateCacheFromDb();
        initOK = true;
        updateCacheFromDbAtEveryMinute();
        return initOK;
    }

    /**
     * 刷新缓存的方法。单线程每隔60秒刷新一次tag，与mysql 同步一次tag的信息
     */
    private void updateCacheFromDbAtEveryMinute() {
        ScheduledExecutorService service = Executors.newSingleThreadScheduledExecutor(new ThreadFactory() {
            @Override
            public Thread newThread(Runnable r) {
                Thread t = new Thread(r);
                t.setName("check-idCache-thread");
                t.setDaemon(true);
                return t;
            }
        });
        service.scheduleWithFixedDelay(new Runnable() {
            @Override
            public void run() {
                updateCacheFromDb();
            }
        }, 60, 60, TimeUnit.SECONDS);
    }

    /**
     * 从mysql中同步tag的信息进入内存中，这里作者做的也很巧妙，并不着急立马就去加载segment我们看到SegmentBuffer里面有一个初始化是否成功的标志字段
     * initOk 他标志着目前这个segmentBuffer是否可用，但是这个方法里面默认是false的，作者在这里巧妙的利用了懒加载的方式，将max的值的更改延后，因为我们思考一种弄场景
     * leaf在美团中可能是全集团公用的，可能部署了上百个节点，那么很有可能这些服务会面临重启，如果每次重启都会默认更新mysql的话，一方面会浪费非常多的step的id，另外一方面很有可能
     * 就算浪费了id也可能会用不到，因此这里面用户使用了懒加载的思想只是先进行占位，当用户在真正使用的时候再去查询并填充segment并更新mysql，因此这里面有个细节就是
     * 如果系统极端在乎平滑性，那么在leaf在对外提供服务前，先手动调用一次，以确保segment被填充完善，降低延时性。
     */
    private void updateCacheFromDb() {
        logger.info("update cache from db");
        StopWatch sw = new Slf4JStopWatch();
        try {
            List<String> dbTags = dao.getAllTags();
            if (dbTags == null || dbTags.isEmpty()) {
                return;
            }
            List<String> cacheTags = new ArrayList<String>(cache.keySet());
            Set<String> insertTagsSet = new HashSet<>(dbTags);
            Set<String> removeTagsSet = new HashSet<>(cacheTags);
            //db中新加的tags灌进cache
            for(int i = 0; i < cacheTags.size(); i++){
                String tmp = cacheTags.get(i);
                if(insertTagsSet.contains(tmp)){
                    insertTagsSet.remove(tmp);
                }
            }
            for (String tag : insertTagsSet) {
                SegmentBuffer buffer = new SegmentBuffer();
                buffer.setKey(tag);
                Segment segment = buffer.getCurrent();
                segment.setValue(new AtomicLong(0));
                segment.setMax(0);
                segment.setStep(0);
                cache.put(tag, buffer);
                logger.info("Add tag {} from db to IdCache, SegmentBuffer {}", tag, buffer);
            }
            //cache中已失效的tags从cache删除
            for(int i = 0; i < dbTags.size(); i++){
                String tmp = dbTags.get(i);
                if(removeTagsSet.contains(tmp)){
                    removeTagsSet.remove(tmp);
                }
            }
            for (String tag : removeTagsSet) {
                cache.remove(tag);
                logger.info("Remove tag {} from IdCache", tag);
            }
        } catch (Exception e) {
            logger.warn("update cache from db exception", e);
        } finally {
            sw.stop("updateCacheFromDb");
        }
    }

    /**
     * 主力接口，用于对外界提供id
     * 判断当前tag缓存是否已经就绪，如果未就绪直接报错，因此要求调用方应该先调用init(),进行基础环境的就绪
     * 缓存就绪成功，从缓存中查看客户端请求的key是否存在，不存在的可能有两种，一种是mysql中没有，这个需要等大概60秒才会刷新，因此在leaf使用过程中应该提前就绪好mysql，让让多个leaf服务都能刷新到相应的key
     * 另外一种可能就是mysql中也没有，当然也会造成cache中没有，两种情况造成的缓存中没有，系统都会返回key不存在，id生成失败
     * 如果缓存中也恰好查到了有key，那么就会因为懒加载的原因造成可能segmentBuffer没有初始化，（任何事情都有两面性）
     * 我们看到美团的处理方式是通过锁定对应的segmentBuffer的对象头，可以说也是无所不用其极的减低锁粒度，不得不说一句nice
     * 另外我们看到使用了双重检查，防止并发问题，这里多啰嗦一句为什么回出现并发问题，两个线程都到了synchronized的临界区后，一个线程拿到了buffer的头锁，进入可能去更新mysql了，如果他执行完他会放开头锁
     * 但是如果不通过判断那么他也会继续执行更新mysql的操作，因此造成不满足我们预期的事情发生了，所以这里通过一个initok的一个标志进行双重判定，那么就算是第二个线程进入后因为第一个线程退出前就更新了linitok为true
     * 所以第二个线程进来后还是不能更新mysql就安全出去临界区了。
     */
    @Override
    public Result get(final String key) {
        if (!initOK) {
            return new Result(EXCEPTION_ID_IDCACHE_INIT_FALSE, Status.EXCEPTION);
        }
        if (cache.containsKey(key)) {
            SegmentBuffer buffer = cache.get(key);
            if (!buffer.isInitOk()) {
                synchronized (buffer) {
                    if (!buffer.isInitOk()) {
                        try {
                            updateSegmentFromDb(key, buffer.getCurrent());
                            logger.info("Init buffer. Update leafkey {} {} from db", key, buffer.getCurrent());
                            buffer.setInitOk(true);
                        } catch (Exception e) {
                            logger.warn("Init buffer {} exception", buffer.getCurrent(), e);
                        }
                    }
                }
            }
            return getIdFromSegmentBuffer(cache.get(key));
        }
        return new Result(EXCEPTION_ID_KEY_NOT_EXISTS, Status.EXCEPTION);
    }

    /**
     * 这个方法主要是用来更新并填充好，指定key对应的SegmentBuffer
     * StopWatch 是一个计时器，作者考虑到这个方法的性能问题，因此加了一个监控
     * 先是判断指定的segmentBuffer是否初始化完成，如果没有初始化完成也就是说没有向数据库去申请id段，那么就去取申请并填充进segmentBuffer
     * 如果是已经初始化完成了，第二个分支其实特定指的是第二次申请
     */
    public void updateSegmentFromDb(String key, Segment segment) {
        StopWatch sw = new Slf4JStopWatch();
        SegmentBuffer buffer = segment.getBuffer();
        LeafAlloc leafAlloc;
        //第一次申请id段
        if (!buffer.isInitOk()) {
            leafAlloc = dao.updateMaxIdAndGetLeafAlloc(key);
            buffer.setStep(leafAlloc.getStep());
            buffer.setMinStep(leafAlloc.getStep());//leafAlloc中的step为DB中的step
        } else if (buffer.getUpdateTimestamp() == 0) {
            //第二次申请id段，因为之前的第一次申请动作谈不上更新，因此在第二次的时候将更新时间进行填充
            leafAlloc = dao.updateMaxIdAndGetLeafAlloc(key);
            buffer.setUpdateTimestamp(System.currentTimeMillis());
            buffer.setStep(leafAlloc.getStep());
            buffer.setMinStep(leafAlloc.getStep());//leafAlloc中的step为DB中的step
        } else {
            //第N次申请id段动态优化步长，我们看到有一个指定的时间以15分钟为例，如果两次领取间隔少于15分钟那么就将step拉大一倍，但是不会超过系统默认的10W的step
            // 这样做的好处其实也是降低mysql压力
            //如果两次申请的超过30分钟那么就将步长调整为原来的一半，但是不会小于最小步长
            long duration = System.currentTimeMillis() - buffer.getUpdateTimestamp();
            int nextStep = buffer.getStep();
            if (duration < SEGMENT_DURATION) {
                if (nextStep * 2 > MAX_STEP) {
                    //do nothing
                } else {
                    nextStep = nextStep * 2;
                }
            } else if (duration < SEGMENT_DURATION * 2) {
                //do nothing with nextStep
            } else {
                nextStep = nextStep / 2 >= buffer.getMinStep() ? nextStep / 2 : nextStep;
            }
            logger.info("leafKey[{}], step[{}], duration[{}mins], nextStep[{}]", key, buffer.getStep(), String.format("%.2f",((double)duration / (1000 * 60))), nextStep);
            LeafAlloc temp = new LeafAlloc();
            temp.setKey(key);
            temp.setStep(nextStep);
            leafAlloc = dao.updateMaxIdByCustomStepAndGetLeafAlloc(temp);
            buffer.setUpdateTimestamp(System.currentTimeMillis());
            buffer.setStep(nextStep);
            buffer.setMinStep(leafAlloc.getStep());//leafAlloc的step为DB中的step
        }
        // must set value before set max
        /**
         * 此处很坑，这是第一版本留下的无效注释
         * https://github.com/Meituan-Dianping/Leaf/issues/16
         * 可以不用强制要求的，因为是单线程更新，并且buffer还没有就绪因此不存在优先可见的问题
         */
        long value = leafAlloc.getMaxId() - buffer.getStep();
        segment.getValue().set(value);
        segment.setMax(leafAlloc.getMaxId());
        segment.setStep(buffer.getStep());
        sw.stop("updateSegmentFromDb", key + " " + segment);
    }

    /**
     * 核心处理方法
     * 通过读写锁提升并发，读锁主要负责id的自增，但是如果只是自增那么靠automic操作就够，因此还涉及到segment的切换，因此此处使用了读写锁进行分离
     * 当需要切换segment的时候读锁也会被挂起来，因为如果不挂起的话会出现脏读。
     * 方法的核心思想总结如下
     * 通过while循环，死循环的去取数据，先是拿到读锁，此处总结一下 JUC包里面的读写锁的特性，读读可并行，读写不可并行，写写不可并行。
     * 在这里从概念上先完成梳理
     * 1、备用buffer的更新是由单线程完成的，这里面是通过cas更新ThreadRunning实现的，因此备用buffer的更新是安全的
     * 2、id的自增是通过AutomicLong实现的因此也不存在自增时候的线程安全问题
     * 3、主备buffer的切换是由读写锁来进行控制的，读锁生效时候时候要么能够自增成功则返回，要么自增不成功，线程开始抢写锁，如果抢上，那么新来的读锁请求就会被挂起，
     * 直到写锁完成buffer的切换，然后通过while循环自增后返回id
     */
    public Result getIdFromSegmentBuffer(final SegmentBuffer buffer) {
        while (true) {
            buffer.rLock().lock();
            try {
                final Segment segment = buffer.getCurrent();
                if (!buffer.isNextReady() && (segment.getIdle() < 0.9 * segment.getStep()) && buffer.getThreadRunning().compareAndSet(false, true)) {
                    service.execute(new Runnable() {
                        @Override
                        public void run() {
                            Segment next = buffer.getSegments()[buffer.nextPos()];
                            boolean updateOk = false;
                            try {
                                updateSegmentFromDb(buffer.getKey(), next);
                                updateOk = true;
                                logger.info("update segment {} from db {}", buffer.getKey(), next);
                            } catch (Exception e) {
                                logger.warn(buffer.getKey() + " updateSegmentFromDb exception", e);
                            } finally {
                                if (updateOk) {
                                    buffer.wLock().lock();
                                    buffer.setNextReady(true);
                                    buffer.getThreadRunning().set(false);
                                    buffer.wLock().unlock();
                                } else {
                                    buffer.getThreadRunning().set(false);
                                }
                            }
                        }
                    });
                }
                long value = segment.getValue().getAndIncrement();
                if (value < segment.getMax()) {
                    return new Result(value, Status.SUCCESS);
                }
            } finally {
                buffer.rLock().unlock();
            }
            waitAndSleep(buffer);
            buffer.wLock().lock();
            try {
                //这里进行这么判断是因为可能有多个写锁排队在这里，一个写锁更新成了后，那么后面的线程直接取就好，不需要走后续的修改操作了。
                final Segment segment = buffer.getCurrent();
                long value = segment.getValue().getAndIncrement();
                if (value < segment.getMax()) {
                    return new Result(value, Status.SUCCESS);
                }
                //检查备用buffer是否完成了准备，如果准备完成则进行切换，如果未准备完成则抛出异常代表主buffer还有从buffer都没有准备好，系统暂时不可用。
                //产生的原因可能是刷新线程池阻塞，这可能性还是蛮小的，这也是为什么中间件作者在 update代码段加入 stopwatch监控的原因吧。
                if (buffer.isNextReady()) {
                    buffer.switchPos();
                    buffer.setNextReady(false);
                } else {
                    logger.error("Both two segments in {} are not ready!", buffer);
                    return new Result(EXCEPTION_ID_TWO_SEGMENTS_ARE_NULL, Status.EXCEPTION);
                }
            } finally {
                buffer.wLock().unlock();
            }
        }
    }

    private void waitAndSleep(SegmentBuffer buffer) {
        int roll = 0;
        while (buffer.getThreadRunning().get()) {
            roll += 1;
            if(roll > 10000) {
                try {
                    TimeUnit.MILLISECONDS.sleep(10);
                    break;
                } catch (InterruptedException e) {
                    logger.warn("Thread {} Interrupted",Thread.currentThread().getName());
                    break;
                }
            }
        }
    }

    public List<LeafAlloc> getAllLeafAllocs() {
        return dao.getAllLeafAllocs();
    }

    public Map<String, SegmentBuffer> getCache() {
        return cache;
    }

    public IDAllocDao getDao() {
        return dao;
    }

    public void setDao(IDAllocDao dao) {
        this.dao = dao;
    }
}
