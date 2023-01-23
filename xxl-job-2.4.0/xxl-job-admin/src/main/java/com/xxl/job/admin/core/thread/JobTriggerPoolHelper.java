package com.xxl.job.admin.core.thread;

import com.xxl.job.admin.core.conf.XxlJobAdminConfig;
import com.xxl.job.admin.core.trigger.TriggerTypeEnum;
import com.xxl.job.admin.core.trigger.XxlJobTrigger;
import lombok.extern.slf4j.Slf4j;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * job trigger thread pool helper
 * 作业触发线程池助手
 *
 * @author xuxueli 2018-07-03 21:08:07
 */
@Slf4j
public class JobTriggerPoolHelper {
    // job timeout count
    private volatile long minTim = System.currentTimeMillis() / 60000;     // ms > min

    // 此map用来记录任务超时次数，不过实际上并不是超时，只是执行时间大于500ms，应该叫做执行慢的次数才对
    // https://stackoverflow.com/questions/29404851/do-we-need-to-make-concurrenthashmap-volatile
    private final ConcurrentMap<Integer, AtomicInteger> jobTimeoutCountMap = new ConcurrentHashMap<>();

    // ---------------------- trigger pool ----------------------

    // fast/slow thread pool
    // 快慢线程池：部分慢的任务会拖慢整个线程池，因此我们需要将快慢任务线程池隔离
    private ThreadPoolExecutor fastTriggerPool = null;
    private ThreadPoolExecutor slowTriggerPool = null;

    public void start() {
        fastTriggerPool = new ThreadPoolExecutor(
                10,
                XxlJobAdminConfig.getAdminConfig().getTriggerPoolFastMax(), // 最大线程数，最小为200
                60L,
                TimeUnit.SECONDS,
                new LinkedBlockingQueue<>(1000), // 队列数1000
                r -> new Thread(r, "xxl-job, admin JobTriggerPoolHelper-fastTriggerPool-" + r.hashCode()));

        slowTriggerPool = new ThreadPoolExecutor(
                10,
                XxlJobAdminConfig.getAdminConfig().getTriggerPoolSlowMax(), // 最大线程数，最小为100
                60L,
                TimeUnit.SECONDS,
                new LinkedBlockingQueue<>(2000), // 队列数2000
                r -> new Thread(r, "xxl-job, admin JobTriggerPoolHelper-slowTriggerPool-" + r.hashCode()));
    }

    public void stop() {
        //triggerPool.shutdown();
        fastTriggerPool.shutdownNow();
        slowTriggerPool.shutdownNow();
        LOGGER.info(">>>>>>>>> xxl-job trigger thread pool shutdown success.");
    }

    /**
     * add trigger
     */
    public void addTrigger(final int jobId,
                           final TriggerTypeEnum triggerType,
                           final int failRetryCount,
                           final String executorShardingParam,
                           final String executorParam,
                           final String addressList) {
        // choose thread pool
        ThreadPoolExecutor triggerPool_ = fastTriggerPool;
        AtomicInteger jobTimeoutCount = jobTimeoutCountMap.get(jobId);
        // 作业-1分钟内超时10次，将会使用慢线程池来执行
        if (jobTimeoutCount != null && jobTimeoutCount.get() > 10) {      // job-timeout 10 times in 1 min
            triggerPool_ = slowTriggerPool;
        }

        // trigger
        triggerPool_.execute(() -> {

            long start = System.currentTimeMillis();

            try {
                // do trigger
                XxlJobTrigger.trigger(jobId, triggerType, failRetryCount, executorShardingParam, executorParam, addressList);
            } catch (Exception e) {
                LOGGER.error(e.getMessage(), e);
            } finally {

                // check timeout-count-map
                long minTim_now = System.currentTimeMillis() / 60000;
                // 当前时间戳和60000相除的结果，如果和既定变量不一致，那么肯定已经过去1分钟甚至更久，那么清空jobTimeoutCountMap
                if (minTim != minTim_now) {
                    minTim = minTim_now;
                    jobTimeoutCountMap.clear();
                }

                // incr timeout-count-map
                long cost = System.currentTimeMillis() - start;
                // 如果任务执行时间大于500ms，那么记为超时一次
                if (cost > 500) {       // ob-timeout threshold 500ms
                    AtomicInteger timeoutCount = jobTimeoutCountMap.putIfAbsent(jobId, new AtomicInteger(1));
                    if (timeoutCount != null) {
                        timeoutCount.incrementAndGet();
                    }
                }
            }
        });
    }

    // ---------------------- helper ----------------------

    private static final JobTriggerPoolHelper JOB_TRIGGER_POOL_HELPER = new JobTriggerPoolHelper();

    public static void toStart() {
        JOB_TRIGGER_POOL_HELPER.start();
    }

    public static void toStop() {
        JOB_TRIGGER_POOL_HELPER.stop();
    }

    /**
     * @param jobId
     * @param triggerType
     * @param failRetryCount        >=0: use this param
     *                              <0: use param from job info config
     * @param executorShardingParam
     * @param executorParam         null: use job param
     *                              not null: cover job param
     */
    public static void trigger(int jobId, TriggerTypeEnum triggerType, int failRetryCount, String executorShardingParam, String executorParam, String addressList) {
        JOB_TRIGGER_POOL_HELPER.addTrigger(jobId, triggerType, failRetryCount, executorShardingParam, executorParam, addressList);
    }

}
