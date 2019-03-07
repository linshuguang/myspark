package org.apache.spark.util.clock;

import org.apache.spark.util.clock.Clock;

/**
 * Created by kenya on 2019/3/5.
 */
public class SystemClock implements Clock {
    long minPollTime = 25L;

    @Override
    public long getTimeMillis() {
        return System.currentTimeMillis();
    }

    @Override
    public long waitTillTime(long targetTime){
        long currentTime = System.currentTimeMillis();

        long waitTime = targetTime - currentTime;
        if (waitTime <= 0) {
            return currentTime;
        }

        long pollTime = Math.round(Math.max(waitTime / 10.0, minPollTime));

        while (true) {
            currentTime = System.currentTimeMillis();
            waitTime = targetTime - currentTime;
            if (waitTime <= 0) {
                return currentTime;
            }
            long sleepTime = Math.min(waitTime, pollTime);
            try {
                Thread.sleep(sleepTime);
            }catch (InterruptedException e){
                continue;
            }
        }
        //return -1;
    }
}
