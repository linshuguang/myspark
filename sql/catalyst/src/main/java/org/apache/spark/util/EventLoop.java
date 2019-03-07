package org.apache.spark.util;

import lombok.Data;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.spark.sql.catalyst.parser.ParserUtils.NonFatal;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.atomic.AtomicBoolean;
/**
 * Created by kenya on 2019/3/5.
 */
@Data
public abstract class EventLoop<E> {
    private static final Logger LOGGER = LoggerFactory.getLogger(EventLoop.class);

    String name;
    public EventLoop(String name){
        this.name = name;
    }

    private BlockingQueue<E> eventQueue = new LinkedBlockingDeque<>();
    private AtomicBoolean stopped = new AtomicBoolean(false);

    private Thread eventThread = new Thread(name){

        @Override
        public void run(){
            try {
                while (!stopped.get()) {
                    E event = eventQueue.take();
                    try {
                        onReceive(event);
                    } catch(Exception e) {
                        if(NonFatal(e)){
                            try{
                                onError(e);
                            }catch (Exception ee){
                                if(NonFatal(ee)){
                                    LOGGER.error("Unexpected error in " + name, e);
                                }
                            }
                        }
                    }
                }
            } catch(InterruptedException ie) {
            }catch (Exception e){
                if(NonFatal(e)) {
                    LOGGER.error("Unexpected error in " + name, e);
                }
            }
        }
    };



    protected abstract void onReceive(E event);

    /**
     * Invoked if `onReceive` throws any non fatal error. Any non fatal error thrown from `onError`
     * will be ignored.
     */
    protected abstract void onError(Throwable e);
}
