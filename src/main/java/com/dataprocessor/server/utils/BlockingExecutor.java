package com.dataprocessor.server.utils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

public final class BlockingExecutor {
    private static final Logger logger = LoggerFactory.getLogger(BlockingExecutor.class);

    private final int concurrencyLevel;
    private final AtomicInteger executionsCounter;
    private final Object monitor;
    private final ExceptionHandler exceptionHandler;
    private final AtomicBoolean isClosing;

    public BlockingExecutor(final int concurrencyLevel, final ExceptionHandler exceptionHandler){
        this.exceptionHandler = exceptionHandler;
        this.concurrencyLevel = concurrencyLevel;
        this.executionsCounter = new AtomicInteger(0);
        this.isClosing = new AtomicBoolean(false);
        this.monitor = new Object();
        logger.info("BlockingExecutor started. Concurrency level: {}.", concurrencyLevel);
    }

    public BlockingExecutor(final int concurrencyLevel){
        this(concurrencyLevel, (cause)-> logger.warn("Error in blocking executor.", cause));
    }

    public BlockingExecutor(){
        this(Math.max(10, Runtime.getRuntime().availableProcessors() * 4));
    }

    public final void execute(final Runnable task){
        if (isClosing.get())
            return;

        if (executionsCounter.incrementAndGet() > concurrencyLevel){
            do{
                synchronized (monitor){
                    try {monitor.wait(1000);}catch (final Throwable ignored){}
                }
            }while (executionsCounter.get() > concurrencyLevel);
        }
        Thread.startVirtualThread(()->{
            try{
                task.run();
            }catch (final Throwable cause){
                exceptionHandler.onException(cause);
            }finally {
                executionsCounter.decrementAndGet();
                synchronized (monitor){
                    monitor.notify();
                }
            }
        });
    }

    public final void close(){
        if (!isClosing.getAndSet(true)){
            while (executionsCounter.incrementAndGet() > 0){
                synchronized (monitor){
                    monitor.notifyAll();
                }
                try {Thread.sleep(100);}catch (final Throwable ignored){}
            }
        }
    }

    public static interface ExceptionHandler{
        void onException(final Throwable cause);
    }
}
