package com.dataprocessor.server.utils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;

public final class TryUtil {

    public static final VoidTryHandler attempt(final TryVoid tryBlock, final Logger logger){

        return new VoidTryHandler() {
            private volatile TryErrorHandler errorHandler = error -> logger.error("Try block error: ", error);
            private volatile VoidTrySuccessHandler successHandler = () -> logger.info("Try block success.");
            private volatile FinallyHandler finallyHandler = () -> logger.debug("Try block finally reached.");
            @Override
            public final void doTry() {
                try{
                    tryBlock.attempt();
                    successHandler.onSuccess();
                }catch (final Throwable error){
                    errorHandler.onError(error);
                }finally {
                    finallyHandler.onFinally();
                }
            }
            @Override
            public final void doTry(final VoidTryResultHandler handler) {
                try{
                    tryBlock.attempt();
                    handler.onResult();
                    successHandler.onSuccess();
                }catch (final Throwable error){
                    errorHandler.onError(error);
                }finally {
                    finallyHandler.onFinally();
                }
            }
            @Override
            public final VoidTryHandler onError(final TryErrorHandler errorHandler) {
                assert errorHandler != null;
                this.errorHandler = errorHandler;
                return this;
            }
            @Override
            public final VoidTryHandler onSuccess(final VoidTrySuccessHandler successHandler) {
                assert successHandler != null;
                this.successHandler = successHandler;
                return this;
            }
            @Override
            public final VoidTryHandler onFinally(final FinallyHandler finallyHandler) {
                assert finallyHandler != null;
                this.finallyHandler = finallyHandler;
                return this;
            }
        };
    }

    public static final VoidTryHandler attempt(final TryVoid tryBlock){
        return attempt(tryBlock, LoggerFactory.getLogger("TryUtilLogger"));
    }

    public static final <T> TryHandler<T> attempt(final Try<T> tryBlock, final Logger logger){
        return new TryHandler<T>() {
            private volatile TryErrorHandler errorHandler = error -> logger.error("Try block error: ", error);
            private volatile TrySuccessHandler<T> successHandler = value -> logger.debug("Try block success.");
            private volatile FinallyHandler finallyHandler = () -> logger.debug("Try block finally reached.");
            private volatile boolean closeOnFinally = true;
            private volatile T result = null;
            @Override
            public final void doTry() {
                try{
                    final T r = result = tryBlock.attempt();
                    successHandler.onSuccess(r);
                }catch (final Throwable error){
                    errorHandler.onError(error);
                }finally {
                    if (closeOnFinally){
                        tryToClose();
                    }
                    finallyHandler.onFinally();
                }
            }
            @Override
            public final T getResult() {
                return result;
            }
            @Override
            public final void doTry(final TryResultHandler<T> handler) {
                try{
                    final T r = result = tryBlock.attempt();
                    handler.onResult(r);
                    successHandler.onSuccess(r);
                }catch (final Throwable error){
                    errorHandler.onError(error);
                }finally {
                    if (closeOnFinally){
                        tryToClose();
                    }
                    finallyHandler.onFinally();
                }
            }
            private final void tryToClose(){
                final T r = result;
                if (r == null)
                    return;

                try {
                    r.getClass().getMethod("close").invoke(r);
                }catch (final Throwable ignored){}
            }
            @Override
            public final TryHandler<T> onError(final TryErrorHandler errorHandler) {
                assert errorHandler != null;
                this.errorHandler = errorHandler;
                return this;
            }
            @Override
            public final TryHandler<T> onSuccess(final TrySuccessHandler<T> successHandler) {
                assert successHandler != null;
                this.successHandler = successHandler;
                return this;
            }
            @Override
            public final TryHandler<T> closeOnFinally(final boolean closeOnFinally) {
                this.closeOnFinally = closeOnFinally;
                return this;
            }
            @Override
            public final TryHandler<T> onFinally(final FinallyHandler finallyHandler) {
                assert finallyHandler != null;
                this.finallyHandler = finallyHandler;
                return this;
            }
        };
    }

    public static final <T> TryHandler<T> attempt(final Try<T> tryBlock){
        return attempt(tryBlock, LoggerFactory.getLogger("TryUtilLogger"));
    }

    public static interface TryHandler<T>{
        void doTry();
        T getResult();
        void doTry(final TryResultHandler<T> handler);
        TryHandler<T> onError(final TryErrorHandler errorHandler);
        TryHandler<T> onSuccess(final TrySuccessHandler<T> successHandler);
        TryHandler<T> closeOnFinally(final boolean closeOnFinally);
        TryHandler<T> onFinally(final FinallyHandler finallyHandler);
    }
    public static interface VoidTryHandler{
        void doTry();
        void doTry(final VoidTryResultHandler handler);
        VoidTryHandler onError(final TryErrorHandler errorHandler);
        VoidTryHandler onSuccess(final VoidTrySuccessHandler successHandler);
        VoidTryHandler onFinally(final FinallyHandler finallyHandler);
    }
    public static interface FinallyHandler{
        void onFinally();
    }
    public static interface TrySuccessHandler<T>{
        void onSuccess(final T value);
    }
    public static interface VoidTrySuccessHandler{
        void onSuccess();
    }
    public static interface TryErrorHandler{
        void onError(final Throwable error);
    }
    public static interface TryResultHandler<T>{
        void onResult(final T result);
    }
    public static interface VoidTryResultHandler{
        void onResult();
    }
    public static interface Try<T>{
        T attempt() throws Throwable;
    }
    public static interface TryVoid{
        void attempt() throws Throwable;
    }
}
