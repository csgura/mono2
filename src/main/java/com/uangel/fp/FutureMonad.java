package com.uangel.fp;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.function.Function;

public class FutureMonad {
    public static <T,U> CompletableFuture<U> map(CompletableFuture<T> f , Function<? super T, ? extends U> mf ) {
        return f.thenApply(mf);
    }

    public static <T,U> CompletableFuture<U> flatMap(CompletableFuture<T> f , Function<? super T, ? extends CompletionStage<U>> mf ) {
        return f.thenCompose(mf);
    }

    public static <T> CompletableFuture<T> flatten(CompletableFuture<CompletableFuture<T>> f) {
        return f.thenCompose(i ->i);
    }

    public static <T> CompletableFuture<Throwable> failed(CompletableFuture<T> f) {
        return f.handle( (t, err) -> {
            if(err != null) {
                if (err.getCause() != null) {
                    return CompletableFuture.completedFuture(err.getCause());
                }
                return CompletableFuture.completedFuture(err);
            }
            return CompletableFuture.<Throwable>failedFuture(new IllegalStateException("Future.success"));
        }).thenCompose(i -> i);
    }

}
