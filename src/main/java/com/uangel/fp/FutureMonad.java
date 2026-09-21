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

}
