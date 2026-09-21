package com.uangel.fp;

import io.vavr.Function2;
import io.vavr.Value;
import io.vavr.collection.Iterator;
import io.vavr.collection.List;
import io.vavr.collection.Stream;
import lombok.experimental.ExtensionMethod;
import reactor.core.publisher.Mono;

import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.function.Function;


// traverse 는 순차적으로 동작
// ptraverse 는 병렬로 동작
// 끝에 F, M 등은 Future, Mono 의미
@ExtensionMethod({VavrStreamTraversable.class, FutureMonad.class})
public class ListTraversable {

    public static <C,V,U> Mono2<C, List<U>> traverseM2(List<V> list , C c, Function2<C,V, Mono2<C,U>> f) {
        return Stream.ofAll(list).traverseM2(c,f).map(Value::toList);
    }

    public static <C,V,U> Mono2<C, java.util.List<U>> traverseM2(java.util.List<V> list , C c, Function2<C,V, Mono2<C,U>> f) {
        return Stream.ofAll(list).traverseM2(c,f).map(Value::toJavaList);
    }

    public static <T,U> Mono<List<U>> traverseM(List<T> list, Function<T,Mono<U>> f) {
        return Stream.ofAll(list).traverseM(f).map(Value::toList);
    }

    public static <T,U> Mono<java.util.List<U>> traverseM(java.util.List<T> list, Function<T,Mono<U>> f) {
        return Stream.ofAll(list).traverseM(f).map(Value::toJavaList);
    }

    public static <T,U> CompletableFuture<List<U>> traverseF(List<T> list,  Function<T,CompletableFuture<U>> f) {
        return Stream.ofAll(list).traverseF(f).map(Value::toList);
    }

    public static <T,U> CompletableFuture<java.util.List<U>> traverseF(java.util.List<T> list,  int numParallel , Function<T,CompletableFuture<U>> f) {
        return Stream.ofAll(list).traverseF(f).map(Value::toJavaList);
    }

    public static <T> Mono<List<T>> sequenceM(List<Mono<T>> list) {
        return Stream.ofAll(list).traverseM(i -> i).map(Value::toList);
    }

    public static <T> Mono<java.util.List<T>> sequenceM(java.util.List<Mono<T>> list) {
        return Stream.ofAll(list).traverseM(i -> i).map(Value::toJavaList);
    }

    public static <T> CompletableFuture<List<T>> sequenceF(List<CompletableFuture<T>> list) {
        return Stream.ofAll(list).traverseF(i -> i).map(Value::toList);
    }

    public static <T> CompletableFuture<java.util.List<T>> sequenceF(java.util.List<CompletableFuture<T>> list) {
        return Stream.ofAll(list).traverseF(i -> i).map(Value::toJavaList);
    }

    public static <T,U> Mono<List<U>> ptraverseM(List<T> list,  int numParallel , Function<T,Mono<U>> f) {
        return Stream.ofAll(list).ptraverseM(numParallel,f).map(Value::toList);
    }

    public static <T,U> Mono<java.util.List<U>> ptraverseM(java.util.List<T> list,  int numParallel , Function<T,Mono<U>> f) {
        return Stream.ofAll(list).ptraverseM(numParallel,f).map(Value::toJavaList);
    }

    public static <T,U> CompletableFuture<List<U>> ptraverseF(List<T> list,  int numParallel , Function<T,CompletableFuture<U>> f) {
       return Stream.ofAll(list).ptraverseF(numParallel,f).map(Value::toList);
    }

    public static <T,U> CompletableFuture<java.util.List<U>> ptraverseF(java.util.List<T> list,  int numParallel , Function<T,CompletableFuture<U>> f) {
        return Stream.ofAll(list).ptraverseF(numParallel,f).map(Value::toJavaList);
    }
}
