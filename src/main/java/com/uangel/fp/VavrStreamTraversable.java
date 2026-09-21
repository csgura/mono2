package com.uangel.fp;

import io.vavr.Function2;
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
@ExtensionMethod(FutureMonad.class)
public class VavrStreamTraversable {

    public static <C,V,U> Mono2<C, Stream<U>> traverseM2(Stream<V> list , C c, Function2<C,V, Mono2<C,U>> f) {
        if (list.isEmpty()) {
            return Mono2.contextOf(c).replace(Stream.empty());
        }

        var sub = list.tail();
        return f.apply(c,  list.head())
            .zflatMap(
                (nc, head) ->
                    traverseM2(sub, nc,  f)
                        .map(tail -> tail.prepend(head))
            );
    }

    private static <A> Stream<A> optional2Stream(Optional<A> o) {
        return o.map(Stream::of).orElseGet(Stream::empty);
    }

    private static <U> Stream<U> mergeOneByOne(Stream<U> a, Stream<U> b) {
        var zipped = a.map(Optional::of).zipAll(b.map(Optional::of), Optional.empty(), Optional.<U>empty());
        return zipped.flatMap(t -> optional2Stream(t._1).appendAll(optional2Stream(t._2)) );
    }


    public static <T,U> CompletableFuture<Stream<U>> traverseF(Stream<T> stream, Function<T,CompletableFuture<U>> f) {
        if(stream.isEmpty()){
            return CompletableFuture.completedFuture(Stream.empty());
        }
        return f.apply( stream.head()).flatMap(head -> traverseF(stream.tail(), f)
            .map(tail -> tail.prepend(head)));
    }

    public static <T,U> CompletableFuture<Stream<U>> ptraverseF(Stream<T> stream, int numParallel, Function<T,CompletableFuture<U>> f) {
        if(numParallel > 1) {
            var pair =  stream.zipWithIndex().partition(t -> t._2 % 2 == 0);
            var odd = ptraverseF(pair._1.map(t -> t._1), numParallel/2,f);
            var even = ptraverseF(pair._2.map(t -> t._1), numParallel/2,f);
            return odd.flatMap(o -> even.map(e -> mergeOneByOne(o,e)));
        }
        return traverseF(stream , f);
    }

    public static <T,U> Mono<Stream<U>> traverseM(Stream<T> stream, Function<T,Mono<U>> f) {
        if(stream.isEmpty()){
            return Mono.just(Stream.empty());
        }
        return f.apply( stream.head()).flatMap(head -> traverseM(stream.tail(), f).map(tail -> tail.prepend(head)));
    }

    public static <T,U> Mono<Stream<U>> ptraverseM(Stream<T> stream, int numParallel, Function<T,Mono<U>> f) {
        if(numParallel > 1) {
            // Divide and conquer
            var pair =  stream.zipWithIndex().partition(t -> t._2 % 2 == 0);
            var odd = ptraverseM(pair._1.map(t -> t._1), numParallel/2,f);
            var even = ptraverseM(pair._2.map(t -> t._1), numParallel/2,f);
            return odd.flatMap(o -> even.map(e -> mergeOneByOne(o,e)));
        }
        return traverseM(stream , f);
    }
}
