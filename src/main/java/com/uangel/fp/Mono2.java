package com.uangel.fp;


import io.vavr.*;
import io.vavr.control.Try;
import org.jspecify.annotations.NonNull;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Mono;
import reactor.core.publisher.SignalType;
import reactor.core.scheduler.Scheduler;
import reactor.util.retry.Retry;

import java.time.Duration;
import java.util.NoSuchElementException;
import java.util.Optional;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.function.Supplier;

/*
name convention:
 C 는 Context , V 는 Value 를 의미
 get 은  Context 의 값을  V 로 옮기는 것을 의미
 put 은 V 의 값을 Context로 옮기는 것을 의미
 modify 는 C 를 바꾸는 것을 의미
 transform 은 성공일 때 C,V 둘다 바꾸는 것을 의미
 either는 성공/실패 두가지 경우에 대해 , C,V 둘다 바꾸는 것을 의미
 recover 는 error 를 복구하는 것을 의미
 z 는 get 앞에 붙는 경우 C와 V 를 tuple로 zip 해서 리턴 한다는 의미
 z 가 map 앞에 붙는 경우 callback 에서 C,V 를 둘 다 아규먼트로 받는 다는 의미

 끝에 S 는 Supplier
 끝에 T 는 Try
 끝에 F 는 Future
 끝에 M 은 Mono
 끝에 O 는 Optional 을 의미
 put이나 modify 뒤에 With 가 오는 경우,  with 함수 (C,V) -> C  를 사용한다는 의미
 Tuple 이 필요한 경우 2,3,4 등의 숫자를 붙임

 V 쪽 작업 하다 에러가 발생해도 C 는 ExceptionWithContext 에 보존되서
 recover 나 mapError 에서 참조 가능하고, context() 나 run() 호출 시 error 와 함께 마지막 C 를 리턴해 준다

 C 의 타입을 변경할 수 없다. ExceptionWithContext 의 type casting 이 안전하지 않기 때문
 C의 타입을 변경할 수 있는 경우는 , 성공인 경우와 실패인 경우에 대해 모두 callback을 제공하는 either 와 modify 뿐이다.
 eitherM 의 경우 실패할 수 있으므로 C의 타입을 변경하는 것은 불가능하다.
 */

/**
 *
 * @param <C> Context
 * @param <V> Value
 */
public class Mono2<C, V> {
    static class ExceptionWithContext extends RuntimeException {
        private final Object context;
        final Throwable err;

        ExceptionWithContext(Object c, Throwable err) {
            super(err);
            this.context = c;
            this.err = err;
        }

        @SuppressWarnings("unchecked")
        <T> T getContext() {
            return (T) context;
        }
    }

    private static Throwable wrap(Object context, Throwable err) {
        return err instanceof ExceptionWithContext ? err : new ExceptionWithContext(context, err);
    }

    private static <T> Mono<@NonNull  T> attempt(Object context, Supplier<Mono<@NonNull T>> supplier) {
        try {
            return supplier.get().onErrorMap(err -> wrap(context, err));
        } catch (Throwable e) {
            return Mono.error(wrap(context, e));
        }
    }

    private static <CI, VI, CO, UO> Mono<@NonNull Tuple2<CO, UO>> mapHandle(
        Mono<@NonNull Tuple2<CI, VI>> source,
        Function<Tuple2<CI, VI>, Tuple2<CO, UO>> mapper
    ) {
        return source.handle((t, sink) -> {
            try {
                sink.next(mapper.apply(t));
            } catch (Throwable e) {
                sink.error(wrap(t._1, e));
            }
        });
    }

    public final Mono<@NonNull  Tuple2<C, V>> mono;

    private Mono2(Mono<@NonNull Tuple2<C, V>> mono) {
        this.mono = mono;
    }

    private static <C, V> Mono2<C, V> apply(Mono<@NonNull Tuple2<C, V>> mono) {
        return new Mono2<>(mono);
    }

    public static <C, V> Mono2<C, V> fromFuture(C c, CompletableFuture<V> future) {
        return from(c, Mono.fromFuture(future));
    }

    public static <C, V> Mono2<C, V> from(C c, Mono<@NonNull V> source) {
        return new Mono2<>(attempt(c, () -> source.map(v -> Tuple.of(c, v))));
    }

    public static <C, V> Mono2<C, V> fromCallable(C c, Callable<V> callable) {
        return from(c, Mono.fromCallable(callable));
    }

    public static <C, V> Mono2<C, V> fromSupplier(C c, Supplier<V> supplier) {
        return from(c, Mono.fromSupplier(supplier));
    }

    public static <C, V> Mono2<C, V> defer(Supplier<Mono2<C, V>> supplier) {
        return apply(Mono.defer(() -> supplier.get().mono));
    }

    public static <C, V> Mono2<C, V> error(C c, Throwable err) {
        return new Mono2<>(Mono.error(wrap(c, err)));
    }

    public static <C, V> Mono2<C, V> of(C c, V v) {
        return new Mono2<>(Mono.just(Tuple.of(c, v)));
    }

    public static <C> Mono2<C, Tuple0> contextOf(C c) {
        return of(c, Tuple0.instance());
    }

    public Mono2<C, V> cache() {
        return apply(mono.cache());
    }

    public Mono2<C, V> cache(Duration ttl) {
        return apply(mono.cache(ttl));
    }

    public Mono2<C, V> share() {
        return apply(mono.share());
    }

    public Mono2<C, V> subscribeOn(Scheduler scheduler) {
        return apply(mono.subscribeOn(scheduler));
    }

    public Mono2<C, V> publishOn(Scheduler scheduler) {
        return apply(mono.publishOn(scheduler));
    }

    public Mono2<C, V> checkpoint(String description) {
        return apply(mono.checkpoint(description));
    }

    public Mono2<C, V> log() {
        return apply(mono.log());
    }

    public Mono2<C, V> log(String category) {
        return apply(mono.log(category));
    }

    public <U> Mono2<C,U> transform(Function2<C, ? super V, Tuple2<C, U>> f) {
        return apply(mapHandle(mono, t -> f.apply(t._1,t._2)));
    }

    public <U> Mono2<C, U> transformM(Function2<C, ? super V, Mono<@NonNull Tuple2<C, U>>> f) {
        return apply(mono.flatMap(t -> attempt(t._1, () -> f.apply(t._1,t._2))));
    }

    public <U> Mono2<C, U> transformF(Function2<C, ? super V, CompletableFuture<Tuple2<C, U>>> f) {
        return transformM((c, v) -> Mono.fromFuture(f.apply(c, v)));
    }

    public <U> Mono2<C, U> transformT(Function2<C, ? super V, Try<Tuple2<C, U>>> f) {
        return transformF((c, v) -> f.apply(c, v).toCompletableFuture());
    }

    public Mono2<C, V> recover(Function2<C, Throwable, Tuple2<C, V>> rf) {
        return recoverM((c, err) -> Mono.just(rf.apply(c, err)));
    }

    public Mono2<C, V> mapError(Function2<C, Throwable, Throwable> mf) {
        return apply(mono.onErrorMap(err -> {
            if (err instanceof ExceptionWithContext ei) {
                try {
                    return new ExceptionWithContext(ei.getContext(), mf.apply(ei.getContext(), ei.err));
                } catch (Throwable e) {
                    return wrap(ei.getContext(), e);
                }
            }
            return err;
        }));
    }

    public <CO, R> Mono2<CO, R> either(Function2<C, ? super V, Tuple2<CO, R>> onSuccess, Function2<C, Throwable, Tuple2<CO, R>> onFailure) {
        return apply(mapHandle(mono, t -> onSuccess.apply(t._1, t._2)).onErrorResume(err -> {
            if (err instanceof ExceptionWithContext ei) {
                return attempt(ei.getContext(), () -> Mono.just(onFailure.apply(ei.getContext(), ei.err)));
            }
            return Mono.error(err);
        }));
    }

    public <R> Mono2<C, R> eitherM(Function2<C, ? super V, Mono<@NonNull Tuple2<C, R>>> onSuccess, Function2<C, Throwable, Mono<@NonNull Tuple2<C, R>>> onFailure) {
        return transformM(onSuccess).recoverM(onFailure);
    }

    public Mono2<C, V> recoverM(Function2<C, Throwable, Mono<@NonNull Tuple2<C, V>>> rf) {
        return apply(mono.onErrorResume(err -> {
            if (err instanceof ExceptionWithContext ei) {
                return attempt(ei.getContext(), () -> rf.apply(ei.getContext(), ei.err));
            }
            return Mono.error(err);
        }));
    }

    public Mono2<C, V> recoverF(Function2<C, Throwable, CompletableFuture<Tuple2<C, V>>> rf) {
        return recoverM((c, err) -> Mono.fromFuture(rf.apply(c, err)));
    }

    public Mono2<C, V> recoverT(Function2<C, Throwable, Try<Tuple2<C, V>>> rf) {
        return recoverF((c, err) -> rf.apply(c, err).toCompletableFuture());
    }

    public <E extends Throwable> Mono2<C, V> recover(Class<E> type, Function2<C, E, Tuple2<C, V>> rf) {
        return recoverM((c, err) -> {
            if (type.isInstance(err)) {
                return Mono.just(rf.apply(c, type.cast(err)));
            }
            return Mono.error(wrap(c, err));
        });
    }

    public <E extends Throwable> Mono2<C, V> mapError(Class<E> type, Function2<C, E, Throwable> mf) {
        return mapError((c, err) -> type.isInstance(err) ? mf.apply(c, type.cast(err)) : err);
    }

    public Mono2<C, V> retry() {
        return apply(mono.retry());
    }

    public Mono2<C, V> retry(long numRetries) {
        return apply(mono.retry(numRetries));
    }

    public Mono2<C, V> retryWhen(Retry retry) {
        return apply(mono.retryWhen(retry));
    }

    public Mono<@NonNull Tuple2<C, Optional<Throwable>>> context() {
        return mono.map(t -> t.map2(v -> Optional.<Throwable>empty()))
            .onErrorResume(err -> {
                if (err instanceof ExceptionWithContext ec) {
                    return Mono.just(Tuple.of(ec.getContext(), Optional.of(ec.err)));
                }
                return Mono.error(err);
            });
    }

    public CompletableFuture<V> eval() {
        return value().toFuture();
    }

    public CompletableFuture<Tuple2<C,Try<V>>> run() {
        return map(Try::success)
            .recover((c,err) -> Tuple.of(c, Try.failure(err)))
            .mono.toFuture();
    }

    public CompletableFuture<Tuple2<C,Optional<Throwable>>> exec() {
        return context().toFuture();
    }


    public Mono<@NonNull V> value() {
        return mono.map(Tuple2::_2).onErrorMap(err -> {
            if (err instanceof ExceptionWithContext ec) {
                return ec.err;
            }
            return err;
        });
    }

    public Mono2<C, Tuple0> unit() {
        return replace(Tuple0.instance());
    }

    public <U> Mono2<C, U> replace(U u) {
        return map(v -> u);
    }

    public <U> Mono2<C, U> replaceM(Mono<@NonNull U> u) {
        return mapM(v -> u);
    }

    public <U> Mono2<C, U> replaceF(CompletableFuture<U> u) {
        return mapF(v -> u);
    }

    public <U> Mono2<C, U> replaceT(Try<U> u) {
        return mapT(v -> u);
    }

    public <U> Mono2<C, U> map(Function1<? super V, ? extends U> mf) {
        return apply(mapHandle(mono, t -> t.map2(mf)));
    }

    public <U> Mono2<C, U> zmap(Function2<? super C, ? super V, ? extends U> mf) {
        return apply(mapHandle(mono, t -> Tuple.of(t._1, mf.apply(t._1, t._2))));
    }

    public <U> Mono2<C, U> mapM(Function1<? super V, Mono<@NonNull U>> mf) {
        return apply(mono.flatMap(t -> attempt(t._1, () -> mf.apply(t._2).map(u -> Tuple.of(t._1, u)))));
    }

    public <U> Mono2<C, U> zmapM(Function2<? super C, ? super V, Mono<@NonNull U>> mf) {
        return apply(mono.flatMap(t -> attempt(t._1, () -> mf.apply(t._1, t._2).map(u -> Tuple.of(t._1, u)))));
    }

    public <U> Mono2<C, U> mapF(Function1<? super V, ? extends CompletionStage<U>> mf) {
        return mapM(v -> Mono.fromFuture(mf.apply(v).toCompletableFuture()));
    }

    public <U> Mono2<C, U> zmapF(Function2<? super C , ? super V, ? extends CompletionStage<U>> mf) {
        return zmapM((c, v) -> Mono.fromFuture(mf.apply(c, v).toCompletableFuture()));
    }

    private static <V> Mono<@NonNull V> tryIntoMono(Try<V> tv) {
        return tv.fold(Mono::error, Mono::just);
    }

    public <U> Mono2<C, U> mapT(Function1<? super V, Try<U>> mf) {
        return mapM(v -> tryIntoMono(mf.apply(v)));
    }

    public <U> Mono2<C, U> zmapT(Function2<? super C, ? super V, Try<U>> mf) {
        return zmapM((c,v) -> tryIntoMono(mf.apply(c, v)));
    }

    public <U> Mono2<C, U> flatMap(Function1<? super V, Mono2<C, U>> mf) {
        return apply(mono.flatMap(t -> attempt(t._1, () -> mf.apply(t._2).mono)));
    }

    public <U> Mono2<C, U> then(Mono2<C, U> next) {
        return apply(mono.flatMap(t -> attempt(t._1, () -> next.mono)));
    }
    // filter 는 Mono2 와 안 맞는듯
    private Mono2<C, V> filter(Predicate<? super V> pred) {
        return filter((c, v) -> pred.test(v));
    }

    private Mono2<C, V> filter(Function2<C, ? super V, Boolean> pred) {
        return apply(mapHandle(mono, t -> {
            if (Boolean.TRUE.equals(pred.apply(t._1, t._2))) {
                return t;
            }
            throw new NoSuchElementException("filter");
        }));
    }
//
//    public Mono2<C, V> filterWhen(Function1<? super V, Mono<Boolean>> pred) {
//        return filterWhen((c, v) -> pred.apply(v));
//    }
//
//    public Mono2<C, V> filterWhen(Function2<C, ? super V, Mono<Boolean>> pred) {
//        return apply(mono.flatMap(t -> attempt(t._1, () -> pred.apply(t._1, t._2).flatMap(pass ->
//            Boolean.TRUE.equals(pass) ? Mono.just(t) : Mono.error(new NoSuchElementException("filterWhen"))
//        ))));
//    }
//

    public <U> Mono2<C, Tuple2<V, U>> zipWhen(Function1<? super V, Mono<@NonNull U>> other) {
        return zipWhen((c, v) -> other.apply(v));
    }

    public <U> Mono2<C, Tuple2<V, U>> zipWhen(Function2<C, ? super V, Mono<@NonNull U>> other) {
        return apply(mono.flatMap(t -> attempt(t._1, () ->
            other.apply(t._1, t._2).map(u -> Tuple.of(t._1, Tuple.of(t._2, u)))
        )));
    }

    public <U, R> Mono2<C, R> zipWhen(Function1<? super V, Mono<@NonNull U>> other, Function2<? super V, ? super U, ? extends R> combinator) {
        return zipWhen(other).map(t -> combinator.apply(t._1, t._2));
    }

    public <U> Mono2<C, Tuple2<V, U>> zipWith(Mono<@NonNull U> other) {
        return zipWhen(v -> other);
    }

    public <U> Mono2<C, Tuple2<V, U>> zipWith(Mono2<?, U> other) {
        return zipWhen(v -> other.value());
    }

    public <U, R> Mono2<C, R> zipWith(Mono<@NonNull U> other, Function2<? super V, ? super U, ? extends R> combinator) {
        return zipWhen(v -> other, combinator);
    }

    public Mono2<C, V> delayUntil(Function1<? super V, ? extends Publisher<?>> other) {
        return delayUntil((c, v) -> other.apply(v));
    }

    public Mono2<C, V> delayUntil(Function2<C, ? super V, ? extends Publisher<?>> other) {
        return apply(mono.flatMap(t -> attempt(t._1, () ->
            Mono.just(t).delayUntil(x -> other.apply(x._1, x._2))
        )));
    }

    public Mono2<C, V> delayElement(Duration delay) {
        return apply(mono.delayElement(delay));
    }

    public <U> Mono2<C, U> cast(Class<U> type) {
        return map(type::cast);
    }

    public <U> Mono2<C, U> ofType(Class<U> type) {
        return filter(type::isInstance).cast(type);
    }

    public Mono2<C, Tuple2<Long, V>> elapsed() {
        return apply(mono.elapsed().map(t -> {
            var cv = t.getT2();
            return Tuple.of(cv._1, Tuple.of(t.getT1(), cv._2));
        }));
    }

    public Mono2<C, V> doOnNext(Consumer<? super V> consumer) {
        return doOnNext((c, v) -> consumer.accept(v));
    }

    public Mono2<C, V> doOnNext(BiConsumer<? super C, ? super V> consumer) {
        return apply(mapHandle(mono, t -> {
            consumer.accept(t._1, t._2);
            return t;
        }));
    }

    public Mono2<C, V> doOnError(Consumer<? super Throwable> consumer) {
        return doOnError((c, err) -> consumer.accept(err));
    }

    public Mono2<C, V> doOnError(BiConsumer<? super C, ? super Throwable> consumer) {
        return apply(mono.doOnError(err -> {
            if (err instanceof ExceptionWithContext ei) {
                consumer.accept(ei.getContext(), ei.err);
            }
        }));
    }

    public Mono2<C, V> doFinally(Consumer<SignalType> onFinally) {
        return apply(mono.doFinally(onFinally));
    }

    public Mono2<C, V> doOnCancel(Runnable onCancel) {
        return apply(mono.doOnCancel(onCancel));
    }

    public Mono2<C, Tuple0> putWith(Function2<C, ? super V, C> wf) {
        return apply(mapHandle(mono, t -> Tuple.of(wf.apply(t._1, t._2), Tuple0.instance())));
    }

    public Mono2<C, V> putSet(BiConsumer<C, ? super V> wf) {
        return apply(mapHandle(mono, t -> {
            wf.accept(t._1, t._2);
            return t;
        }));
    }

    public Mono2<C, C> getC() {
        return apply(mono.map(t -> Tuple.of(t._1, t._1)));
    }

    public Mono2<C, Tuple2<C,V>> zgetC() {
        return apply(mono.map(t -> Tuple.of(t._1, Tuple.of(t._1,t._2))));
    }

    public <R> Mono2<C, R> getS(Function1<? super C, ? extends R> gf) {
        return getC().map(gf);
    }

    public <R> Mono2<C, Tuple2<R,V>> zgetS(Function1<? super C, ? extends R> gf) {
        return zgetC().map(t -> t.map1(gf));
    }

    public <R> Mono2<C, R> getT(Function1<? super C, Try<R>> gf) {
        return getC().mapT(gf);
    }

    public <R> Mono2<C, Tuple2<R,V>> zgetT(Function1<? super C, Try<R>> gf) {
        return zgetC().mapT(t -> gf.apply(t._1).map(r -> Tuple.of(r,t._2)));
    }

    public <R> Mono2<C, R> getM(Function1<? super C, Mono<@NonNull R>> gf) {
        return getC().mapM(gf);
    }

    public <R> Mono2<C, Tuple2<R,V>> zgetM(Function1<? super C, Mono<@NonNull R>> gf) {
        return zgetC().mapM(t -> gf.apply(t._1).map(r -> Tuple.of(r, t._2)));
    }

    public <R> Mono2<C, R> getF(Function1<? super C, ? extends CompletionStage<R>> gf) {
        return getC().mapF(gf);
    }

    public <R> Mono2<C, Tuple2<R,V>> zgetF(Function1<? super C, ? extends CompletionStage<R>> gf) {
        return zgetC().mapF(t -> gf.apply(t._1).thenApply(r -> Tuple.of(r, t._2)));
    }

    public <CO> Mono2<CO, V> modify(Function1<C, CO> onSuccess, Function2<C,Throwable,CO> onError) {
        return apply(mapHandle(mono, t -> t.map1(onSuccess)).onErrorMap(err -> remapContext(err, onError)));
    }

    // V 혹은 error 를 이용해서 C 를 modify 하는 경우
    public <CO> Mono2<CO, Tuple0> modifyWith(Function2<C, ? super V, CO> onSuccess, Function2<C,Throwable,CO> onError) {
        return apply(mapHandle(mono, t -> Tuple.of(onSuccess.apply(t._1, t._2), Tuple0.instance())).onErrorMap(err -> remapContext(err, onError)));
    }

    private static <C, CO> Throwable remapContext(Throwable err, Function2<C, Throwable, CO> onError) {
        if (err instanceof ExceptionWithContext ei) {
            try {
                return new ExceptionWithContext(onError.apply(ei.getContext(), ei.err), ei.err);
            } catch (Throwable e) {
                return wrap(ei.getContext(), e);
            }
        }
        return err;
    }

    public <A, B> Mono2<C, Tuple2<A, B>> getS2(Function<? super C, ? extends A> g1, Function<? super C, ? extends B> g2) {
        return getS(c -> Tuple.of(g1.apply(c), g2.apply(c)));
    }

    public <A, B, R> Mono2<C, R> getS2map(Function<? super C, ? extends A> g1, Function<? super C, ? extends B> g2, Function2<? super A, ? super B, ? extends R> mf) {
        return getS2(g1, g2).map(t -> mf.apply(t._1,t._2));
    }

    public <A, B, R> Mono2<C, R> getS2mapM(Function<? super C, ? extends A> g1, Function<? super C, ? extends B> g2, Function2<? super A, ? super B, Mono<@NonNull R>> mf) {
        return getS2(g1, g2).mapM(t -> mf.apply(t._1, t._2));
    }

    public <A, B, R> Mono2<C, R> getS2mapF(Function<? super C, ? extends A> g1, Function<? super C, ? extends B> g2, Function2<? super A, ? super B, ? extends CompletionStage<R>> mf) {
        return getS2(g1, g2).mapF(t -> mf.apply(t._1,t._2));
    }

    public <A1, A2, A3> Mono2<C, Tuple3<A1, A2, A3>> getS3(Function<? super C,? extends A1> g1, Function<? super C, ? extends A2> g2, Function<? super C, ? extends A3> g3) {
        return getS(c -> Tuple.of(g1.apply(c), g2.apply(c), g3.apply(c)));
    }

    public <A1, A2, A3, R> Mono2<C, R> getS3map(Function<? super C,? extends A1> g1, Function<? super C, ? extends A2> g2, Function<? super C, ? extends A3> g3, Function3<A1, A2, A3, R> mf) {
        return getS3(g1, g2, g3).map(t -> mf.apply(t._1,t._2,t._3));
    }

    public <A1, A2, A3, R> Mono2<C, R> getS3mapM(Function<? super C,? extends A1> g1, Function<? super C, ? extends A2> g2, Function<? super C, ? extends A3> g3, Function3<A1, A2, A3, Mono<@NonNull R>> mf) {
        return getS3(g1, g2, g3).mapM(t -> mf.apply(t._1,t._2,t._3));
    }

    public <A1, A2, A3, R> Mono2<C, R> getS3mapF(Function<? super C,? extends A1> g1, Function<? super C, ? extends A2> g2, Function<? super C, ? extends A3> g3, Function3<A1, A2, A3, CompletableFuture<R>> mf) {
        return getS3(g1, g2, g3).mapF(t -> mf.apply(t._1,t._2,t._3));
    }

    public <A1, A2, A3, A4> Mono2<C, Tuple4<A1, A2, A3, A4>> getS4(Function<? super C,? extends A1> g1, Function<? super C, ? extends A2> g2, Function<? super C, ? extends A3> g3, Function<? super C, ? extends A4> g4) {
        return getS(c -> Tuple.of(g1.apply(c), g2.apply(c), g3.apply(c), g4.apply(c)));
    }

    public <A1, A2, A3, A4, R> Mono2<C, R> getS4map(Function<? super C,? extends A1> g1, Function<? super C, ? extends A2> g2, Function<? super C, ? extends A3> g3, Function<? super C, ? extends A4> g4, Function4<? super A1, ? super A2,? super A3,? super A4, ? extends R> mf) {
        return getS4(g1, g2, g3, g4).map(t -> mf.apply(t._1,t._2,t._3,t._4));
    }

    public <A1, A2, A3, A4, R> Mono2<C, R> getS4mapM(Function<? super C,? extends A1> g1, Function<? super C, ? extends A2> g2, Function<? super C, ? extends A3> g3, Function<? super C, ? extends A4> g4, Function4<? super A1, ? super A2,? super A3,? super A4, Mono<@NonNull R>> mf) {
        return getS4(g1, g2, g3, g4).mapM(t -> mf.apply(t._1,t._2,t._3,t._4));
    }

    public <A1, A2, A3, A4, R> Mono2<C, R> getS4mapF(Function<? super C,? extends A1> g1, Function<? super C, ? extends A2> g2, Function<? super C, ? extends A3> g3, Function<? super C, ? extends A4> g4, Function4<? super A1, ? super A2,? super A3,? super A4, ? extends CompletionStage<R>> mf) {
        return getS4(g1, g2, g3, g4).mapF(t -> mf.apply(t._1,t._2,t._3,t._4));
    }

    public <A1, A2, A3, A4, A5> Mono2<C, Tuple5<A1, A2, A3, A4, A5>> getS5(Function<? super C,? extends A1> g1, Function<? super C, ? extends A2> g2, Function<? super C, ? extends A3> g3, Function<? super C, ? extends A4> g4, Function<? super C, ? extends A5> g5) {
        return getS(c -> Tuple.of(g1.apply(c), g2.apply(c), g3.apply(c), g4.apply(c), g5.apply(c)));
    }

    public <A1, A2, A3, A4, A5, R> Mono2<C, R> getS5map(Function<? super C,? extends A1> g1, Function<? super C, ? extends A2> g2, Function<? super C, ? extends A3> g3, Function<? super C, ? extends A4> g4, Function<? super C, ? extends A5> g5, Function5<? super A1, ? super A2, ? super A3, ? super A4, ? super A5, ? extends R> mf) {
        return getS5(g1, g2, g3, g4, g5).map(t -> mf.apply(t._1,t._2,t._3,t._4, t._5));
    }

    public <A1, A2, A3, A4, A5, R> Mono2<C, R> getS5mapM(Function<? super C,? extends A1> g1, Function<? super C, ? extends A2> g2, Function<? super C, ? extends A3> g3, Function<? super C, ? extends A4> g4, Function<? super C, ? extends A5> g5, Function5<? super A1, ? super A2, ? super A3, ? super A4, ? super A5, Mono<@NonNull R>> mf) {
        return getS5(g1, g2, g3, g4, g5).mapM(t -> mf.apply(t._1,t._2,t._3,t._4, t._5));
    }

    public <A1, A2, A3, A4, A5, R> Mono2<C, R> getS5mapF(Function<? super C,? extends A1> g1, Function<? super C, ? extends A2> g2, Function<? super C, ? extends A3> g3, Function<? super C, ? extends A4> g4, Function<? super C, ? extends A5> g5, Function5<? super A1, ? super A2, ? super A3, ? super A4, ? super A5, ? extends CompletionStage<R>> mf) {
        return getS5(g1, g2, g3, g4, g5).mapF(t -> mf.apply(t._1,t._2,t._3,t._4, t._5));
    }

    public <A1, A2, A3, A4, A5, A6> Mono2<C, Tuple6<A1, A2, A3, A4, A5, A6>> getS6(Function<? super C,? extends A1> g1, Function<? super C, ? extends A2> g2, Function<? super C, ? extends A3> g3, Function<? super C, ? extends A4> g4, Function<? super C, ? extends A5> g5, Function<? super C, ? extends A6> g6) {
        return getS(c -> Tuple.of(g1.apply(c), g2.apply(c), g3.apply(c), g4.apply(c), g5.apply(c), g6.apply(c)));
    }

    public <A1, A2, A3, A4, A5, A6, R> Mono2<C, R> getS6map(Function<? super C,? extends A1> g1, Function<? super C, ? extends A2> g2, Function<? super C, ? extends A3> g3, Function<? super C, ? extends A4> g4, Function<? super C, ? extends A5> g5, Function<? super C, ? extends A6> g6, Function6<? super A1, ? super A2, ? super A3, ? super A4, ? super A5, ? super A6, ? extends R> mf) {
        return getS6(g1, g2, g3, g4, g5, g6).map(t -> mf.apply(t._1, t._2, t._3, t._4, t._5, t._6));
    }

    public <A1, A2, A3, A4, A5, A6, R> Mono2<C, R> getS6mapM(Function<? super C,? extends A1> g1, Function<? super C, ? extends A2> g2, Function<? super C, ? extends A3> g3, Function<? super C, ? extends A4> g4, Function<? super C, ? extends A5> g5, Function<? super C, ? extends A6> g6, Function6<? super A1, ? super A2, ? super A3, ? super A4, ? super A5, ? super A6, Mono<@NonNull R>> mf) {
        return getS6(g1, g2, g3, g4, g5, g6).mapM(t -> mf.apply(t._1, t._2, t._3, t._4, t._5, t._6));
    }

    public <A1, A2, A3, A4, A5, A6, R> Mono2<C, R> getS6mapF(Function<? super C,? extends A1> g1, Function<? super C, ? extends A2> g2, Function<? super C, ? extends A3> g3, Function<? super C, ? extends A4> g4, Function<? super C, ? extends A5> g5, Function<? super C, ? extends A6> g6, Function6<? super A1, ? super A2, ? super A3, ? super A4, ? super A5, ? super A6, ? extends CompletionStage<R>> mf) {
        return getS6(g1, g2, g3, g4, g5, g6).mapF(t -> mf.apply(t._1, t._2, t._3, t._4, t._5, t._6));
    }

    public <A1, A2, A3, A4, A5, A6, A7> Mono2<C, Tuple7<A1, A2, A3, A4, A5, A6, A7>> getS7(Function<? super C,? extends A1> g1, Function<? super C, ? extends A2> g2, Function<? super C, ? extends A3> g3, Function<? super C, ? extends A4> g4, Function<? super C, ? extends A5> g5, Function<? super C, ? extends A6> g6, Function<? super C, ? extends A7> g7) {
        return getS(c -> Tuple.of(g1.apply(c), g2.apply(c), g3.apply(c), g4.apply(c), g5.apply(c), g6.apply(c), g7.apply(c)));
    }

    public <A1, A2, A3, A4, A5, A6, A7, R> Mono2<C, R> getS7map(Function<? super C,? extends A1> g1, Function<? super C, ? extends A2> g2, Function<? super C, ? extends A3> g3, Function<? super C, ? extends A4> g4, Function<? super C, ? extends A5> g5, Function<? super C, ? extends A6> g6, Function<? super C, ? extends A7> g7, Function7<? super A1, ? super A2, ? super A3, ? super A4, ? super A5, ? super A6, ? super A7, ? extends R> mf) {
        return getS7(g1, g2, g3, g4, g5, g6, g7).map(t -> mf.apply(t._1, t._2, t._3, t._4, t._5, t._6, t._7));
    }

    public <A1, A2, A3, A4, A5, A6, A7, R> Mono2<C, R> getS7mapM(Function<? super C,? extends A1> g1, Function<? super C, ? extends A2> g2, Function<? super C, ? extends A3> g3, Function<? super C, ? extends A4> g4, Function<? super C, ? extends A5> g5, Function<? super C, ? extends A6> g6, Function<? super C, ? extends A7> g7, Function7<? super A1, ? super A2, ? super A3, ? super A4, ? super A5, ? super A6, ? super A7, Mono<@NonNull R>> mf) {
        return getS7(g1, g2, g3, g4, g5, g6, g7).mapM(t -> mf.apply(t._1, t._2, t._3, t._4, t._5, t._6, t._7));
    }

    public <A1, A2, A3, A4, A5, A6, A7, R> Mono2<C, R> getS7mapF(Function<? super C,? extends A1> g1, Function<? super C, ? extends A2> g2, Function<? super C, ? extends A3> g3, Function<? super C, ? extends A4> g4, Function<? super C, ? extends A5> g5, Function<? super C, ? extends A6> g6, Function<? super C, ? extends A7> g7, Function7<? super A1, ? super A2, ? super A3, ? super A4, ? super A5, ? super A6, ? super A7, ? extends CompletionStage<R>> mf) {
        return getS7(g1, g2, g3, g4, g5, g6, g7).mapF(t -> mf.apply(t._1, t._2, t._3, t._4, t._5, t._6, t._7));
    }

    public <A1, A2, A3, A4, A5, A6, A7, A8> Mono2<C, Tuple8<A1, A2, A3, A4, A5, A6, A7, A8>> getS8(Function<? super C,? extends A1> g1, Function<? super C, ? extends A2> g2, Function<? super C, ? extends A3> g3, Function<? super C, ? extends A4> g4, Function<? super C, ? extends A5> g5, Function<? super C, ? extends A6> g6, Function<? super C, ? extends A7> g7, Function<? super C, ? extends A8> g8) {
        return getS(c -> Tuple.of(g1.apply(c), g2.apply(c), g3.apply(c), g4.apply(c), g5.apply(c), g6.apply(c), g7.apply(c), g8.apply(c)));
    }

    public <A1, A2, A3, A4, A5, A6, A7, A8, R> Mono2<C, R> getS8map(Function<? super C,? extends A1> g1, Function<? super C, ? extends A2> g2, Function<? super C, ? extends A3> g3, Function<? super C, ? extends A4> g4, Function<? super C, ? extends A5> g5, Function<? super C, ? extends A6> g6, Function<? super C, ? extends A7> g7, Function<? super C, ? extends A8> g8, Function8<? super A1, ? super A2, ? super A3, ? super A4, ? super A5, ? super A6, ? super A7, ? super A8, ? extends R> mf) {
        return getS8(g1, g2, g3, g4, g5, g6, g7, g8).map(t -> mf.apply(t._1, t._2, t._3, t._4, t._5, t._6, t._7, t._8));
    }

    public <A1, A2, A3, A4, A5, A6, A7, A8, R> Mono2<C, R> getS8mapM(Function<? super C,? extends A1> g1, Function<? super C, ? extends A2> g2, Function<? super C, ? extends A3> g3, Function<? super C, ? extends A4> g4, Function<? super C, ? extends A5> g5, Function<? super C, ? extends A6> g6, Function<? super C, ? extends A7> g7, Function<? super C, ? extends A8> g8, Function8<? super A1, ? super A2, ? super A3, ? super A4, ? super A5, ? super A6, ? super A7, ? super A8, Mono<@NonNull R>> mf) {
        return getS8(g1, g2, g3, g4, g5, g6, g7, g8).mapM(t -> mf.apply(t._1, t._2, t._3, t._4, t._5, t._6, t._7, t._8));
    }

    public <A1, A2, A3, A4, A5, A6, A7, A8, R> Mono2<C, R> getS8mapF(Function<? super C,? extends A1> g1, Function<? super C, ? extends A2> g2, Function<? super C, ? extends A3> g3, Function<? super C, ? extends A4> g4, Function<? super C, ? extends A5> g5, Function<? super C, ? extends A6> g6, Function<? super C, ? extends A7> g7, Function<? super C, ? extends A8> g8, Function8<? super A1, ? super A2, ? super A3, ? super A4, ? super A5, ? super A6, ? super A7, ? super A8, ? extends CompletionStage<R>> mf) {
        return getS8(g1, g2, g3, g4, g5, g6, g7, g8).mapF(t -> mf.apply(t._1, t._2, t._3, t._4, t._5, t._6, t._7, t._8));
    }
}
