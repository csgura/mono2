package com.uangel.fp;


import io.vavr.*;
import io.vavr.control.Try;
import org.jspecify.annotations.NonNull;
import reactor.core.publisher.Mono;

import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.function.BiConsumer;
import java.util.function.Function;

/*
name convention:
 C 는 Context , V 는 Value 를 의미
 get 은  Context 의 값을  V 로 옮기는 것을 의미
 put 은 V 의 값을 Context로 옮기는 것을 의미
 modify 는 C 를 바꾸는 것을 의미
 transform 은 성공일 때 C,V 둘다 바꾸는 것을 의미
 either는 성공/실패 두가지 경우에 대해 , C,V 둘다 바꾸는 것을 의미
 recover 는 error 를 복구하는 것을 의미

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
    static class ExceptionWithContext extends Throwable {
        private final Object context;
        final Throwable err;

        ExceptionWithContext(Object c, Throwable err) {
            this.context = c;
            this.err = err;
        }

        @SuppressWarnings("unchecked")
        <T> T getContext() {
            return (T) context;
        }
    }

    public final Mono<@NonNull  Tuple2<C, V>> mono;

    private Mono2(Mono<@NonNull Tuple2<C, V>> mono) {
        this.mono = mono;
    }

    private static <C, V> Mono2<C, V> apply(Mono<@NonNull Tuple2<C, V>> mono) {
        return new Mono2<>(mono);
    }

    public static <C, V> Mono2<C, V> fromFuture(C c, CompletableFuture<V> future) {
        return new Mono2<>(Mono.fromFuture(future).map(v -> Tuple.of(c, v)));
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

    public <U> Mono2<C,U> transform(Function2<C, V, Tuple2<C, U>> f) {
        return apply(mono.map(t -> f.tupled().apply(t)));
    }

    public <U> Mono2<C, U> transformM(Function2<C, V, Mono<@NonNull Tuple2<C, U>>> f) {
        return apply(mono.flatMap(t -> f.tupled().apply(t).onErrorMap(err -> new ExceptionWithContext(t._1, err))));
    }

    public <U> Mono2<C, U> transformF(Function2<C, V, CompletableFuture<Tuple2<C, U>>> f) {
        return transformM((c, v) -> Mono.fromFuture(f.apply(c, v)));
    }

    public <U> Mono2<C, U> transformT(Function2<C, V, Try<Tuple2<C, U>>> f) {
        return transformF((c, v) -> f.apply(c, v).toCompletableFuture());
    }

    public Mono2<C, V> recover(Function2<C, Throwable, Tuple2<C, V>> rf) {
        return recoverM((c, err) -> Mono.just(rf.apply(c, err)));
    }

    public Mono2<C, V> mapError(Function2<C, Throwable, Throwable> mf) {
        return apply(mono.onErrorMap(err -> {
            if (err instanceof ExceptionWithContext ei) {
                return new ExceptionWithContext(ei.getContext(), mf.apply(ei.getContext(), ei.err));
            }
            return err;
        }));
    }

    public <CO, R> Mono2<CO, R> either(Function2<C, V, Tuple2<CO, R>> onSuccess, Function2<C, Throwable, Tuple2<CO, R>> onFailure) {
        return apply(mono.map(t -> onSuccess.apply(t._1, t._2)).onErrorResume(err -> {
            if (err instanceof ExceptionWithContext ei) {
                return Mono.just(onFailure.apply(ei.getContext(), ei.err));
            }
            return Mono.error(err);
        }));
    }

    public <R> Mono2<C, R> eitherM(Function2<C, V, Mono<@NonNull Tuple2<C, R>>> onSuccess, Function2<C, Throwable, Mono<@NonNull Tuple2<C, R>>> onFailure) {
        return transformM(onSuccess).recoverM(onFailure);
    }

    public Mono2<C, V> recoverM(Function2<C, Throwable, Mono<@NonNull Tuple2<C, V>>> rf) {
        return apply(mono.onErrorResume(err -> {
            if (err instanceof ExceptionWithContext ei) {
                return rf.apply(ei.getContext(), ei.err);
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

    public <U> Mono2<C, U> map(Function1<V, U> mf) {
        return apply(mono.map(t -> t.map2(mf)));
    }

    public <U> Mono2<C, U> mapM(Function1<V, Mono<@NonNull U>> mf) {
        return apply(mono.flatMap(t -> mf.apply(t._2).onErrorMap(err -> new ExceptionWithContext(t._1, err)).map(u -> Tuple.of(t._1, u))));
    }

    public <U> Mono2<C, U> mapF(Function1<V, CompletableFuture<U>> mf) {
        return mapM(v -> Mono.fromFuture(mf.apply(v)));
    }

    private static <V> Mono<@NonNull V> tryIntoMono(Try<V> tv) {
        return tv.fold(Mono::error, Mono::just);
    }

    public <U> Mono2<C, U> mapT(Function1<V, Try<U>> mf) {
        return mapM(v -> tryIntoMono(mf.apply(v)));
    }

    public <U> Mono2<C, U> flatMap(Function1<V, Mono2<C, U>> mf) {
        return apply(mono.flatMap(t -> mf.apply(t._2).mono.onErrorMap(err -> new ExceptionWithContext(t._1, err))));
    }

    public Mono2<C, Tuple0> putWith(Function2<C, V, C> wf) {
        return apply(mono.map(t -> Tuple.of(wf.apply(t._1, t._2), Tuple0.instance())));
    }



    public Mono2<C, V> putSet(BiConsumer<C, V> wf) {
        return apply(mono.map(t -> {
            wf.accept(t._1, t._2);
            return t;
        }));
    }

    public Mono2<C, C> getC() {
        return apply(mono.map(t -> Tuple.of(t._1, t._1)));
    }

    public <R> Mono2<C, R> getS(Function1<C, R> gf) {
        return getC().map(gf);
    }

    public <R> Mono2<C, R> getT(Function1<C, Try<R>> gf) {
        return getC().mapT(gf);
    }

    public <R> Mono2<C, R> getM(Function1<C, Mono<@NonNull R>> gf) {
        return getC().mapM(gf);
    }

    public <R> Mono2<C, R> getF(Function1<C, CompletableFuture<R>> gf) {
        return getC().mapF(gf);
    }

    public <CO> Mono2<CO, V> modify(Function1<C, CO> onSuccess, Function2<C,Throwable,CO> onError) {
        return apply(mono.map(t -> t.map1(onSuccess)).onErrorMap(err -> {
            if (err instanceof ExceptionWithContext ei) {
                var newc = onError.apply(ei.getContext(), ei.err);
                return new ExceptionWithContext(newc, err);
            }
            return err;
        }));
    }

    // V 혹은 error 를 이용해서 C 를 modify 하는 경우
    public <CO> Mono2<CO, Tuple0> modifyWith(Function2<C, V, CO> onSuccess, Function2<C,Throwable,CO> onError) {
        return apply(mono.map(t -> Tuple.of(onSuccess.apply(t._1, t._2), Tuple0.instance())).onErrorMap(err -> {
            if (err instanceof ExceptionWithContext ec) {
                return new ExceptionWithContext(onError.apply(ec.getContext(), err), err);
            }
            return err;
        }));
    }

    public <A, B> Mono2<C, Tuple2<A, B>> getS2(Function<C, A> g1, Function<C, B> g2) {
        return getS(c -> Tuple.of(g1.apply(c), g2.apply(c)));
    }

    public <A, B, R> Mono2<C, R> getS2map(Function<C, A> g1, Function<C, B> g2, Function2<A, B, R> mf) {
        return getS2(g1, g2).map(mf.tupled());
    }

    public <A, B, R> Mono2<C, R> getS2mapM(Function<C, A> g1, Function<C, B> g2, Function2<A, B, Mono<@NonNull R>> mf) {
        return getS2(g1, g2).mapM(mf.tupled());
    }

    public <A, B, R> Mono2<C, R> getS2mapF(Function<C, A> g1, Function<C, B> g2, Function2<A, B, CompletableFuture<R>> mf) {
        return getS2(g1, g2).mapF(mf.tupled());
    }

    public <A1, A2, A3> Mono2<C, Tuple3<A1, A2, A3>> getS3(Function<C, A1> g1, Function<C, A2> g2, Function<C, A3> g3) {
        return getS(c -> Tuple.of(g1.apply(c), g2.apply(c), g3.apply(c)));
    }

    public <A1, A2, A3, R> Mono2<C, R> getS3map(Function<C, A1> g1, Function<C, A2> g2, Function<C, A3> g3, Function3<A1, A2, A3, R> mf) {
        return getS3(g1, g2, g3).map(mf.tupled());
    }

    public <A1, A2, A3, R> Mono2<C, R> getS3mapM(Function<C, A1> g1, Function<C, A2> g2, Function<C, A3> g3, Function3<A1, A2, A3, Mono<@NonNull R>> mf) {
        return getS3(g1, g2, g3).mapM(mf.tupled());
    }

    public <A1, A2, A3, R> Mono2<C, R> getS3mapF(Function<C, A1> g1, Function<C, A2> g2, Function<C, A3> g3, Function3<A1, A2, A3, CompletableFuture<R>> mf) {
        return getS3(g1, g2, g3).mapF(mf.tupled());
    }

    public <A1, A2, A3, A4> Mono2<C, Tuple4<A1, A2, A3, A4>> getS4(Function<C, A1> g1, Function<C, A2> g2, Function<C, A3> g3, Function<C, A4> g4) {
        return getS(c -> Tuple.of(g1.apply(c), g2.apply(c), g3.apply(c), g4.apply(c)));
    }

    public <A1, A2, A3, A4, R> Mono2<C, R> getS4map(Function<C, A1> g1, Function<C, A2> g2, Function<C, A3> g3, Function<C, A4> g4, Function4<A1, A2, A3, A4, R> mf) {
        return getS4(g1, g2, g3, g4).map(mf.tupled());
    }

    public <A1, A2, A3, A4, R> Mono2<C, R> getS4mapM(Function<C, A1> g1, Function<C, A2> g2, Function<C, A3> g3, Function<C, A4> g4, Function4<A1, A2, A3, A4, Mono<@NonNull R>> mf) {
        return getS4(g1, g2, g3, g4).mapM(mf.tupled());
    }

    public <A1, A2, A3, A4, R> Mono2<C, R> getS4mapF(Function<C, A1> g1, Function<C, A2> g2, Function<C, A3> g3, Function<C, A4> g4, Function4<A1, A2, A3, A4, CompletableFuture<R>> mf) {
        return getS4(g1, g2, g3, g4).mapF(mf.tupled());
    }

    public <A1, A2, A3, A4, A5> Mono2<C, Tuple5<A1, A2, A3, A4, A5>> getS5(Function<C, A1> g1, Function<C, A2> g2, Function<C, A3> g3, Function<C, A4> g4, Function<C, A5> g5) {
        return getS(c -> Tuple.of(g1.apply(c), g2.apply(c), g3.apply(c), g4.apply(c), g5.apply(c)));
    }

    public <A1, A2, A3, A4, A5, R> Mono2<C, R> getS5map(Function<C, A1> g1, Function<C, A2> g2, Function<C, A3> g3, Function<C, A4> g4, Function<C, A5> g5, Function5<A1, A2, A3, A4, A5, R> mf) {
        return getS5(g1, g2, g3, g4, g5).map(mf.tupled());
    }

    public <A1, A2, A3, A4, A5, R> Mono2<C, R> getS5mapM(Function<C, A1> g1, Function<C, A2> g2, Function<C, A3> g3, Function<C, A4> g4, Function<C, A5> g5, Function5<A1, A2, A3, A4, A5, Mono<@NonNull R>> mf) {
        return getS5(g1, g2, g3, g4, g5).mapM(mf.tupled());
    }

    public <A1, A2, A3, A4, A5, R> Mono2<C, R> getS5mapF(Function<C, A1> g1, Function<C, A2> g2, Function<C, A3> g3, Function<C, A4> g4, Function<C, A5> g5, Function5<A1, A2, A3, A4, A5, CompletableFuture<R>> mf) {
        return getS5(g1, g2, g3, g4, g5).mapF(mf.tupled());
    }
}
