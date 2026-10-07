package com.uangel.fp;

import io.vavr.*;
import lombok.experimental.ExtensionMethod;
import reactor.core.publisher.Mono;

import java.util.NoSuchElementException;
import java.util.Optional;
import java.util.concurrent.CompletionStage;
import java.util.function.Function;

@ExtensionMethod(OptionalMonad.class)
public class TupleM2 {
    public static <C,A1,A2> Mono2<C, Tuple2<A1,A2>> t1append(Mono2<C,A1> m2 , Function<? super C, ? extends A2> gf) {
        return m2.getWith(gf, Tuple::of);
    }

    public static <C,A1,A2> Mono2<C, Tuple2<A1,Optional<A2>>> t1appendO(Mono2<C,A1> m2 , Function<? super C, ? extends A2> gf) {
        return m2.getWithO(gf, (t,v) -> Tuple.of(t,v.map(i ->i)));
    }

    public static <C,A1,A2> Mono2<C, Tuple2<A1,A2>> t1appendNonNull(Mono2<C,A1> m2 , Function<? super C, ? extends A2> gf, Function<? super C, Throwable> onNull) {
        return m2.getWithNonNull(gf , Tuple::of, onNull);
    }

    public static <C,A1,A2> Mono2<C, Tuple2<A1,A2>> t1appendNonNull(Mono2<C,A1> m2 , Function<? super C, ? extends A2> gf, String fmt , Object ... args) {
        return m2.getWithNonNull(gf , Tuple::of, fmt, args);
    }

    public static <C,A1,A2,A3> Mono2<C, Tuple3<A1,A2,A3>> t2append(Mono2<C,Tuple2<A1,A2>> m2 , Function<? super C, ? extends A3> gf) {
        return m2.getWith(gf, Tuple2::append);
    }

    public static <C,A1,A2,A3> Mono2<C, Tuple3<A1,A2,Optional<A3>>> t2appendO(Mono2<C,Tuple2<A1,A2>> m2 , Function<? super C, ? extends A3> gf) {
        return m2.getWithO(gf, (t, v) -> t.append(v.map(i -> i )));
    }

    public static <C,A1,A2,A3> Mono2<C, Tuple3<A1,A2,A3>> t2appendNonNull(Mono2<C,Tuple2<A1,A2>> m2 , Function<? super C, ? extends A3> gf,  Function<? super C, Throwable> onNull) {
        return m2.getWithNonNull(gf, Tuple2::append,onNull );
    }

    public static <C,A1,A2,A3> Mono2<C, Tuple3<A1,A2,A3>> t2appendNonNull(Mono2<C,Tuple2<A1,A2>> m2 , Function<? super C, ? extends A3> gf,  String fmt , Object ... args) {
        return m2.getWithNonNull(gf, Tuple2::append, fmt,args);
    }


    public static <C,A1,A2,R> Mono2<C, R> t2map(Mono2<C,Tuple2<A1,A2>> m2 , Function2<? super A1, ? super A2, ? extends R> gf) {
        return m2.map(tp -> gf.apply(tp._1, tp._2));
    }

    public static <C,A1,A2,R> Mono2<C, R> t2mapM(Mono2<C,Tuple2<A1,A2>> m2 , Function2<? super A1, ? super A2, Mono<R>> gf) {
        return m2.mapM(tp -> gf.apply(tp._1, tp._2));
    }

    public static <C,A1,A2,R> Mono2<C, R> t2mapF(Mono2<C,Tuple2<A1,A2>> m2 , Function2<? super A1, ? super A2, ? extends CompletionStage<R>> gf) {
        return m2.mapF(tp -> gf.apply(tp._1, tp._2));
    }

    public static <C,A1,A2,A3,R> Mono2<C, R> t3map(Mono2<C,Tuple3<A1,A2,A3>> m2 , Function3<? super A1, ? super A2, ? super A3, ? extends R> gf) {
        return m2.map(tp -> gf.apply(tp._1, tp._2, tp._3));
    }

    public static <C,A1,A2,A3,R> Mono2<C, R> t3mapM(Mono2<C,Tuple3<A1,A2,A3>> m2 , Function3<? super A1, ? super A2, ? super A3, Mono<R>> gf) {
        return m2.mapM(tp -> gf.apply(tp._1, tp._2, tp._3));
    }

    public static <C,A1,A2,A3,R> Mono2<C, R> t3mapF(Mono2<C,Tuple3<A1,A2,A3>> m2 , Function3<? super A1, ? super A2, ? super A3, ? extends CompletionStage<R>> gf) {
        return m2.mapF(tp -> gf.apply(tp._1, tp._2, tp._3));
    }

}
