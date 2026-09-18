package com.uangel.fp;


import io.vavr.Function2;
import io.vavr.control.Try;

import java.util.NoSuchElementException;
import java.util.Optional;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.function.Supplier;

// Mono2<C,Optional<V>> Monad Transformer
public class OptionalM2 {
    public static <C,V,R>  Mono2<C, Optional<R>> submap(Mono2<C,Optional<V>> m2, Function<? super V, ? extends R> mf ) {
        return m2.map(o -> o.map(mf));
    }

    public static <C,V,R>  Mono2<C, Optional<R>> subflatMap(Mono2<C,Optional<V>> m2, Function<? super V, ? extends Optional<R>> mf ) {
        return m2.map(o -> o.flatMap(mf));
    }

    public static <C,V>  Mono2<C, Optional<V>> subfilter(Mono2<C,Optional<V>> m2, Predicate<? super V> mf ) {
        return m2.map(o -> o.filter(mf));
    }

    public static <C,V>  Mono2<C, Optional<V>> subfilter(Mono2<C,Optional<V>> m2, Function2<? super C, ? super V, Boolean> mf ) {
        return m2.zmap((c, o) -> o.filter(iv -> mf.apply(c, iv)));
    }

    public static <C,V> Mono2<C, V> subget(Mono2<C,Optional<V>> m2, Supplier<Throwable> errf) {
        return m2.mapT(ov -> ov.map(Try::success).orElseGet(() -> Try.failure(errf.get())));
    }

    public static <C,V> Mono2<C, V> subget(Mono2<C,Optional<V>> m2, String message) {
        return m2.mapT(ov -> ov.map(Try::success).orElseGet(() -> Try.failure(new NoSuchElementException(message))));
    }

}
