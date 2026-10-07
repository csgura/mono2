package com.uangel.fp;

import io.vavr.control.Try;

import java.util.NoSuchElementException;
import java.util.Optional;
import java.util.function.Supplier;

public class OptionalMonad {
    public static <T> Try<T> intoTry(Optional<T> o , Supplier<Throwable> onEmpty) {
        return o.map(Try::success).orElseGet(() -> Try.failure(onEmpty.get()));
    }


    public static <T> Try<T> intoTry(Optional<T> o , String fmt, Object ... args) {
        return intoTry(o, () -> new NoSuchElementException(String.format(fmt, args)));
    }
}
