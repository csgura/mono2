package com.uangel.test.fp;

import com.uangel.fp.Mono2;
import io.vavr.Tuple;
import io.vavr.Tuple0;
import io.vavr.control.Try;
import lombok.AllArgsConstructor;
import lombok.NoArgsConstructor;
import lombok.Value;
import lombok.With;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Mono;

import java.io.IOException;
import java.util.concurrent.CompletableFuture;
import java.util.function.Function;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicReference;

@Value
@With
@AllArgsConstructor
@NoArgsConstructor(force = true, staticName = "empty")
class RecoverContext {
    String status;
}

public class TestMono2 {
    @Test
    public void testMono2Recover() throws ExecutionException, InterruptedException {
        var m = Mono2.contextOf(RecoverContext.empty())
            .replace("Begin")
            .putWith(RecoverContext::withStatus)
            .mapT(u -> Try.<String>failure(new IOException("ioerror")))
            .recover((c, err) -> Tuple.of(c, c.getStatus()));

        var value = m.value();

        Assertions.assertEquals("Begin", value.block());

        var context =  Mono2.contextOf(RecoverContext.empty())
            .replace("Context")
            .putWith(RecoverContext::withStatus)
            .mapT(u -> Try.<String>failure(new IOException("ioerror"))).context().block();

        Assertions.assertEquals("Context", context._1.getStatus());
        Assertions.assertTrue(context._2.isPresent());
        Assertions.assertEquals(context._2.get().getClass(), IOException.class);

        var res =  Mono2.contextOf(RecoverContext.empty())
            .replace("Run")
            .putWith(RecoverContext::withStatus)
            .mapT(u -> Try.<String>failure(new IOException("ioerror"))).run().get();

        Assertions.assertEquals("Run", res._1.getStatus());
        Assertions.assertTrue(res._2.isFailure());
        Assertions.assertEquals(res._2.failed().get().getClass(), IOException.class);

    }

    @Test
    public void testFromFutureFailurePreservesContext() throws ExecutionException, InterruptedException {
        var failed = new CompletableFuture<String>();
        failed.completeExceptionally(new IOException("fromFuture"));
        var m = Mono2.fromFuture("F", failed);

        Assertions.assertEquals("recovered:F", m.recover((c, err) -> Tuple.of(c, "recovered:" + c)).value().block());

        var ctx = m.context().block();
        Assertions.assertEquals("F", ctx._1);
        Assertions.assertEquals(IOException.class, ctx._2.orElseThrow().getClass());

        var res = m.run().get();
        Assertions.assertEquals("F", res._1);
        Assertions.assertTrue(res._2.isFailure());
        Assertions.assertEquals(IOException.class, res._2.failed().get().getClass());
    }

    @Test
    public void testMapThrowIsRecoverable() {
        var m = Mono2.of("MAP", "x")
            .map(v -> {
                throw new IllegalStateException("map-boom");
            });

        Assertions.assertEquals("recovered:MAP:IllegalStateException",
            m.recover((c, err) -> Tuple.of(c, "recovered:" + c + ":" + err.getClass().getSimpleName()))
                .value().block());

        var thrown = Assertions.assertThrows(IllegalStateException.class, () -> m.value().block());
        Assertions.assertEquals("map-boom", thrown.getMessage());
    }

    @Test
    public void testFlatMapFailureKeepsInnerContextAndOriginalError() {
        var m = Mono2.of("Outer", "x")
            .flatMap(v -> Mono2.of("Inner", v)
                .mapT(u -> Try.<String>failure(new IOException("inner-fail"))));

        var seenC = new AtomicReference<String>();
        var seenErr = new AtomicReference<Throwable>();
        Assertions.assertEquals("recovered", m.recover((c, err) -> {
            seenC.set(c);
            seenErr.set(err);
            return Tuple.of(c, "recovered");
        }).value().block());
        Assertions.assertEquals("Inner", seenC.get());
        Assertions.assertEquals(IOException.class, seenErr.get().getClass());

        var ctx = m.context().block();
        Assertions.assertEquals("Inner", ctx._1);
        Assertions.assertEquals(IOException.class, ctx._2.orElseThrow().getClass());

        var thrown = Assertions.assertThrows(Exception.class, () -> m.value().block());
        Assertions.assertEquals(IOException.class, rootCause(thrown).getClass());
    }

    @Test
    public void testFlatMapSuccessUsesInnerContext() {
        var m = Mono2.of("Outer", "x").flatMap(v -> Mono2.of("Inner", v + "!"));
        Assertions.assertEquals("x!", m.value().block());
        Assertions.assertEquals("Inner", m.context().block()._1);
        Assertions.assertTrue(m.context().block()._2.isEmpty());
    }

    @Test
    public void testTransformMDoesNotClobberInnerContext() {
        var inner = Mono2.of("InnerT", "v")
            .mapT(u -> Try.<String>failure(new IOException("tm-fail")));
        var m = Mono2.of("OuterT", "x").transformM((c, v) -> inner.mono);

        var ctx = m.context().block();
        Assertions.assertEquals("InnerT", ctx._1);
        Assertions.assertEquals(IOException.class, ctx._2.orElseThrow().getClass());
    }

    @Test
    public void testModifyErrorKeepsOriginalThrowable() {
        var m = Mono2.of("C", "v")
            .mapT(u -> Try.<String>failure(new IOException("mod-fail")))
            .modify(c -> c + "-ok", (c, err) -> c + ":" + err.getClass().getSimpleName());

        var ctx = m.context().block();
        Assertions.assertEquals("C:IOException", ctx._1);
        Assertions.assertEquals(IOException.class, ctx._2.orElseThrow().getClass());

        var seenErr = new AtomicReference<Throwable>();
        m.recover((c, err) -> {
            seenErr.set(err);
            return Tuple.of(c, "ok");
        }).value().block();
        Assertions.assertEquals(IOException.class, seenErr.get().getClass());
    }

    @Test
    public void testModifyWithErrorPassesOriginalThrowableToCallback() {
        var m = Mono2.of("C", "v")
            .mapT(u -> Try.<String>failure(new IOException("modw-fail")))
            .modifyWith((c, v) -> c + "-ok", (c, err) -> c + ":" + err.getClass().getSimpleName());

        var ctx = m.context().block();
        Assertions.assertEquals("C:IOException", ctx._1);
        Assertions.assertEquals(IOException.class, ctx._2.orElseThrow().getClass());
    }

    @Test
    public void testEitherOnSuccessThrowGoesToOnFailure() {
        var m = Mono2.of("E", "v")
            .either(
                (c, v) -> {
                    throw new RuntimeException("either-boom");
                },
                (c, err) -> Tuple.of(c, "fail:" + err.getMessage())
            );

        Assertions.assertEquals("fail:either-boom", m.value().block());
        var ctx = m.context().block();
        Assertions.assertEquals("E", ctx._1);
        Assertions.assertTrue(ctx._2.isEmpty());
    }

    @Test
    public void testEitherMSuccessPathFailureRecovers() {
        var m = Mono2.of("EM", "v")
            .eitherM(
                (c, v) -> Mono.error(new IOException("eitherM-fail")),
                (c, err) -> Mono.just(Tuple.of(c, "fail:" + err.getClass().getSimpleName() + ":" + c))
            );

        Assertions.assertEquals("fail:IOException:EM", m.value().block());
    }

    @Test
    public void testRecoverMFailurePreservesContext() {
        var m = Mono2.of("R", "v")
            .mapT(u -> Try.<String>failure(new IOException("first")))
            .recoverM((c, err) -> Mono.error(new IllegalStateException("second")));

        var ctx = m.context().block();
        Assertions.assertEquals("R", ctx._1);
        Assertions.assertEquals(IllegalStateException.class, ctx._2.orElseThrow().getClass());
        Assertions.assertEquals("second", ctx._2.orElseThrow().getMessage());

        Assertions.assertEquals("got:second",
            m.recover((c, err) -> Tuple.of(c, "got:" + err.getMessage())).value().block());
    }

    @Test
    public void testMapErrorOnFromFuture() {
        var failed = new CompletableFuture<String>();
        failed.completeExceptionally(new IOException("mapError-raw"));
        var m = Mono2.fromFuture("ME", failed)
            .mapError((c, err) -> new IllegalStateException("mapped:" + c));

        var ctx = m.context().block();
        Assertions.assertEquals("ME", ctx._1);
        Assertions.assertEquals(IllegalStateException.class, ctx._2.orElseThrow().getClass());
        Assertions.assertEquals("mapped:ME", ctx._2.orElseThrow().getMessage());
    }

    @Test
    public void testPutWithAndTransformThrowAreRecoverable() {
        var put = Mono2.of("C", "v")
            .putWith((c, v) -> {
                throw new IllegalStateException("put");
            });
        var putCtx = put.context().block();
        Assertions.assertEquals("C", putCtx._1);
        Assertions.assertEquals("put", putCtx._2.orElseThrow().getMessage());
        Assertions.assertEquals(Tuple0.instance(),
            put.recover((c, err) -> Tuple.of(c, Tuple0.instance())).value().block());

        var transformed = Mono2.of("T", "v")
            .transform((c, v) -> {
                throw new IllegalStateException("transform");
            });
        var transformedCtx = transformed.context().block();
        Assertions.assertEquals("T", transformedCtx._1);
        Assertions.assertEquals("transform", transformedCtx._2.orElseThrow().getMessage());
    }

    @Test
    public void testGetS6To8() {
        var base = Mono2.contextOf(1);
        Function<Integer, Integer> plus = n -> n;

        var t6 = base.getS6(plus, c -> c + 1, c -> c + 2, c -> c + 3, c -> c + 4, c -> c + 5).value().block();
        Assertions.assertEquals(Tuple.of(1, 2, 3, 4, 5, 6), t6);
        Assertions.assertEquals(21, base.getS6map(plus, c -> c + 1, c -> c + 2, c -> c + 3, c -> c + 4, c -> c + 5,
            (a, b, c, d, e, f) -> a + b + c + d + e + f).value().block());
        Assertions.assertEquals(21, base.getS6mapM(plus, c -> c + 1, c -> c + 2, c -> c + 3, c -> c + 4, c -> c + 5,
            (a, b, c, d, e, f) -> Mono.just(a + b + c + d + e + f)).value().block());
        Assertions.assertEquals(21, base.getS6mapF(plus, c -> c + 1, c -> c + 2, c -> c + 3, c -> c + 4, c -> c + 5,
            (a, b, c, d, e, f) -> CompletableFuture.completedFuture(a + b + c + d + e + f)).value().block());

        var t7 = base.getS7(plus, c -> c + 1, c -> c + 2, c -> c + 3, c -> c + 4, c -> c + 5, c -> c + 6).value().block();
        Assertions.assertEquals(Tuple.of(1, 2, 3, 4, 5, 6, 7), t7);
        Assertions.assertEquals(28, base.getS7map(plus, c -> c + 1, c -> c + 2, c -> c + 3, c -> c + 4, c -> c + 5, c -> c + 6,
            (a, b, c, d, e, f, g) -> a + b + c + d + e + f + g).value().block());

        var t8 = base.getS8(plus, c -> c + 1, c -> c + 2, c -> c + 3, c -> c + 4, c -> c + 5, c -> c + 6, c -> c + 7).value().block();
        Assertions.assertEquals(Tuple.of(1, 2, 3, 4, 5, 6, 7, 8), t8);
        Assertions.assertEquals(36, base.getS8map(plus, c -> c + 1, c -> c + 2, c -> c + 3, c -> c + 4, c -> c + 5, c -> c + 6, c -> c + 7,
            (a, b, c, d, e, f, g, h) -> a + b + c + d + e + f + g + h).value().block());
        Assertions.assertEquals(36, base.getS8mapM(plus, c -> c + 1, c -> c + 2, c -> c + 3, c -> c + 4, c -> c + 5, c -> c + 6, c -> c + 7,
            (a, b, c, d, e, f, g, h) -> Mono.just(a + b + c + d + e + f + g + h)).value().block());
        Assertions.assertEquals(36, base.getS8mapF(plus, c -> c + 1, c -> c + 2, c -> c + 3, c -> c + 4, c -> c + 5, c -> c + 6, c -> c + 7,
            (a, b, c, d, e, f, g, h) -> CompletableFuture.completedFuture(a + b + c + d + e + f + g + h)).value().block());
    }

    private static Throwable rootCause(Throwable t) {
        var cur = t;
        while (cur.getCause() != null && cur.getCause() != cur) {
            cur = cur.getCause();
        }
        return cur;
    }
}
