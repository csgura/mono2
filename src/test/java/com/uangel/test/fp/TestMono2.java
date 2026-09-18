package com.uangel.test.fp;

import com.uangel.fp.Mono2;
import io.vavr.Tuple;
import io.vavr.control.Try;
import lombok.AllArgsConstructor;
import lombok.NoArgsConstructor;
import lombok.Value;
import lombok.With;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.concurrent.ExecutionException;

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
}
