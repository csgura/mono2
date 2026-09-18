package com.uangel.test.fp;

import com.uangel.fp.Mono2;
import io.vavr.Tuple;
import io.vavr.Tuple0;
import io.vavr.Tuple2;
import lombok.AllArgsConstructor;
import lombok.NoArgsConstructor;
import lombok.Value;
import lombok.With;
import org.jspecify.annotations.NonNull;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Mono;

import java.util.NoSuchElementException;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

@Value
@With
@AllArgsConstructor
@NoArgsConstructor(staticName = "empty", force = true)
class StyleContext {
    int result1;
    int result2;
    int sum;
    Throwable errorIndication;
}

public class Mono2CodingStyleTest {
    CompletableFuture<Integer> getResult1() {
        return CompletableFuture.completedFuture(10);
    }

    Mono<@NonNull Integer> getResult2(StyleContext ctx, int result1) {
        return Mono.just(result1 + 21);
    }

    Mono<@NonNull Integer> getResult2Fail(StyleContext ctx, int result1) {
        return Mono.error(new NoSuchElementException("not found"));
    }

    Mono<@NonNull Integer> sum(StyleContext ctx) {
        return Mono.fromCallable(() -> {
           return ctx.getResult1() + ctx.getResult2();
        });
    }
    @Test
    public void codingStyleAllSuccess() throws ExecutionException, InterruptedException {
        // 단일 Mono<V> 는 사용하다 보면 불편한 점이 많은데,
        // C가 Context 라고 하면
        // Mono<C> 를 가지고 map 이나 flatMap 으로 계산하고 나면
        // Mono<V> 로 바뀌면서 C 가 사라진다는 점이 제일 불편합니다.
        // callback hell 이 발생할 수도 있는 것도 문제점입니다.

        // Mono2<C,V> 는  C 를 유지하면서 V 에 대한 계산을 할 수 있어 편합니다.
        // naming convention 은 Mono2.java 에 설명되어 있습니다.

        // 아래 예제에서는 var 대신 explicit 타입으로 선언했는데, 실제로는 var 를 사용하고, chain 을 사용하여 불필요한 변수 선언이 없도록 합니다.
        // 처음에는 V가 빈 채로 생성됩니다.
        Mono2<StyleContext, Tuple0> m1 = Mono2.contextOf(StyleContext.empty());

        // 계산 결과가 V에 저장됩니다.
        Mono2<StyleContext, Integer> m2 = m1.mapF(unit -> getResult1());

        // putWith 를 사용하면 계산 결과를 C 에 update 할 수 있습니다.
        Mono2<StyleContext, Tuple0> m3 = m2.putWith(StyleContext::withResult1);

        // getS[N]map 시리즈를 사용하면,  C 에 있는 값들을 사용해서  function을 호출 할 수 있습니다.
        // chain 으로 바로 putWith 를 사용해서 C 에 반영할 수 있습니다.
        Mono2<StyleContext, Tuple0> m4 = m3.getS2mapM(c -> c, StyleContext::getResult1, this::getResult2)
            .putWith(StyleContext::withResult2);

        // 호출 하려는 함수가 C 만 필요하다면, getC 후에 map을 호출 하면 됩니다.
        Mono2<StyleContext, Integer> m5 = m4.getC().mapM(this::sum);

        // 결과 값이 필요하면 eval 을, context 가 필요하면 exec 을, 둘다 필요하면 run을 호출 합니다.
        var sum = m5.eval().get();
        Assertions.assertEquals(41, sum);

        // var 와 chain 을 사용하면 다음과 같습니다.
        var chained = Mono2.contextOf(StyleContext.empty())
            .mapF(unit -> getResult1())
            .putWith(StyleContext::withResult1)
            .getS2mapM(c -> c, StyleContext::getResult1, this::getResult2)
            .putWith(StyleContext::withResult2)
            .getC().mapM(this::sum)
            .eval().get()
            ;

        Assertions.assertEquals(41, chained);

    }

    @Test
    public void codingStyleRecover() throws ExecutionException, InterruptedException {
        // 단일 Mono<V> 는 사용하다 보면 불편한 점이 많은데,
        // C가 Context 라고 하면
        // Mono<C> 를 가지고 map 이나 flatMap 으로 계산하고 나면
        // Mono<V> 로 바뀌면서 C 가 사라진다는 점이 제일 불편합니다.
        // callback hell 이 발생할 수도 있는 것도 문제점입니다.

        // Mono2<C,V> 는  C 를 유지하면서 V 에 대한 계산을 할 수 있어 편합니다.

        // 아래 예제에서는 var 대신 explicit 타입으로 선언했는데, 실제로는 var 를 사용하고, chain 을 사용하여 불필요한 변수 선언이 없도록 합니다.
        // 처음에는 V가 빈 채로 생성됩니다.
        var m1 = Mono2.contextOf(StyleContext.empty())
        .mapF(unit -> getResult1()).putWith(StyleContext::withResult1);


        // 여기 까지는 codingStyleAllSuccess 와 동일 했는데, 에러가 발생한 경우를 보겠습니다.
        Mono2<StyleContext, Integer> m2 = m1.getS2mapM(c -> c, StyleContext::getResult1, this::getResult2Fail);


        // recover 를 호출하면 실패 직전의 C 값이 아규먼트로 넘어 옵니다.
        // 에러 발생 사실을 C 에 기록해 둘 수 있습니다.
        Mono2<StyleContext, Tuple0> m3 = m2.recover((c , err) ->
            Tuple.of(c.withErrorIndication(err), c.getResult1() + 31)
        ).putWith(StyleContext::withResult2);

        // context 가 필요하면 exec 을 호출 합니다.
        var result = m3.getC().mapM(this::sum).putWith(StyleContext::withSum).exec().get();
        Assertions.assertEquals(51, result._1.getSum());
        // C 에 기록해둔 error 가 있어야 합니다.
        Assertions.assertEquals(NoSuchElementException.class, result._1.getErrorIndication().getClass());


        // recover 없이 exec 실행하면 다음과 같이 됩니다.
        // Future<Tuple2> 로 StyleContext 와 Optional<Throwable> 을 리턴해 주는데
        // 에러가 발생했어도 C 는 실패직전의 C 를 리턴해 줍니다.
        // 에러가 발생했다면 Optional<Throwable> 부분을 확인해야 합니다.
        CompletableFuture<Tuple2<StyleContext, Optional<Throwable>>> m4 = Mono2.contextOf(StyleContext.empty())
            .mapF(unit -> getResult1())
            .putWith(StyleContext::withResult1)
            .getS2mapM(c -> c, StyleContext::getResult1, this::getResult2Fail)
            .putWith(StyleContext::withResult2).
            getC().mapM(this::sum).putWith(StyleContext::withSum)
            .exec();

        // C 를 가져오는 것은 실패 없이 항상 성공합니다.
        var noRecover = m4.get();

        // resutl1 까지는 진행 되었기 때문에 result1 의 값은 10이 들어 있습니다.
        Assertions.assertEquals(10, noRecover._1.getResult1());
        // result2 부터 에러가 발생했기 때문에 값들이 0입니다.
        Assertions.assertEquals(0, noRecover._1.getResult2());
        Assertions.assertEquals(0, noRecover._1.getSum());

        // 발생한 에러는 두번째 리턴인 Optional 부분에 있습니다.
        Assertions.assertTrue( noRecover._2.isPresent());
        Assertions.assertEquals(NoSuchElementException.class, noRecover._2.get().getClass());

    }


    Mono<@NonNull Integer> asyncSum(int a, int b) {
        return Mono.just(a+b);
    }

    @Test
    public void antiPattern() {
        // mono는 함수 이기 때문에
        // 두번 참조 하면 두번 실행 됩니다.
        // 막으려면 cache 를 호출해 두어야 합니다.
        var m1 = Mono.fromCallable(() -> {
            System.out.println("m1 called");
            return 1;
        });

        var m2 = Mono.just(2);

        var m3 = m1.flatMap( v1 -> m2.flatMap( v2 -> asyncSum(v1,v2)));

        // m1을 참조해서 m3 를 만들었는데, m4 를 만들기 위해서 m1을 한번더 참조하면
        // m1 called 가 두번 출력됩니다.
        var m4 = m3.flatMap(v3 -> m1.map(v1 -> v1+v3));
        var sum = m4.block();
        Assertions.assertEquals(4, sum);

    }
}
