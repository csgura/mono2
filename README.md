# Mono2

Reactor `Mono<V>` 위에서 Context `C`를 끝까지 들고 가는 래퍼입니다.

`Mono<C>`로 시작했다가 `map` / `flatMap`을 하면 타입이 `Mono<V>`로 바뀌면서 `C`가 사라집니다. 그때마다 `C`를 람다로 닫거나 중첩 callback으로 넘기게 됩니다. `Mono2<C, V>`는 `(C, V)` 쌍을 한 파이프라인으로 유지해서 그 불편을 줄입니다. Haskell `State`에 가깝지만, 실제 타입은 이미 `C`가 들어 있는 `Mono<(C, V)>`이고 실패 시 마지막 `C`를 에러에 붙입니다.

## 요구 사항

- Java 21
- [Reactor](https://projectreactor.io/) 3.8
- [Vavr](https://www.vavr.io/) 1.0 (Tuple, Try, FunctionN)

```xml
<dependency>
    <groupId>com.uangel.fp</groupId>
    <artifactId>mono2</artifactId>
    <version>1.0-SNAPSHOT</version>
</dependency>
```

## 이름 규칙

| 기호 | 의미 |
|---|---|
| `C` | Context |
| `V` | Value |
| `get` | `C` → `V` |
| `put` | `V` → `C` |
| `modify` | `C`만 변경 |
| `transform` | 성공일 때 `C`, `V` 둘 다 변경 |
| `either` | 성공/실패 모두에서 `C`, `V` 변경 |
| `recover` | 에러 복구 |
| `z` (`zmap`, `zgetS`, …) | 콜백이 `C`와 `V`를 같이 받거나, 결과를 `(C에서 꺼낸 값, V)` tuple로 줌 |
| 끝 `S` | Supplier |
| 끝 `T` | Vavr `Try` |
| 끝 `F` | `Future` / `CompletionStage` |
| 끝 `M` | `Mono` |
| 끝 `O` | `Optional` |
| `With` | `(C, V) -> C` |
| `2`…`8` | Vavr Tuple 개수 |

`V` 쪽에서 에러가 나도 `C`는 `ExceptionWithContext`에 남습니다. `recover` / `mapError`에서 그 `C`를 쓸 수 있고, `context()` / `run()` / `exec()`는 에러와 함께 마지막 `C`를 돌려줍니다.

`C`의 타입을 바꾸는 연산은 성공·실패 콜백을 둘 다 받는 `either`와 `modify`뿐입니다. `eitherM`은 실패할 수 있어서 `C` 타입을 바꾸지 않습니다.

## 사용 예

Context는 immutable wither가 잘 맞습니다.

```java
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
```

성공 경로:

```java
var sum = Mono2.contextOf(StyleContext.empty())
    .mapF(unit -> getResult1())                 // V = 10
    .putWith(StyleContext::withResult1)         // C.result1 = 10
    .getS2mapM(c -> c, StyleContext::getResult1, this::getResult2)
    .putWith(StyleContext::withResult2)         // C.result2 = 31
    .getM(this::sum)                            // V = 41
    .eval().get();
```

실패해도 직전 `C`는 남습니다.

```java
var result = Mono2.contextOf(StyleContext.empty())
    .mapF(unit -> getResult1())
    .putWith(StyleContext::withResult1)
    .getS2mapM(c -> c, StyleContext::getResult1, this::getResult2Fail)
    .recover((c, err) -> Tuple.of(c.withErrorIndication(err), c.getResult1() + 31))
    .putWith(StyleContext::withResult2)
    .getM(this::sum)
    .putWith(StyleContext::withSum)
    .exec().get();

// result._1 은 마지막 Context
// result._2 는 Optional<Throwable> (성공이면 empty)
```

`recover` 없이 `exec()`만 호출해도 Future는 성공합니다. 실패한 필드만 비어 있고, 예외는 `Optional`에 들어 있습니다.

## 종료

| 메서드 | 반환 | 용도 |
|---|---|---|
| `eval()` | `CompletableFuture<V>` | 값만 |
| `exec()` | `CompletableFuture<Tuple2<C, Optional<Throwable>>>` | Context + 에러 여부 |
| `run()` | `CompletableFuture<Tuple2<C, Try<V>>>` | Context + 값/에러 |
| `value()` | `Mono<V>` | Reactor로 이어갈 때. 실패면 원본 예외 |
| `context()` | `Mono<Tuple2<C, Optional<Throwable>>>` | `exec()`의 Mono 버전 |

같은 `Mono2`를 두 번 구독하면 소스도 두 번 실행됩니다. 막으려면 `cache()`를 먼저 호출하세요.

## API 요약

### 생성

`of(c, v)`, `contextOf(c)`, `from(c, mono)`, `fromFuture`, `fromCallable`, `fromSupplier`, `defer`, `error(c, err)`

### Context

- `getC` / `getS` / `getM` / `getF` / `getT` — `C`에서 값을 꺼내 `V`로
- `zgetC`, `zgetS`, … — 꺼낸 값과 기존 `V`를 tuple로 (`V`가 마지막)
- `getS2` … `getS8`, `getSNmap` / `mapM` / `mapF` — `C`의 필드를 여러 개 꺼내 함수 호출
- `zgetS2` … `zgetS7`, `zgetSNmap` / `mapM` / `mapF` — getter N개 + 기존 `V`를 tuple로 (`zgetS7` → `Tuple8`)
- `putWith((c, v) -> c')` — `V`를 `C`에 반영하고 `V`는 `Tuple0`
- `putSet` — mutable `C`에 in-place 반영
- `modify` / `modifyWith` — `C` 타입 변경 (성공/실패 콜백 필수)

### Value

`map` / `mapM` / `mapF` / `mapT`, 앞에 `z`가 붙으면 `(C, V)`를 같이 받습니다.  
`replace` / `replaceM` / `replaceF` / `replaceT`, `unit()`, `flatMap`, `then`, `transform*`

### 에러

`recover` / `recoverM` / `recoverF` / `recoverT`, `recover(Class, …)`  
`mapError`, `mapError(Class, …)`  
`either` / `eitherM`  
`retry` / `retryWhen`

### 그 외 Mono에서 옮긴 것

`zipWhen` / `zipWith`, `delayUntil` / `delayElement`, `cast` / `ofType`, `elapsed`  
`doOnNext` / `doOnError` / `doFinally` / `doOnCancel`  
`subscribeOn` / `publishOn`, `cache(Duration)`, `share`, `checkpoint`, `log`

`zipWith`는 현재 값이 나온 뒤에 other를 구독합니다. 병렬 zip이 아닙니다.

`filter`는 public API가 아닙니다. 거절을 empty로 끝내면 `C`가 사라져서 Mono2 모델과 맞지 않습니다. `Optional<V>`가 필요하면 `OptionalM2`를 쓰세요.

`timeout`은 값이 나오기 전에 터지면 `C`를 붙일 수 없습니다. inner `Mono`에 `timeout`을 걸고 `from` / `mapM`으로 감싸면 그 시점의 `C`가 붙습니다.

## OptionalM2

`Mono2<C, Optional<V>>`용 헬퍼입니다.

```java
OptionalM2.submap(m, v -> v.name());
OptionalM2.subflatMap(m, v -> find(v));
OptionalM2.subfilter(m, v -> v.active());
OptionalM2.subget(m, "not found");   // empty면 에러. C는 유지
```

## 빌드

```bash
mvn test
```

## License

Apache License 2.0. `LICENSE` 참고.
