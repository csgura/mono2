package com.uangel.fp;

import io.vavr.Function2;
import io.vavr.collection.List;


public class ListTraversable {

    public static <C,V,U> Mono2<C, List<U>> traverseM2(List<V> list , C c, Function2<C,V, Mono2<C,U>> f) {
        if (list.isEmpty()) {
            return Mono2.contextOf(c).replace(List.of());
        }

        var sub = list.tail();
        return f.apply(c,  list.get(0))
            .zflatMap(
                (nc, head) ->
                    traverseM2(sub, nc,  f)
                        .map(tail -> tail.prepend(head))
        );
    }
}
