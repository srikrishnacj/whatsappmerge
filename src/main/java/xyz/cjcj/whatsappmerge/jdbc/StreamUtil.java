package xyz.cjcj.whatsappmerge.jdbc;

import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class StreamUtil {
    public static <T> Set<List<T>> chunked(Stream<T> stream, int chunkSize) {
        List<T> list = stream.collect(Collectors.toList());
        return chunked(list, chunkSize);
    }

    public static <T> Set<List<T>> chunked(Iterable iterable, int chunkSize) {
        Set<List<T>> set = new HashSet<>();

        List<T> temp = new LinkedList<>();
        Iterator<T> iterator = iterable.iterator();
        int count = 1;
        while (iterator.hasNext()) {
            T t = iterator.next();
            temp.add(t);
            if (count % chunkSize == 0) {
                set.add(temp);
                temp = new LinkedList<>();
            }
            count++;
        }
        set.add(temp);
        return set;
    }

    public static <T> Stream<T> from(Iterable<T> iterable) {
        List<T> list = new LinkedList<>();
        iterable.forEach(list::add);
        return list.stream();
    }
}
