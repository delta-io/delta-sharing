package io.whitefox.api.server;

import java.time.OffsetDateTime;
import java.util.List;
import java.util.function.Function;
import java.util.stream.Collectors;

public class CommonMappers {

  public static long parseTimestamp(String ts) {
    return OffsetDateTime.parse(ts, java.time.format.DateTimeFormatter.ISO_OFFSET_DATE_TIME)
        .toInstant()
        .toEpochMilli();
  }

  public static <A, B> List<B> mapList(List<A> list, Function<A, B> f) {
    return list.stream().map(f).collect(Collectors.toList());
  }
}
