package io.whitefox.api.deltasharing;

import io.whitefox.core.Schema;
import io.whitefox.core.Share;
import io.whitefox.core.Table;
import java.util.List;
import java.util.function.Function;
import java.util.stream.Collectors;

public class Mappers {
  public static io.whitefox.api.deltasharing.model.Share share2api(Share p) {
    return new io.whitefox.api.deltasharing.model.Share().id(p.id()).name(p.name());
  }

  public static io.whitefox.api.deltasharing.model.Schema schema2api(Schema schema) {
    return new io.whitefox.api.deltasharing.model.Schema()
        .name(schema.name())
        .share(schema.share());
  }

  public static io.whitefox.api.deltasharing.model.Table table2api(Table table) {
    return new io.whitefox.api.deltasharing.model.Table()
        .name(table.name())
        .share(table.share())
        .schema(table.schema());
  }

  public static <A, B> List<B> mapList(List<A> list, Function<A, B> f) {
    return list.stream().map(f).collect(Collectors.toList());
  }
}
