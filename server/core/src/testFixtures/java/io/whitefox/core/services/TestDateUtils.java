package io.whitefox.core.services;

import java.sql.Timestamp;
import java.time.OffsetDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Optional;

public class TestDateUtils {
  public static Optional<Timestamp> parseTimestamp(String timestamp) {
    return Optional.ofNullable(timestamp)
        .map(ts -> new Timestamp(OffsetDateTime.parse(ts, DateTimeFormatter.ISO_OFFSET_DATE_TIME)
            .toInstant()
            .toEpochMilli()));
  }
}
