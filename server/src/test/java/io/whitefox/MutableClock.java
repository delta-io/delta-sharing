package io.whitefox;

import java.time.Clock;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZoneOffset;

public class MutableClock extends Clock {

  private Instant time;

  public MutableClock() {
    this(Instant.ofEpochMilli(0));
  }

  public MutableClock(Instant instant) {
    time = instant;
  }

  @Override
  public ZoneId getZone() {
    return ZoneOffset.UTC;
  }

  @Override
  public Clock withZone(ZoneId zone) {
    return new MutableClock();
  }

  @Override
  public Instant instant() {
    return time;
  }

  public void tickMillis(long millis) {
    time = time.plusMillis(millis);
  }

  public void tickSeconds(long millis) {
    time = time.plusSeconds(millis);
  }
}
