// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates.
// Proprietary code. All rights reserved.

package com.daml.ledger.javaapi.data;

import com.daml.ledger.api.v1.ValueOuterClass;
import java.time.Instant;
import java.util.Objects;
import java.util.concurrent.TimeUnit;
import org.checkerframework.checker.nullness.qual.NonNull;

/**
 * A Timestamp value is represented as microseconds since the UNIX epoch.
 *
 * @see com.daml.ledger.api.v1.ValueOuterClass.Value#getTimestamp()
 */
public final class Timestamp extends Value {

  /**
   * Constructs a {@link Timestamp} from milliseconds since UNIX epoch.
   *
   * @param millis milliseconds since UNIX epoch.
   */
  @NonNull
  public static Timestamp fromMillis(long millis) {
    return new Timestamp(millis * 1000);
  }

  /**
   * Constructs a {@link Timestamp} value from an {@link Instant} up to microsecond precision. This
   * is a lossy conversion as nanoseconds are not preserved.
   */
  @NonNull
  public static Timestamp fromInstant(@NonNull Instant instant) {
    return new Timestamp(instant.getEpochSecond() * 1_000_000L + instant.getNano() / 1000L);
  }

  private final long value;

  /**
   * Constructs a {@link Timestamp} from a microsecond value.
   *
   * @param value The number of microseconds since UNIX epoch.
   */
  public Timestamp(long value) {
    this.value = value;
  }

  /**
   * This is an alias for {@link Timestamp#toInstant()}
   *
   * @return the microseconds stored in this timestamp
   */
  @NonNull
  public Instant getValue() {
    return toInstant();
  }

  @NonNull
  public long getMicroseconds() {
    return value;
  }

  /** @return The point in time represented by this timestamp as {@link Instant}. */
  public Instant toInstant() {
    return Instant.ofEpochSecond(TimeUnit.MICROSECONDS.toSeconds(value), value % 1_000_000 * 1000);
  }

  @Override
  public ValueOuterClass.Value toProto() {
    return ValueOuterClass.Value.newBuilder().setTimestamp(this.value).build();
  }

  @Override
  public String toString() {
    return "Timestamp{" + "value=" + value + '}';
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    Timestamp timestamp = (Timestamp) o;
    return value == timestamp.value;
  }

  @Override
  public int hashCode() {

    return Objects.hash(value);
  }
}
