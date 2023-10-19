// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.javaapi.data.codegen.json;

import static java.util.Arrays.asList;
import static java.util.Collections.emptyList;
import static java.util.Collections.emptyMap;
import static org.junit.jupiter.api.Assertions.assertEquals;

import com.daml.ledger.javaapi.data.Unit;
import java.io.IOException;
import java.io.StringWriter;
import java.math.BigDecimal;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.Month;
import java.time.ZoneOffset;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import org.junit.jupiter.api.Test;
import org.junit.platform.runner.JUnitPlatform;
import org.junit.runner.RunWith;

@RunWith(JUnitPlatform.class)
public class JsonLfEncodersTest {

  @Test
  void testUnit() throws IOException {
    assertEquals("{}", intoString(JsonLfEncoders.unit));
  }

  @Test
  void testBool() throws IOException {
    checkWriteAll(JsonLfEncoders::bool, Case.of(true, "true"), Case.of(false, "false"));
  }

  @Test
  void testInt64() throws IOException {
    checkWriteAll(
        JsonLfEncoders::int64,
        Case.of(42L, "42"),
        Case.of(-42L, "-42"),
        Case.of(0L, "0"),
        Case.of(-0L, "0"),
        Case.of(9223372036854775807L, "9223372036854775807"),
        Case.of(-9223372036854775808L, "-9223372036854775808"));
  }

  @Test
  void testNumeric() throws IOException {
    checkWriteAll(
        JsonLfEncoders::numeric,
        Case.of(dec("42"), "42"),
        Case.of(dec("-42"), "-42"),
        Case.of(dec("0"), "0"),
        Case.of(dec("-0"), "0"),
        Case.of(dec("0.3"), "0.3"),
        Case.of(
            dec("-9999999999999999999999999999.9999999999"),
            "-9999999999999999999999999999.9999999999"),
        Case.of(
            dec("9999999999999999999999999999.9999999999"),
            "9999999999999999999999999999.9999999999"));
  }

  @Test
  void testTimestamp() throws IOException {
    checkWriteAll(
        JsonLfEncoders::timestamp,
        Case.of(
            timestampUTC(1990, Month.NOVEMBER, 9, 4, 30, 23, 123400),
            "\"1990-11-09T04:30:23.123400Z\""),
        Case.of(
            timestampUTC(1990, Month.NOVEMBER, 9, 4, 30, 23, 123456),
            "\"1990-11-09T04:30:23.123456Z\""),
        Case.of(
            timestampUTC(9999, Month.DECEMBER, 31, 23, 59, 59, 999999),
            "\"9999-12-31T23:59:59.999999Z\""),
        Case.of(
            timestampUTC(1990, Month.NOVEMBER, 9, 4, 30, 23, 123456, 900),
            "\"1990-11-09T04:30:23.123456Z\""),
        Case.of(timestampUTC(1990, Month.NOVEMBER, 9, 4, 30, 23, 0), "\"1990-11-09T04:30:23Z\""),
        Case.of(
            timestampUTC(1990, Month.NOVEMBER, 9, 4, 30, 23, 123000),
            "\"1990-11-09T04:30:23.123Z\""),
        Case.of(
            timestampUTC(1990, Month.NOVEMBER, 9, 4, 30, 23, 120000),
            "\"1990-11-09T04:30:23.120Z\""),
        Case.of(timestampUTC(1, Month.JANUARY, 1, 0, 0, 0, 0), "\"0001-01-01T00:00:00Z\""));
  }

  @Test
  void testDate() throws IOException {
    checkWriteAll(
        JsonLfEncoders::date,
        Case.of(date(2019, Month.JUNE, 18), "\"2019-06-18\""),
        Case.of(date(9999, Month.DECEMBER, 31), "\"9999-12-31\""),
        Case.of(date(1, Month.JANUARY, 1), "\"0001-01-01\""));
  }

  @Test
  void testParty() throws IOException {
    checkWriteAll(JsonLfEncoders::party, Case.of("Alice", "\"Alice\""));
  }

  @Test
  void testText() throws IOException {
    checkWriteAll(
        JsonLfEncoders::text,
        Case.of("", "\"\""),
        Case.of("\\", "\"\\\\\""),
        Case.of(" ", "\" \""),
        Case.of("hello", "\"hello\""));
  }

  @Test
  void testContractId() throws IOException {
    checkWriteAll(JsonLfEncoders::contractId, Case.of(new Tmpl.Cid("deadbeef"), "\"deadbeef\""));
  }

  @Test
  void testEnum() throws IOException {
    checkWriteAll(
        JsonLfEncoders.enumeration(Suit::toDamlName),
        Case.of(Suit.HEARTS, "\"Hearts\""),
        Case.of(Suit.DIAMONDS, "\"Diamonds\""),
        Case.of(Suit.CLUBS, "\"Clubs\""),
        Case.of(Suit.SPADES, "\"Spades\""));
  }

  @Test
  void testList() throws IOException {
    checkWriteAll(
        JsonLfEncoders.list(JsonLfEncoders::int64),
        Case.of(emptyList(), "[]"),
        Case.of(asList(1L, 2L), "[1, 2]"));
  }

  @Test
  void testListNested() throws IOException {
    checkWriteAll(
        JsonLfEncoders.list(JsonLfEncoders.list(JsonLfEncoders::int64)),
        Case.of(emptyList(), "[]"),
        Case.of(asList(asList(1L), emptyList(), asList(2L, 3L)), "[[1], [], [2, 3]]"));
  }

  <K, V> Map<K, V> orderedMap(K key1, V val1, K key2, V val2) {
    Map<K, V> map = new LinkedHashMap<>();
    map.put(key1, val1);
    map.put(key2, val2);
    return map;
  }

  @Test
  void testTextMap() throws IOException {
    checkWriteAll(
        JsonLfEncoders.textMap(JsonLfEncoders::int64),
        Case.of(emptyMap(), "{}"),
        Case.of(orderedMap("foo", 1L, "bar", 2L), "{\"foo\": 1, \"bar\": 2}"));
  }

  @Test
  void testGenMap() throws IOException {
    checkWriteAll(
        JsonLfEncoders.genMap(JsonLfEncoders::text, JsonLfEncoders::int64),
        Case.of(emptyMap(), "[]"),
        Case.of(orderedMap("foo", 1L, "bar", 2L), "[[\"foo\", 1], [\"bar\", 2]]"));
  }

  @Test
  void testOptionalNonNested() throws IOException {
    checkWriteAll(
        JsonLfEncoders.optional(JsonLfEncoders::int64),
        Case.of(Optional.empty(), "null"),
        Case.of(Optional.of(42L), "42"));
  }

  @Test
  void testOptionalNested() throws IOException {
    checkWriteAll(
        JsonLfEncoders.optionalNested(JsonLfEncoders.optional(JsonLfEncoders::int64)),
        Case.of(Optional.empty(), "null"),
        Case.of(Optional.of(Optional.empty()), "[]"),
        Case.of(Optional.of(Optional.of(42L)), "[42]"));
  }

  @Test
  void testOptionalNestedDeeper() throws IOException {
    checkWriteAll(
        JsonLfEncoders.optionalNested(
            JsonLfEncoders.optionalNested(
                JsonLfEncoders.optionalNested(JsonLfEncoders.optional(JsonLfEncoders::int64)))),
        Case.of(Optional.empty(), "null"),
        Case.of(Optional.of(Optional.empty()), "[]"),
        Case.of(Optional.of(Optional.of(Optional.empty())), "[[]]"),
        Case.of(Optional.of(Optional.of(Optional.of(Optional.empty()))), "[[[]]]"),
        Case.of(Optional.of(Optional.of(Optional.of(Optional.of(42L)))), "[[[42]]]"));
  }

  @Test
  void testOptionalLists() throws IOException {
    checkWriteAll(
        JsonLfEncoders.optional(JsonLfEncoders.list(JsonLfEncoders.list(JsonLfEncoders::int64))),
        Case.of(Optional.empty(), "null"),
        Case.of(Optional.of(emptyList()), "[]"),
        Case.of(Optional.of(asList(emptyList())), "[[]]"),
        Case.of(Optional.of(asList(asList(42L))), "[[42]]"));
  }

  @Test
  void testVariant() throws IOException {
    checkWriteAll(
        JsonLfEncoders.variant(
            v -> {
              if (v instanceof SomeVariant.Bar)
                return JsonLfEncoders.Field.of(
                    "Bar", JsonLfEncoders.int64(((SomeVariant.Bar) v).x));
              else if (v instanceof SomeVariant.Baz)
                return JsonLfEncoders.Field.of("Baz", JsonLfEncoders.unit);
              else if (v instanceof SomeVariant.Quux)
                return JsonLfEncoders.Field.of(
                    "Quux",
                    JsonLfEncoders.optional(JsonLfEncoders::int64).apply(((SomeVariant.Quux) v).x));
              else if (v instanceof SomeVariant.Flarp)
                return JsonLfEncoders.Field.of(
                    "Flarp",
                    JsonLfEncoders.list(JsonLfEncoders::int64).apply(((SomeVariant.Flarp) v).x));
              else return null;
            }),
        Case.of(new SomeVariant.Bar(42L), "{\"tag\": \"Bar\", \"value\": 42}"),
        Case.of(new SomeVariant.Baz(Unit.getInstance()), "{\"tag\": \"Baz\", \"value\": {}}"),
        Case.of(new SomeVariant.Quux(Optional.empty()), "{\"tag\": \"Quux\", \"value\": null}"),
        Case.of(new SomeVariant.Quux(Optional.of(42L)), "{\"tag\": \"Quux\", \"value\": 42}"),
        Case.of(new SomeVariant.Flarp(asList(42L)), "{\"tag\": \"Flarp\", \"value\": [42]}"));
  }

  @Test
  void testRecord() throws IOException {
    checkWriteAll(
        r ->
            JsonLfEncoders.record(
                JsonLfEncoders.Field.of("i", JsonLfEncoders.list(JsonLfEncoders::int64).apply(r.i)),
                JsonLfEncoders.Field.of("b", JsonLfEncoders.bool(r.b))),
        Case.of(new SomeRecord(asList(), true), "{\"i\": [], \"b\": true}"),
        Case.of(new SomeRecord(asList(1L, 2L), true), "{\"i\": [1, 2], \"b\": true}"));
  }

  private BigDecimal dec(String s) {
    return new BigDecimal(s);
  }

  private Instant timestampUTC(
      int year, Month month, int day, int hour, int minute, int second, int micros) {
    return LocalDateTime.of(year, month, day, hour, minute, second, micros * 1000)
        .toInstant(ZoneOffset.UTC);
  }

  private Instant timestampUTC(
      int year, Month month, int day, int hour, int minute, int second, int micros, int nanos) {
    return LocalDateTime.of(year, month, day, hour, minute, second, micros * 1000 + nanos)
        .toInstant(ZoneOffset.UTC);
  }

  private LocalDate date(int year, Month month, int day) {
    return LocalDate.of(year, month, day);
  }

  class SomeRecord {
    private final List<Long> i;
    private final Boolean b;

    public SomeRecord(List<Long> i, Boolean b) {
      this.i = i;
      this.b = b;
    }

    public String toString() {
      return String.format("SomeRecord{i=%s,b=%s}", i, b);
    }

    @Override
    public boolean equals(Object o) {
      return o != null
          && (o instanceof SomeRecord)
          && ((SomeRecord) o).i.equals(i)
          && (((SomeRecord) o).b.equals(b));
    }

    @Override
    public int hashCode() {
      return Objects.hash(i, b);
    }
  }

  abstract static class SomeVariant {
    static class Bar extends SomeVariant {
      private final Long x;

      public Bar(Long x) {
        this.x = x;
      }

      @Override
      public boolean equals(Object o) {
        return o != null && (o instanceof Bar) && x == ((Bar) o).x;
      }

      @Override
      public int hashCode() {
        return Objects.hash(x);
      }

      @Override
      public String toString() {
        return String.format("Bar(%s)", x);
      }
    }

    static class Baz extends SomeVariant {
      private final Unit x;

      public Baz(Unit x) {
        this.x = x;
      }
      // All units are the same, and thus so are all Baz's
      @Override
      public boolean equals(Object o) {
        return o != null && (o instanceof Baz);
      }

      @Override
      public int hashCode() {
        return 1;
      }

      @Override
      public String toString() {
        return "Baz()";
      }
    }

    static class Quux extends SomeVariant {
      private final Optional<Long> x;

      public Quux(Optional<Long> x) {
        this.x = x;
      }

      @Override
      public boolean equals(Object o) {
        return o != null && (o instanceof Quux) && x.equals(((Quux) o).x);
      }

      @Override
      public int hashCode() {
        return Objects.hash(x);
      }

      @Override
      public String toString() {
        return String.format("Quux(%s)", x);
      }
    }

    static class Flarp extends SomeVariant {
      private final List<Long> x;

      public Flarp(List<Long> x) {
        this.x = x;
      }

      @Override
      public boolean equals(Object o) {
        return o != null && (o instanceof Flarp) && x.equals(((Flarp) o).x);
      }

      @Override
      public int hashCode() {
        return Objects.hash(x);
      }

      @Override
      public String toString() {
        return String.format("Flarp(%s)", x);
      }
    }
  }

  static class Tmpl {
    public static final class Cid extends com.daml.ledger.javaapi.data.codegen.ContractId<Tmpl> {
      public Cid(String id) {
        super(id);
      }
    }
  }

  enum Suit {
    HEARTS,
    DIAMONDS,
    CLUBS,
    SPADES;

    String toDamlName() {
      switch (this) {
        case HEARTS:
          return "Hearts";
        case DIAMONDS:
          return "Diamonds";
        case CLUBS:
          return "Clubs";
        case SPADES:
          return "Spades";
        default:
          return null;
      }
    }

    static final Map<String, Suit> damlNames =
        new HashMap<>() {
          {
            put("Hearts", HEARTS);
            put("Diamonds", DIAMONDS);
            put("Clubs", CLUBS);
            put("Spades", SPADES);
          }
        };
  }

  String intoString(JsonLfEncoder encoder) throws IOException {
    StringWriter writer = new StringWriter();
    encoder.encode(new JsonLfWriter(writer));
    return writer.toString();
  }

  static class Case<T> {
    public final T input;
    public final String output;

    private Case(T input, String output) {
      this.input = input;
      this.output = output;
    }

    static <T> Case<T> of(T input, String output) {
      return new Case(input, output);
    }
  }

  <T> void checkWriteAll(java.util.function.Function<T, JsonLfEncoder> encoderFor, Case<T>... cases)
      throws IOException {
    for (Case<T> c : cases) {
      JsonLfEncoder encoder = encoderFor.apply(c.input);
      assertEquals(c.output, intoString(encoder));
    }
  }
}
