// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.javaapi.data.codegen.json;

import static java.util.Arrays.asList;
import static java.util.Collections.emptyList;
import static java.util.Collections.emptyMap;
import static org.junit.jupiter.api.Assertions.assertEquals;

import com.daml.ledger.javaapi.data.Unit;
import com.daml.ledger.javaapi.data.codegen.json.JsonLfReader.Decoders;
import java.io.IOException;
import java.math.BigDecimal;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.Month;
import java.time.ZoneOffset;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.function.Consumer;
import org.junit.jupiter.api.Test;
import org.junit.platform.runner.JUnitPlatform;
import org.junit.runner.RunWith;

@RunWith(JUnitPlatform.class)
public class JsonLfReaderTest {

  @Test
  void testUnit() throws IOException {
    checkReadAll(Decoders.unit, eq("{}", Unit.getInstance()), eq("\t{\n} ", Unit.getInstance()));
  }

  @Test
  void testBool() throws IOException {
    checkReadAll(Decoders.bool, eq("false", false), eq("true", true));
  }

  @Test
  void testInt64() throws IOException {
    checkReadAll(
        Decoders.int64,
        eq("42", 42L),
        eq("\"+42\"", 42L),
        eq("-42", -42L),
        eq("0", 0L),
        eq("-0", -0L),
        eq("9223372036854775807", 9223372036854775807L),
        eq("\"9223372036854775807\"", 9223372036854775807L),
        eq("-9223372036854775808", -9223372036854775808L),
        eq("\"-9223372036854775808\"", -9223372036854775808L));
  }

  @Test
  void testDecimal() throws IOException {
    checkReadAll(
        Decoders.decimal,
        cmpEq("42", dec("42")),
        cmpEq("42.0", dec("42")),
        cmpEq("\"42\"", dec("42")),
        cmpEq("-42", dec("-42")),
        cmpEq("\"-42\"", dec("-42")),
        cmpEq("0", dec("0")),
        cmpEq("-0", dec("-0")),
        //            cmpEq("0.30000000000000004", dec("0.3")), // TODO(raphael-speyer-da):
        // Appropriate rounding
        cmpEq("2e3", dec("2000")),
        cmpEq(
            "9999999999999999999999999999.9999999999",
            dec("9999999999999999999999999999.9999999999")));
  }

  @Test
  void testTimestamp() throws IOException {
    checkReadAll(
        Decoders.timestamp,
        eq(
            "\"1990-11-09T04:30:23.123456Z\"",
            timestampUTC(1990, Month.NOVEMBER, 9, 4, 30, 23, 123456)),
        eq(
            "\"9999-12-31T23:59:59.999999Z\"",
            timestampUTC(9999, Month.DECEMBER, 31, 23, 59, 59, 999999)),
        //            eq("\"1990-11-09T04:30:23.1234569Z\"", timestampUTC(1990, Month.NOVEMBER,
        // 9,  4, 30, 23, 123457)), // TODO(raphael-speyer-da): Appropriate rounding
        eq("\"1990-11-09T04:30:23Z\"", timestampUTC(1990, Month.NOVEMBER, 9, 4, 30, 23, 0)),
        eq(
            "\"1990-11-09T04:30:23.123Z\"",
            timestampUTC(1990, Month.NOVEMBER, 9, 4, 30, 23, 123000)),
        eq("\"0001-01-01T00:00:00Z\"", timestampUTC(1, Month.JANUARY, 1, 0, 0, 0, 0)));
  }

  @Test
  void testDate() throws IOException {
    checkReadAll(
        Decoders.date,
        eq("\"2019-06-18\"", date(2019, Month.JUNE, 18)),
        eq("\"9999-12-31\"", date(9999, Month.DECEMBER, 31)),
        eq("\"0001-01-01\"", date(1, Month.JANUARY, 1)));
  }

  @Test
  void testParty() throws IOException {
    checkReadAll(Decoders.party, eq("\"Alice\"", "Alice"));
  }

  @Test
  void testText() throws IOException {
    checkReadAll(Decoders.text, eq("\"\"", ""), eq("\" \"", " "), eq("\"hello\"", "hello"));
  }

  @Test
  void testContractId() throws IOException {
    checkReadAll(Decoders.contractId(Tmpl.Cid::new), eq("\"deadbeef\"", new Tmpl.Cid("deadbeef")));
  }

  @Test
  void testEnum() throws IOException {
    checkReadAll(
        Decoders.enumeration(Suit.class),
        eq("\"Hearts\"", Suit.Hearts),
        eq("\"Diamonds\"", Suit.Diamonds),
        eq("\"Clubs\"", Suit.Clubs),
        eq("\"Spades\"", Suit.Spades));
  }

  @Test
  void testList() throws IOException {
    checkReadAll(Decoders.list(Decoders.int64), eq("[]", emptyList()), eq("[1,2]", asList(1L, 2L)));
  }

  @Test
  void testTextMap() throws IOException {
    checkReadAll(
        Decoders.textMap(Decoders.int64),
        eq("{}", emptyMap()),
        eq("{\"foo\":1, \"bar\": 2}", java.util.Map.of("foo", 1L, "bar", 2L)));
  }

  @Test
  void testGenMap() throws IOException {
    checkReadAll(
        Decoders.genMap(Decoders.text, Decoders.int64),
        eq("[]", emptyMap()),
        eq("[[\"foo\", 1], [\"bar\", 2]]", java.util.Map.of("foo", 1L, "bar", 2L)));
  }

  @Test
  void testOptionalNonNested() throws IOException {
    checkReadAll(
        Decoders.optional(Decoders.int64),
        eq("null", Optional.empty()),
        eq("42", Optional.of(42L)));
  }

  @Test
  void testOptionalNested() throws IOException {
    checkReadAll(
        Decoders.optionalNested(Decoders.optional(Decoders.int64)),
        eq("null", Optional.empty()),
        eq("[]", Optional.of(Optional.empty())),
        eq("[42]", Optional.of(Optional.of(42L))));
  }

  @Test
  void testOptionalNestedDeeper() throws IOException {
    checkReadAll(
        Decoders.optionalNested(Decoders.optionalNested(Decoders.optional(Decoders.int64))),
        eq("null", Optional.empty()),
        eq("[]", Optional.of(Optional.empty())),
        eq("[[]]", Optional.of(Optional.of(Optional.empty()))),
        eq("[[42]]", Optional.of(Optional.of(Optional.of(42L)))));
  }

  @Test
  void testOptionalLists() throws IOException {
    checkReadAll(
        Decoders.optional(Decoders.list(Decoders.list(Decoders.int64))),
        eq("null", Optional.empty()),
        eq("[]", Optional.of(emptyList())),
        eq("[[]]", Optional.of(asList(emptyList()))),
        eq("[[42]]", Optional.of(asList(asList(42L)))));
  }

  @Test
  void testVariant() throws IOException, JsonLfDecoder.Error {
    checkReadAll(
        Decoders.variant(
            asList("Bar", "Baz", "Quux", "Flarp"),
            tagName -> {
              switch (tagName) {
                case "Bar":
                  return r -> new SomeVariant.Bar(Decoders.int64.decode(r));
                case "Baz":
                  return r -> new SomeVariant.Baz(Decoders.unit.decode(r));
                case "Quux":
                  return r -> new SomeVariant.Quux(Decoders.optional(Decoders.int64).decode(r));
                case "Flarp":
                  return r -> new SomeVariant.Flarp(Decoders.list(Decoders.int64).decode(r));
                default:
                  return null;
              }
            }),
        eq("{\"tag\": \"Bar\", \"value\": 42}", new SomeVariant.Bar(42L)),
        eq("{\"tag\": \"Baz\", \"value\": {}}", new SomeVariant.Baz(Unit.getInstance())),
        eq("{\"tag\": \"Quux\", \"value\": null}", new SomeVariant.Quux(Optional.empty())),
        eq("{\"tag\": \"Quux\", \"value\": 42}", new SomeVariant.Quux(Optional.of(42L))),
        eq("{\"value\": 42, \"tag\": \"Quux\"}", new SomeVariant.Quux(Optional.of(42L))),
        eq("{\"value\": [42], \"tag\": \"Flarp\"}", new SomeVariant.Flarp(asList(42L))));
  }

  @Test
  void testRecord() throws IOException {
    checkReadAll(
        Decoders.record(
            asList("i", "b"),
            name -> {
              switch (name) {
                case "i":
                  return Decoders.Field.at(0, Decoders.list(Decoders.int64));
                case "b":
                  return Decoders.Field.at(1, Decoders.bool, false);
                default:
                  return null;
              }
            },
            args -> new SomeRecord((List<Long>) args[0], (Boolean) args[1])),
        eq("[[1],true]", new SomeRecord(asList(1L), true)),
        eq("{\"i\":[],\"b\":true}", new SomeRecord(asList(), true)),
        eq("{\"i\":[1,2],\"b\":true}", new SomeRecord(asList(1L, 2L), true)),
        eq("{\"b\":true,\"i\":[1]}", new SomeRecord(asList(1L), true)),
        eq("{\"i\":[1]}", new SomeRecord(asList(1L), false)));
  }

  private BigDecimal dec(String s) {
    return new BigDecimal(s);
  }

  private Instant timestampUTC(
      int year, Month month, int day, int hour, int minute, int second, int micros) {
    return LocalDateTime.of(year, month, day, hour, minute, second, micros * 1000)
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
    Hearts,
    Diamonds,
    Clubs,
    Spades
  }

  private <T> void checkReadAll(JsonLfDecoder<T> decoder, TestCase<T>... testCases)
      throws IOException {
    for (var tc : testCases) {
      JsonLfReader r = new JsonLfReader(tc.input);
      T actual = decoder.decode(r);
      tc.check.accept(actual);
    }
  }

  static class TestCase<T> {
    public String input;
    public final Consumer<T> check;

    public TestCase(String input, Consumer<T> check) {
      this.input = input;
      this.check = check;
    }
  }

  private <T> TestCase<T> eq(String input, T expected) {
    return new TestCase(
        input,
        actual -> {
          assertEquals(
              expected,
              actual,
              String.format("input=%s, expected=%s, actual=%s", input, expected, actual));
        });
  }

  private <T extends Comparable> TestCase<T> cmpEq(String input, T expected) {
    return new TestCase(
        input,
        actual -> {
          assertEquals(
              0,
              expected.compareTo(actual),
              String.format(
                  "unequal by ordering comparison, input=%s, expected=%s, actual=%s",
                  input, expected, actual));
        });
  }
}
