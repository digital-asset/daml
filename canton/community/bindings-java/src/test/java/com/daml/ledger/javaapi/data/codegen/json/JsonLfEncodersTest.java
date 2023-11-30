// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates.
// Proprietary code. All rights reserved.

package com.daml.ledger.javaapi.data.codegen.json;

import static com.daml.ledger.javaapi.data.codegen.json.JsonLfWriter.opts;
import static com.daml.ledger.javaapi.data.codegen.json.TestHelpers.*;
import static java.util.Arrays.asList;
import static java.util.Collections.emptyList;
import static java.util.Collections.emptyMap;
import static org.junit.jupiter.api.Assertions.assertEquals;

import com.daml.ledger.javaapi.data.Unit;
import java.io.IOException;
import java.io.StringWriter;
import java.math.BigDecimal;
import java.time.Month;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Optional;
import org.junit.jupiter.api.Test;
import org.junit.platform.runner.JUnitPlatform;
import org.junit.runner.RunWith;

@RunWith(JUnitPlatform.class)
public class JsonLfEncodersTest {

  @Test
  void testUnit() throws IOException {
    assertEquals("{}", intoString(JsonLfEncoders.unit(Unit.getInstance())));
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
  void testInt64AsString() throws IOException {
    StringWriter sw = new StringWriter();
    JsonLfEncoders.int64(42L).encode(new JsonLfWriter(sw, opts().encodeInt64AsString(true)));
    assertEquals("\"42\"", sw.toString());
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
  void testNumericAsString() throws IOException {
    StringWriter sw = new StringWriter();
    JsonLfEncoders.numeric(new BigDecimal(0.5))
        .encode(new JsonLfWriter(sw, opts().encodeNumericAsString(true)));
    assertEquals("\"0.5\"", sw.toString());
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
                return JsonLfEncoders.Field.of("Baz", JsonLfEncoders.unit(((SomeVariant.Baz) v).x));
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

  String intoString(JsonLfEncoder encoder) throws IOException {
    StringWriter writer = new StringWriter();
    JsonLfWriter.Options opts =
        JsonLfWriter.opts().encodeNumericAsString(false).encodeInt64AsString(false);
    encoder.encode(new JsonLfWriter(writer, opts));
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
