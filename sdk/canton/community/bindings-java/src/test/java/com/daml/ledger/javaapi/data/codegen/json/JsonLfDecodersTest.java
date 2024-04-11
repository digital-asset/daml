// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates.
// Proprietary code. All rights reserved.

package com.daml.ledger.javaapi.data.codegen.json;

import static com.daml.ledger.javaapi.data.codegen.json.TestHelpers.*;
import static java.util.Arrays.asList;
import static java.util.Collections.emptyList;
import static java.util.Collections.emptyMap;
import static org.junit.jupiter.api.Assertions.assertEquals;

import com.daml.ledger.javaapi.data.Unit;
import java.io.IOException;
import java.time.Month;
import java.time.format.DateTimeParseException;
import java.util.List;
import java.util.Optional;
import java.util.regex.Pattern;

import org.junit.Test;

public class JsonLfDecodersTest {

  @Test
  public void testUnit() throws JsonLfDecoder.Error {
    checkReadAll(
        JsonLfDecoders.unit, eq("{}", Unit.getInstance()), eq("\t{\n} ", Unit.getInstance()));
  }

  @Test
  public void testUnitErrors() throws JsonLfDecoder.Error {
    checkReadAll(
        JsonLfDecoders.unit,
        errors("", "Expected { but was nothing at line: 1, column: 0"),
        errors("{", "JSON parse error at line: 1, column: 2", IOException.class),
        errors("}", "JSON parse error at line: 1, column: 1", IOException.class),
        errors("null", "Expected { but was null at line: 1, column: 1"));
  }

  @Test
  public void testBool() throws JsonLfDecoder.Error {
    checkReadAll(JsonLfDecoders.bool, eq("false", false), eq("true", true));
  }

  @Test
  public void testBoolErrors() throws JsonLfDecoder.Error {
    checkReadAll(
        JsonLfDecoders.bool,
        errors("1", "Expected boolean but was 1 at line: 1, column: 1"),
        errors("\"true\"", "Expected boolean but was true at line: 1, column: 1"),
        errors("True", "JSON parse error at line: 1, column: 1", IOException.class));
  }

  @Test
  public void testInt64() throws JsonLfDecoder.Error {
    checkReadAll(
        JsonLfDecoders.int64,
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
  public void testInt64Errors() throws JsonLfDecoder.Error {
    checkReadAll(
        JsonLfDecoders.int64,
        errors("42.3", "Expected int64 but was 42.3 at line: 1, column: 1"),
        errors("+42", "JSON parse error at line: 1, column: 1", IOException.class),
        errors(
            "9223372036854775808",
            "Expected int64 but was 9223372036854775808 at line: 1, column: 1"),
        errors(
            "-9223372036854775809",
            "Expected int64 but was -9223372036854775809 at line: 1, column: 1"),
        errors("\"garbage\"", "Expected int64 but was garbage at line: 1, column: 1"),
        errors("\"   42 \"", "Expected int64 but was    42  at line: 1, column: 1"));
  }

  @Test
  public void testNumeric() throws JsonLfDecoder.Error {
    checkReadAll(
        JsonLfDecoders.numeric(10),
        cmpEq("42", dec("42")),
        cmpEq("42.0", dec("42")),
        cmpEq("\"42\"", dec("42")),
        cmpEq("-42", dec("-42")),
        cmpEq("\"-42\"", dec("-42")),
        cmpEq("0", dec("0")),
        cmpEq("-0", dec("-0")),
        cmpEq("0.30000000000000004", dec("0.3")),
        cmpEq("2e3", dec("2000")),
        cmpEq(
            "-9999999999999999999999999999.9999999999",
            dec("-9999999999999999999999999999.9999999999")),
        cmpEq(
            "9999999999999999999999999999.9999999999",
            dec("9999999999999999999999999999.9999999999")));
  }

  @Test
  public void testNumericErrors() throws JsonLfDecoder.Error {
    checkReadAll(
        JsonLfDecoders.numeric(10),
        errors(
            "\"  42  \"",
            "Expected numeric but was   42   at line: 1, column: 1",
            NumberFormatException.class),
        errors(
            "\"blah\"",
            "Expected numeric but was blah at line: 1, column: 1",
            NumberFormatException.class),
        errors(
            "10000000000000000000000000000",
            "Expected numeric in range (-10^28, 10^28) but was 10000000000000000000000000000 at"
                + " line: 1, column: 1"),
        errors(
            "-10000000000000000000000000000",
            "Expected numeric in range (-10^28, 10^28) but was -10000000000000000000000000000 at"
                + " line: 1, column: 1"),
        errors(
            "99999999999999999999999999990",
            "Expected numeric in range (-10^28, 10^28) but was 99999999999999999999999999990 at"
                + " line: 1, column: 1"));
  }

  @Test
  public void testTimestamp() throws JsonLfDecoder.Error {
    checkReadAll(
        JsonLfDecoders.timestamp,
        eq(
            "\"1990-11-09T04:30:23.123456Z\"",
            timestampUTC(1990, Month.NOVEMBER, 9, 4, 30, 23, 123456)),
        eq(
            "\"9999-12-31T23:59:59.999999Z\"",
            timestampUTC(9999, Month.DECEMBER, 31, 23, 59, 59, 999999)),
        eq(
            "\"1990-11-09T04:30:23.1234569Z\"",
            timestampUTC(1990, Month.NOVEMBER, 9, 4, 30, 23, 123456)),
        eq("\"1990-11-09T04:30:23Z\"", timestampUTC(1990, Month.NOVEMBER, 9, 4, 30, 23, 0)),
        eq(
            "\"1990-11-09T04:30:23.123Z\"",
            timestampUTC(1990, Month.NOVEMBER, 9, 4, 30, 23, 123000)),
        eq("\"1990-11-09T04:30:23.12Z\"", timestampUTC(1990, Month.NOVEMBER, 9, 4, 30, 23, 120000)),
        eq("\"1990-11-09T04:30:23.1Z\"", timestampUTC(1990, Month.NOVEMBER, 9, 4, 30, 23, 100000)),
        eq("\"1990-11-09T04:30:23Z\"", timestampUTC(1990, Month.NOVEMBER, 9, 4, 30, 23, 0)),
        eq("\"0001-01-01T00:00:00Z\"", timestampUTC(1, Month.JANUARY, 1, 0, 0, 0, 0)));
  }

  @Test
  public void testTimestampErrors() throws JsonLfDecoder.Error {
    checkReadAll(
        JsonLfDecoders.timestamp,
        // 24th hour
        errors(
            "\"1990-11-09T24:30:23.123Z\"",
            "Expected valid ISO 8601 date and time in UTC but was 1990-11-09T24:30:23.123Z at"
                + " line: 1, column: 1",
            DateTimeParseException.class),
        // No Z
        errors(
            "\"1990-11-09T04:30:23.123\"",
            "Expected valid ISO 8601 date and time in UTC but was 1990-11-09T04:30:23.123 at line:"
                + " 1, column: 1",
            DateTimeParseException.class),
        // No time
        errors(
            "\"1990-11-09\"",
            "Expected valid ISO 8601 date and time in UTC but was 1990-11-09 at line: 1, column: 1",
            DateTimeParseException.class),
        // Time zone
        errors(
            "\"1990-11-09T04:30:23.123-07:00\"",
            "Expected valid ISO 8601 date and time in UTC but was 1990-11-09T04:30:23.123-07:00 at"
                + " line: 1, column: 1",
            DateTimeParseException.class),
        // No -
        errors(
            "\"19901109T043023Z\"",
            "Expected valid ISO 8601 date and time in UTC but was 19901109T043023Z at line: 1,"
                + " column: 1",
            DateTimeParseException.class));
  }

  @Test
  public void testDate() throws JsonLfDecoder.Error {
    checkReadAll(
        JsonLfDecoders.date,
        eq("\"2019-06-18\"", date(2019, Month.JUNE, 18)),
        eq("\"9999-12-31\"", date(9999, Month.DECEMBER, 31)),
        eq("\"0001-01-01\"", date(1, Month.JANUARY, 1)));
  }

  @Test
  public void testDateErrors() throws JsonLfDecoder.Error {
    checkReadAll(
        JsonLfDecoders.date,
        errors(
            "\"18/06/2019\"",
            "Expected valid ISO 8601 date but was 18/06/2019 at line: 1, column: 1",
            DateTimeParseException.class),
        errors(
            "\"06/18/2019\"",
            "Expected valid ISO 8601 date but was 06/18/2019 at line: 1, column: 1",
            DateTimeParseException.class),
        errors(
            "\"06-18-2019\"",
            "Expected valid ISO 8601 date but was 06-18-2019 at line: 1, column: 1",
            DateTimeParseException.class),
        errors(
            "\"06-18-19\"",
            "Expected valid ISO 8601 date but was 06-18-19 at line: 1, column: 1",
            DateTimeParseException.class),
        errors(
            "\"20190618\"",
            "Expected valid ISO 8601 date but was 20190618 at line: 1, column: 1",
            DateTimeParseException.class),
        // Thirty days hath September
        errors(
            "\"2019-09-31\"",
            "Expected valid ISO 8601 date but was 2019-09-31 at line: 1, column: 1",
            DateTimeParseException.class));
  }

  @Test
  public void testParty() throws JsonLfDecoder.Error {
    checkReadAll(JsonLfDecoders.party, eq("\"Alice\"", "Alice"));
  }

  @Test
  public void testPartyErrors() throws JsonLfDecoder.Error {
    checkReadAll(
        JsonLfDecoders.party,
        errors("{\"unwrap\": \"foo\"}", "Expected text but was { at line: 1, column: 1"),
        errors("42", "Expected text but was 42 at line: 1, column: 1"));
  }

  @Test
  public void testText() throws JsonLfDecoder.Error {
    checkReadAll(JsonLfDecoders.text, eq("\"\"", ""), eq("\" \"", " "), eq("\"hello\"", "hello"));
  }

  @Test
  public void testTextErrors() throws JsonLfDecoder.Error {
    checkReadAll(
        JsonLfDecoders.text,
        errors("{\"unwrap\": \"foo\"}", "Expected text but was { at line: 1, column: 1"),
        errors("42", "Expected text but was 42 at line: 1, column: 1"));
  }

  @Test
  public void testContractId() throws JsonLfDecoder.Error {
    checkReadAll(
        JsonLfDecoders.contractId(Tmpl.Cid::new), eq("\"deadbeef\"", new Tmpl.Cid("deadbeef")));
  }

  @Test
  public void testContractIdErrors() throws JsonLfDecoder.Error {
    checkReadAll(
        JsonLfDecoders.contractId(Tmpl.Cid::new),
        errors("42", "Expected text but was 42 at line: 1, column: 1"));
  }

  @Test
  public void testEnum() throws JsonLfDecoder.Error {
    checkReadAll(
        JsonLfDecoders.enumeration(Suit.damlNames),
        eq("\"Hearts\"", Suit.HEARTS),
        eq("\"Diamonds\"", Suit.DIAMONDS),
        eq("\"Clubs\"", Suit.CLUBS),
        eq("\"Spades\"", Suit.SPADES));
  }

  @Test
  public void testEnumErrors() throws JsonLfDecoder.Error {
    checkReadAll(
        JsonLfDecoders.enumeration(Suit.damlNames),
        errors("Hearts", "JSON parse error at line: 1, column: 1", IOException.class),
        errors(
            "\"HEARTS\"",
            "Expected one of [Spades, Hearts, Diamonds, Clubs] but was HEARTS at line: 1,"
                + " column: 1"),
        errors(
            "\"Joker\"",
            "Expected one of [Spades, Hearts, Diamonds, Clubs] but was Joker at line: 1,"
                + " column: 1"));
  }

  @Test
  public void testList() throws JsonLfDecoder.Error {
    checkReadAll(
        JsonLfDecoders.list(JsonLfDecoders.int64),
        eq("[]", emptyList()),
        eq("[1,2]", asList(1L, 2L)),
        eq("[1,2,\"3\"]", asList(1L, 2L, 3L)));
  }

  @Test
  public void testListNested() throws JsonLfDecoder.Error {
    checkReadAll(
        JsonLfDecoders.list(JsonLfDecoders.list(JsonLfDecoders.int64)),
        eq("[]", emptyList()),
        eq("[[1], [], [2, 3]]", asList(asList(1L), emptyList(), asList(2L, 3L))));
  }

  @Test
  public void testListErrors() throws JsonLfDecoder.Error {
    checkReadAll(
        JsonLfDecoders.list(JsonLfDecoders.int64),
        errors("[", "JSON parse error at line: 1, column: 2", IOException.class),
        errors("[ 1", "JSON parse error at line: 1, column: 4", IOException.class),
        errors("[ 1, ", "JSON parse error at line: 1, column: 6", IOException.class),
        errors("[1, 2, ]", "JSON parse error at line: 1, column: 9", IOException.class),
        errors("[1, false ]", "Expected int64 but was false at line: 1, column: 5"));
  }

  @Test
  public void testTextMap() throws JsonLfDecoder.Error {
    checkReadAll(
        JsonLfDecoders.textMap(JsonLfDecoders.int64),
        eq("{}", emptyMap()),
        eq("{\"foo\":1, \"bar\": 2}", java.util.Map.of("foo", 1L, "bar", 2L)));
  }

  @Test
  public void testTextMapErrors() throws JsonLfDecoder.Error {
    checkReadAll(
        JsonLfDecoders.textMap(JsonLfDecoders.int64),
        errors("{1: true}", "JSON parse error at line: 1, column: 3", IOException.class),
        errors("{x: true}", "JSON parse error at line: 1, column: 3", IOException.class),
        errors("{\"x\": true}", "Expected int64 but was true at line: 1, column: 7"),
        errors("[[\"x\", true]]", "Expected { but was [ at line: 1, column: 1"));
  }

  @Test
  public void testGenMap() throws JsonLfDecoder.Error {
    checkReadAll(
        JsonLfDecoders.genMap(JsonLfDecoders.text, JsonLfDecoders.int64),
        eq("[]", emptyMap()),
        eq("[[\"foo\", 1], [\"bar\", 2]]", java.util.Map.of("foo", 1L, "bar", 2L)));
  }

  @Test
  public void testGenMapErrors() throws JsonLfDecoder.Error {
    checkReadAll(
        JsonLfDecoders.genMap(JsonLfDecoders.text, JsonLfDecoders.int64),
        errors("{}", "Expected [ but was { at line: 1, column: 1"),
        errors("{\"x\": 1}", "Expected [ but was { at line: 1, column: 1"),
        errors("[\"x\", 1]", "Expected [ but was x at line: 1, column: 2"),
        errors("[[\"x\"], [1]]", "Expected int64 but was ] at line: 1, column: 6"),
        errors("[[\"x\", 1, true]]", "Expected ] but was true at line: 1, column: 11"),
        errors("[[1, \"x\"]]", "Expected text but was 1 at line: 1, column: 3"));
  }

  @Test
  public void testOptionalNonNested() throws JsonLfDecoder.Error {
    checkReadAll(
        JsonLfDecoders.optional(JsonLfDecoders.int64),
        eq("null", Optional.empty()),
        eq("42", Optional.of(42L)));
  }

  @Test
  public void testOptionalNonNestedErrors() throws JsonLfDecoder.Error {
    checkReadAll(
        JsonLfDecoders.optional(JsonLfDecoders.int64),
        errors("undefined", "JSON parse error at line: 1, column: 1", IOException.class),
        errors("", "Expected int64 but was nothing at line: 1, column: 0"),
        errors("\"None\"", "Expected int64 but was None at line: 1, column: 1"),
        errors("[]", "Expected int64 but was [ at line: 1, column: 1"));
  }

  @Test
  public void testOptionalNested() throws JsonLfDecoder.Error {
    checkReadAll(
        JsonLfDecoders.optionalNested(JsonLfDecoders.optional(JsonLfDecoders.int64)),
        eq("null", Optional.empty()),
        eq("[]", Optional.of(Optional.empty())),
        eq("[42]", Optional.of(Optional.of(42L))));
  }

  @Test
  public void testOptionalNestedErrors() throws JsonLfDecoder.Error {
    checkReadAll(
        JsonLfDecoders.optionalNested(JsonLfDecoders.optional(JsonLfDecoders.int64)),
        errors("[null]", "Expected ] or item but was null at line: 1, column: 2"),
        errors("[false]", "Expected int64 but was false at line: 1, column: 2"),
        errors("[[]]", "Expected int64 but was [ at line: 1, column: 2"));
  }

  @Test
  public void testOptionalNestedDeeper() throws JsonLfDecoder.Error {
    checkReadAll(
        JsonLfDecoders.optionalNested(
            JsonLfDecoders.optionalNested(
                JsonLfDecoders.optionalNested(JsonLfDecoders.optional(JsonLfDecoders.int64)))),
        eq("null", Optional.empty()),
        eq("[]", Optional.of(Optional.empty())),
        eq("[[]]", Optional.of(Optional.of(Optional.empty()))),
        eq("[[[]]]", Optional.of(Optional.of(Optional.of(Optional.empty())))),
        eq("[[[42]]]", Optional.of(Optional.of(Optional.of(Optional.of(42L))))));
  }

  @Test
  public void testOptionalLists() throws JsonLfDecoder.Error {
    checkReadAll(
        JsonLfDecoders.optional(JsonLfDecoders.list(JsonLfDecoders.list(JsonLfDecoders.int64))),
        eq("null", Optional.empty()),
        eq("[]", Optional.of(emptyList())),
        eq("[[]]", Optional.of(asList(emptyList()))),
        eq("[[42]]", Optional.of(asList(asList(42L)))));
  }

  @Test
  public void testVariant() throws JsonLfDecoder.Error {
    checkReadAll(
        JsonLfDecoders.variant(
            asList("Bar", "Baz", "Quux", "Flarp"),
            tagName -> {
              switch (tagName) {
                case "Bar":
                  return r -> new SomeVariant.Bar(JsonLfDecoders.int64.decode(r));
                case "Baz":
                  return r -> new SomeVariant.Baz(JsonLfDecoders.unit.decode(r));
                case "Quux":
                  return r ->
                      new SomeVariant.Quux(JsonLfDecoders.optional(JsonLfDecoders.int64).decode(r));
                case "Flarp":
                  return r ->
                      new SomeVariant.Flarp(JsonLfDecoders.list(JsonLfDecoders.int64).decode(r));
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
  public void testVariantErrors() throws JsonLfDecoder.Error {
    checkReadAll(
        JsonLfDecoders.variant(
            asList("Bar", "Baz", "Quux", "Flarp"),
            tagName -> {
              switch (tagName) {
                case "Bar":
                  return r -> new SomeVariant.Bar(JsonLfDecoders.int64.decode(r));
                case "Baz":
                  return r -> new SomeVariant.Baz(JsonLfDecoders.unit.decode(r));
                case "Quux":
                  return r ->
                      new SomeVariant.Quux(JsonLfDecoders.optional(JsonLfDecoders.int64).decode(r));
                case "Flarp":
                  return r ->
                      new SomeVariant.Flarp(JsonLfDecoders.list(JsonLfDecoders.int64).decode(r));
                default:
                  return null;
              }
            }),
        errors("{}", "Expected field but was } at line: 1, column: 2"),
        errors("{\"Bar\": 1}", "Expected field tag or value but was Bar at line: 1, column: 2"),
        errors("{\"value\": 1}", "Expected field but was } at line: 1, column: 12"),
        errors(
            "{\"tag\": \"What\"}",
            "Expected one of [Bar, Baz, Quux, Flarp] but was What at line: 1, column: 9"),
        errors(
            "{\"tag\": \"Bar\", \"tag\": \"Baz\"}",
            "Expected field value but was tag at line: 1, column: 16"),
        errors(
            "{\"tag\": \"What\", \"value\": 1}",
            "Expected one of [Bar, Baz, Quux, Flarp] but was What at line: 1, column: 9"),
        errors(
            "{\"value\": 1, \"tag\": \"What\"}",
            "Expected one of [Bar, Baz, Quux, Flarp] but was What at line: 1, column: 21"),
        errors(
            "{\"tag\": \"Bar\", \"tag\": \"What\", \"value\": 1}",
            "Expected field value but was tag at line: 1, column: 16"),
        errors(
            "{\"tag\": \"Bar\", \"value\": false}",
            "Expected int64 but was false at line: 1, column: 25"),
        errors(
            "{\"value\": false, \"tag\": \"Bar\"}",
            "Expected int64 but was false at line: 1, column: 11"),
        errors(
            "{\"x\": 1, \"tag\": \"Bar\", \"y\": 2, \"value\": 1}",
            "Expected field tag or value but was x at line: 1, column: 2"),
        errors(
            "{\"value\": [1,2,false,3], \"tag\": \"Flarp\"}",
            "Expected int64 but was false at line: 1, column: 16"),
        errors(
            "{\"value\": 1, \"tag\": \"Quuz\", \"tag\": \"Bar\"}",
            "Expected one of [Bar, Baz, Quux, Flarp] but was Quuz at line: 1, column: 21"),
        errors(
            "{\"value\": false, \"value\": true, \"tag\": \"Quux\", \"value\": 1, \"tag\":"
                + " \"Bar\"}",
            "Expected field tag but was value at line: 1, column: 18"),
        errors(
            "{\"tag\": \"Bar\", \"value\": 1, \"tag\": \"Baz\"}",
            "Expected } but was tag at line: 1, column: 28"));
  }

  @Test
  public void testRecord() throws JsonLfDecoder.Error {
    checkReadAll(
        JsonLfDecoders.record(
            asList("i", "b"),
            name -> {
              switch (name) {
                case "i":
                  return JsonLfDecoders.JavaArg.at(0, JsonLfDecoders.list(JsonLfDecoders.int64));
                case "b":
                  return JsonLfDecoders.JavaArg.at(1, JsonLfDecoders.bool, false);
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

  @Test
  public void testRecordErrors() throws JsonLfDecoder.Error {
    checkReadAll(
        JsonLfDecoders.record(
            asList("i", "b"),
            name -> {
              switch (name) {
                case "i":
                  return JsonLfDecoders.JavaArg.at(0, JsonLfDecoders.list(JsonLfDecoders.int64));
                case "b":
                  return JsonLfDecoders.JavaArg.at(1, JsonLfDecoders.bool, false);
                default:
                  return null;
              }
            },
            args -> new SomeRecord((List<Long>) args[0], (Boolean) args[1])),
        errors("[1,true]", "Expected [ but was 1 at line: 1, column: 2"),
        errors("[true,[1]]", "Expected [ but was true at line: 1, column: 2"),
        errors("[[1]]", "Expected boolean but was ] at line: 1, column: 5"),
        errors("{\"i\":1,\"b\":true}", "Expected [ but was 1 at line: 1, column: 6"),
        errors("{\"i\":[false],\"b\":true}", "Expected int64 but was false at line: 1, column: 7"),
        errors("{\"b\":false}", "Missing field i at line: 1, column: 11"),
        errors(
            "{\"i\":[1],\"b\":true,\"extra\":2}",
            "Unknown field extra (known fields are [i, b]) at line: 1, column: 19"),
        errors(
            "{\"i\":[1],\"extra\":2,\"b\":true}",
            "Unknown field extra (known fields are [i, b]) at line: 1, column: 10"),
        errors("{\"i\":[1],\"b\":true ", "JSON parse error at line: 1, column: 19"));
  }

  @Test
  public void testUnknownValue() throws JsonLfDecoder.Error {
    JsonLfReader.UnknownValue.read(new JsonLfReader("1")).decodeWith(JsonLfDecoders.int64);
    JsonLfReader.UnknownValue.read(new JsonLfReader("\"88\""))
        .decodeWith(JsonLfDecoders.numeric(2));
    JsonLfReader.UnknownValue.read(new JsonLfReader("[\"hi\", \"there\"]"))
        .decodeWith(JsonLfDecoders.list(JsonLfDecoders.text));

    JsonLfReader.UnknownValue.read(
            new JsonLfReader("[1,2]").moveNext() // Skip [
            )
        .decodeWith(JsonLfDecoders.int64);

    JsonLfReader.UnknownValue.read(
            new JsonLfReader("[1,false , 2]")
                .moveNext() // Skip [
                .moveNext() // Skip 1
            )
        .decodeWith(JsonLfDecoders.bool);

    JsonLfReader.UnknownValue.read(
            new JsonLfReader("{\"a\":{}}")
                .moveNext() // Skip {
                .moveNext() // Skip "a"
            )
        .decodeWith(JsonLfDecoders.unit);

    JsonLfReader.UnknownValue.read(
            new JsonLfReader("[ [\n[ 42]\t] ,[]]").moveNext() // Skip [
            )
        .decodeWith(
            JsonLfDecoders.optionalNested(
                JsonLfDecoders.optionalNested(JsonLfDecoders.optional(JsonLfDecoders.int64))));

    JsonLfReader.UnknownValue.read(
            new JsonLfReader("[ \"hello\", \"world\" ]").moveNext() // Skip [
            )
        .decodeWith(JsonLfDecoders.text);
  }

  @Test
  public void testUnknownValueErrors() throws JsonLfDecoder.Error {
    unknownValueDecodeErrors(
        "42", r -> r, JsonLfDecoders.bool, "Expected boolean but was 42 at line: 1, column: 1");
    unknownValueDecodeErrors(
        "[ 1 , 2, 3]",
        r -> r.moveNext(),
        JsonLfDecoders.bool,
        "Expected boolean but was 1 at line: 1, column: 3");
    unknownValueDecodeErrors(
        "{\n  \"x\": 1,\n  \"y\" : [\n    \"hello\",\n    [ 42]\n    ]\n}",
        r ->
            r.moveNext() // Skip {
                .moveNext() // Skip "x"
                .moveNext() // Skip 1
                .moveNext() // Skip "y"
                .moveNext() // Skip [
                .moveNext(), // Skip "hello"
        JsonLfDecoders.list(JsonLfDecoders.bool),
        "Expected boolean but was 42 at line: 5, column: 7");
  }

  private <T> void checkReadAll(JsonLfDecoder<T> decoder, TestCase<T>... testCases)
      throws JsonLfDecoder.Error {
    for (var tc : testCases) tc.check(decoder);
  }

  @FunctionalInterface
  static interface TestCase<T> {
    void check(JsonLfDecoder<T> decoder) throws JsonLfDecoder.Error;
  }

  @FunctionalInterface
  static interface FunctionThrowsError<In, Out> {
    Out apply(In in) throws JsonLfDecoder.Error;
  }

  private <T> TestCase<T> eq(String input, T expected) {
    return (JsonLfDecoder<T> decoder) -> {
      T actual = decoder.decode(new JsonLfReader(input));
      assertEquals(
          expected,
          actual,
          String.format(
              "input=%s, expected=%s, actual=%s", input, expected.toString(), actual.toString()));
    };
  }

  private <T extends Comparable> TestCase<T> cmpEq(String input, T expected) {
    return (JsonLfDecoder<T> decoder) -> {
      T actual = decoder.decode(new JsonLfReader(input));
      assertEquals(
          0,
          expected.compareTo(actual),
          String.format(
              "unequal by ordering comparison, input=%s, expected=%s, actual=%s",
              input, expected, actual));
    };
  }

  private <T, E extends Throwable> TestCase<T> errors(String input, String errorMessage) {
    return errors(input, errorMessage, null);
  }

  private <T, E extends Throwable> TestCase<T> errors(
      String input, String errorMessage, Class<E> causeClass) {
    return errors(input, Pattern.compile(Pattern.quote(errorMessage)), causeClass);
  }

  private <T, E extends Throwable> TestCase<T> errors(
      String input, Pattern errorMessage, Class<E> causeClass) {
    return (JsonLfDecoder<T> decoder) ->
        this.<T, E>checkDecodeError(
                d -> decoder.decode(new JsonLfReader(input)), input, errorMessage, causeClass)
            .check(decoder);
  }

  private <T> void unknownValueDecodeErrors(
      String input,
      FunctionThrowsError<JsonLfReader, JsonLfReader> updateReader,
      JsonLfDecoder<T> decoder,
      String errorMessage)
      throws JsonLfDecoder.Error {
    JsonLfReader.UnknownValue unknown =
        JsonLfReader.UnknownValue.read(updateReader.apply(new JsonLfReader(input)));
    this.<T, Throwable>checkDecodeError(
            unknown::decodeWith, input, Pattern.compile(Pattern.quote(errorMessage)), null)
        .check(decoder);
  }

  private <T, E extends Throwable> TestCase<T> checkDecodeError(
      FunctionThrowsError<JsonLfDecoder<T>, T> decodeWith,
      String input,
      Pattern errorMessage,
      Class<E> causeClass) {
    return (JsonLfDecoder<T> decoder) -> {
      try {
        T actual = decodeWith.apply(decoder);
        assert false
            : String.format(
                "input='%s' was successfully decoded to %s but should have failed with error"
                    + " message: %s",
                input, actual, errorMessage);
      } catch (JsonLfDecoder.Error e) {
        assert errorMessage.matcher(e.getMessage()).find()
            : String.format(
                "input='%s', expected error message matching\n\t/%s/\nbut was\n\t'%s'",
                input, errorMessage, e.getMessage());
        Throwable cause = e.getCause();
        if (causeClass != null)
          assert cause != null && causeClass.isAssignableFrom(cause.getClass())
              : String.format("Expected cause of %s, but was %s", causeClass, cause);
      }
    };
  }
}
