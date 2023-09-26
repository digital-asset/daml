// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.javaapi.data.codegen.json;

import com.daml.ledger.javaapi.data.Unit;
import com.daml.ledger.javaapi.data.codegen.ContractId;
import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonLocation;
import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;
import java.io.IOException;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.time.Instant;
import java.time.LocalDate;
import java.time.format.DateTimeParseException;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;

// Reads LF-JSON data in a streaming fashion.
// Usage for these is simply to construct them, and then pass them into a decoder
// which can attempt decode the appropriate type.
// Can be used by code-gen.
public class JsonLfReader {
  private final String json; // Used to reference unknown values until they can be decoded.
  private static final JsonFactory jsonFactory = new JsonFactory();
  private final JsonParser parser;

  public JsonLfReader(String json) throws JsonLfDecoder.Error {
    this.json = json;
    try {
      parser = jsonFactory.createParser(json);
    } catch (IOException e) {
      throw new JsonLfDecoder.Error("Failed to create parser", locationStart(), e);
    }
    try {
      parser.nextToken();
    } catch (IOException e) {
      throw new JsonLfDecoder.Error("JSON parse error", locationStart(), e);
    }
  }

  /// Readers for built-in LF types. ///
  public static class Decoders {

    public static final JsonLfDecoder<Unit> unit =
        r -> {
          r.readStartObject();
          r.readEndObject();
          return Unit.getInstance();
        };

    public static final JsonLfDecoder<Boolean> bool =
        r -> {
          r.expectIsAt("boolean", JsonToken.VALUE_TRUE, JsonToken.VALUE_FALSE);
          Boolean value = null;
          try {
            value = r.parser.getBooleanValue();
          } catch (IOException e) {
            r.parseExpected("true or false", e);
          }
          r.moveNext();
          return value;
        };

    public static final JsonLfDecoder<Long> int64 =
        r -> {
          r.expectIsAt("int64", JsonToken.VALUE_NUMBER_INT, JsonToken.VALUE_STRING);
          Long value = null;
          try {
            value = Long.parseLong(r.parser.getText());
          } catch (IOException e) {
            r.parseExpected("int64", e);
          } catch (NumberFormatException e) {
            r.parseExpected("int64", e);
          }
          r.moveNext();
          return value;
        };

    public static JsonLfDecoder<BigDecimal> numeric(int scale) {
      assert scale >= 0 : "negative numeric scale " + scale;
      return r -> {
        r.expectIsAt(
            "numeric",
            JsonToken.VALUE_NUMBER_INT,
            JsonToken.VALUE_NUMBER_FLOAT,
            JsonToken.VALUE_STRING);
        BigDecimal value = null;
        try {
          value = new BigDecimal(r.parser.getText());
        } catch (NumberFormatException e) {
          r.parseExpected("numeric", e);
        } catch (IOException e) {
          r.parseExpected("numeric", e);
        }
        int orderMag = MAX_NUMERIC_PRECISION - scale; // Available digits on lhs of decimal point
        if (value.precision() - value.scale() > orderMag) {
          r.parseExpected(String.format("numeric in range (-10^%s, 10^%s)", orderMag, orderMag));
        }
        if (value.scale() > scale) value = value.setScale(scale, RoundingMode.HALF_EVEN);
        r.moveNext();
        return value;
      };
    }

    public static final JsonLfDecoder<Instant> timestamp =
        r -> {
          r.expectIsAt("timestamp", JsonToken.VALUE_STRING);
          Instant value = null;
          try {
            value = Instant.parse(r.parser.getText()).truncatedTo(ChronoUnit.MICROS);
          } catch (DateTimeParseException e) {
            r.parseExpected("valid ISO 8601 date and time in UTC", e);
          } catch (IOException e) {
            r.parseExpected("timestamp", e);
          }
          r.moveNext();
          return value;
        };

    public static final JsonLfDecoder<LocalDate> date =
        r -> {
          r.expectIsAt("date", JsonToken.VALUE_STRING);
          LocalDate value = null;
          try {
            value = LocalDate.parse(r.parser.getText());
          } catch (DateTimeParseException e) {
            r.parseExpected("valid ISO 8601 date", e);
          } catch (IOException e) {
            r.parseExpected("date", e);
          }
          r.moveNext();
          return value;
        };

    public static final JsonLfDecoder<String> text =
        r -> {
          r.expectIsAt("text", JsonToken.VALUE_STRING);
          String value = null;
          try {
            value = r.parser.getText();
          } catch (IOException e) {
            r.parseExpected("valid textual value", e);
          }
          r.moveNext();
          return value;
        };

    public static final JsonLfDecoder<String> party = text;

    public static <C extends ContractId<?>> JsonLfDecoder<C> contractId(
        Function<String, C> constr) {
      return r -> {
        String id = text.decode(r);
        return constr.apply(id);
      };
    }

    // Read an list with an unknown number of items of the same type.
    public static <T> JsonLfDecoder<List<T>> list(JsonLfDecoder<T> decodeItem) {
      return r -> {
        List<T> list = new ArrayList<>();
        r.readStartArray();
        while (r.notEndArray()) {
          T item = decodeItem.decode(r);
          list.add(item);
        }
        r.readEndArray();
        return list;
      };
    }

    // Read a map with textual keys, and unknown number of items of the same type.
    public static <V> JsonLfDecoder<Map<String, V>> textMap(JsonLfDecoder<V> decodeValue) {
      return r -> {
        Map<String, V> map = new LinkedHashMap<>();
        r.readStartObject();
        while (r.notEndObject()) {
          String key = r.readFieldName();
          V val = decodeValue.decode(r);
          map.put(key, val);
        }
        r.readEndObject();
        return map;
      };
    }

    // Read a map with unknown number of items of the same types.
    public static <K, V> JsonLfDecoder<Map<K, V>> genMap(
        JsonLfDecoder<K> decodeKey, JsonLfDecoder<V> decodeVal) {
      return r -> {
        Map<K, V> map = new LinkedHashMap<>();
        // Maps are represented as an array of 2-element arrays.
        r.readStartArray();
        while (r.notEndArray()) {
          r.readStartArray();
          K key = decodeKey.decode(r);
          V val = decodeVal.decode(r);
          r.readEndArray();
          map.put(key, val);
        }
        r.readEndArray();
        return map;
      };
    }

    // The T type should not itself be Optional<?>. In that case use OptionalNested below.
    public static <T> JsonLfDecoder<Optional<T>> optional(JsonLfDecoder<T> decodeVal) {
      return r -> {
        if (r.parser.currentToken() == JsonToken.VALUE_NULL) {
          r.moveNext();
          return Optional.empty();
        } else {
          T some = decodeVal.decode(r);
          assert (!(some instanceof Optional))
              : "Used `optional` to decode a "
                  + some.getClass()
                  + " but `optionalNested` must be used for the outer decoders of nested"
                  + " Optional";
          return Optional.of(some);
        }
      };
    }

    public static <T> JsonLfDecoder<Optional<Optional<T>>> optionalNested(
        JsonLfDecoder<Optional<T>> decodeVal) {
      return r -> {
        if (r.parser.currentToken() == JsonToken.VALUE_NULL) {
          r.moveNext();
          return Optional.empty();
        } else {
          r.readStartArray();
          if (r.parser.currentToken() == JsonToken.VALUE_NULL) r.parseExpected("] or item");
          Optional<T> val = r.notEndArray() ? decodeVal.decode(r) : Optional.empty();
          r.readEndArray();
          return Optional.of(val);
        }
      };
    }

    public static <E extends Enum<E>> JsonLfDecoder<E> enumeration(Map<String, E> damlNameToEnum) {
      return r -> r.readFromText(damlNameToEnum::get, new ArrayList<>(damlNameToEnum.keySet()));
    }

    // Provides a generic way to read a variant type, by specifying each tag.
    public static <T> JsonLfDecoder<T> variant(
        List<String> tagNames, Function<String, JsonLfDecoder<? extends T>> decoderByName) {
      return r -> {
        r.readStartObject();
        T result = null;
        switch (r.readFieldName("tag", "value")) {
          case "tag":
            {
              var decoder = r.readFromText(decoderByName, tagNames);
              r.readFieldName("value");
              result = decoder.decode(r);
              break;
            }
          case "value":
            {
              UnknownValue unknown = UnknownValue.read(r); // Can't decode until we know the tag.
              r.readFieldName("tag");
              var decoder = r.readFromText(decoderByName, tagNames);
              result = unknown.decodeWith(decoder);
              break;
            }
        }
        r.readEndObject();
        return result;
      };
    }

    // Provides a generic way to read a record type, with a constructor arg for each field.
    // This is a little fragile, so is better used by code-gen. Specifically:
    // - The constructor must cast the elements and pass them to the T's constructor appropriately.
    // - The elements of fieldNames should all evaluate to non non-null when applied to
    // fieldsByName.
    // - The argIndex field values should correspond to the args passed to the constructor.
    //
    // e.g.
    //     r.record(
    //        asList("i", "b"),
    //        name -> {
    //          switch (name) {
    //            case "i":
    //              return JsonLfReader.Field.at(0, r.list(r.int64()));
    //            case "b":
    //              return JsonLfReader.Field.at(1, r.bool(), false);
    //            default:
    //              return null;
    //          }
    //        },
    //        args -> new Foo((List<Long>) args[0], (Boolean) args[1]))
    //     )
    public static <T> JsonLfDecoder<T> record(
        List<String> fieldNames,
        Function<String, Field<? extends Object>> fieldsByName,
        Function<Object[], T> constr) {
      return r -> {
        Object[] args = new Object[fieldNames.size()];
        if (r.isStartObject()) {
          r.readStartObject();
          while (r.notEndObject()) {
            var fieldLoc = r.locationStart();
            String fieldName = r.readFieldName();
            var field = fieldsByName.apply(fieldName);
            if (field == null) r.unknownField(fieldName, fieldNames, fieldLoc);
            else args[field.argIndex] = field.decode.decode(r);
          }
          r.readEndObject();
        } else if (r.isStartArray()) {
          r.readStartArray();
          for (String fieldName : fieldNames) {
            var field = fieldsByName.apply(fieldName);
            args[field.argIndex] = field.decode.decode(r);
          }
          r.readEndArray();
        } else {
          r.parseExpected("object or array");
        }

        // Handle missing fields.
        for (String fieldName : fieldNames) {
          Field<? extends Object> field = fieldsByName.apply(fieldName);
          if (args[field.argIndex] != null) continue;
          if (field.defaultVal == null) r.missingField(fieldName);
          args[field.argIndex] = field.defaultVal;
        }

        return constr.apply(args);
      };
    }

    public static class Field<T> {
      final int argIndex;
      final JsonLfDecoder<T> decode;
      final T defaultVal; // If non-null, used to populate value of missing fields.

      private Field(int argIndex, JsonLfDecoder<T> decode, T defaultVal) {
        this.argIndex = argIndex;
        this.decode = decode;
        this.defaultVal = defaultVal;
      }

      public static <T> Field<T> at(int argIndex, JsonLfDecoder<T> decode, T defaultVal) {
        return new Field<T>(argIndex, decode, defaultVal);
      }

      public static <T> Field<T> at(int argIndex, JsonLfDecoder<T> decode) {
        return new Field<T>(argIndex, decode, null);
      }
    }

    @SuppressWarnings("unchecked")
    // Can be used within the `constr` arg to `record`, to allow casting without producing warnings.
    public static <T> T cast(Object o) {
      return (T) o;
    }
  }

  // Represents the current reader location within the input. Not intended for JSON values > 2GB.
  public static class Location {
    public final int line;
    public final int column;
    public final int charOffset;

    public Location(int line, int column, int charOffset) {
      this.line = line;
      this.column = column;
      this.charOffset = charOffset;
    }

    public Location advance(Location that) {
      int col = (that.line == 1) ? this.column + that.column - 1 : that.column;
      return new Location(this.line + that.line - 1, col, this.charOffset + that.charOffset);
    }
  }

  // Represents a value whose type is not yet known, but should be preserved for later decoding.
  public static class UnknownValue {
    private final String jsonRepr;
    private final Location start;

    private UnknownValue(String jsonRepr, Location start) {
      this.jsonRepr = jsonRepr;
      this.start = start;
    }

    public static UnknownValue read(JsonLfReader r) throws JsonLfDecoder.Error {
      Location from = r.locationStart();
      try {
        r.parser.skipChildren();
        Location to = r.locationEnd();
        String repr = r.json.substring(from.charOffset, to.charOffset).trim();
        r.moveNext();
        return new UnknownValue(repr, from);
      } catch (IOException e) {
        throw new JsonLfDecoder.Error("cannot read unknown value", r.locationStart(), e);
      }
    }

    public <T> T decodeWith(JsonLfDecoder<T> decoder) throws JsonLfDecoder.Error {
      try {
        return decoder.decode(new JsonLfReader(this.jsonRepr));
      } catch (JsonLfDecoder.Error e) {
        throw e.fromStartLocation(this.start); // Adjust location to offset by the start position.
      }
    }
  }

  // Can override these two for different handling of these cases.
  protected void missingField(String fieldName) throws JsonLfDecoder.Error {
    throw new JsonLfDecoder.Error(String.format("Missing field %s", fieldName), locationStart());
  }

  protected void unknownField(String fieldName, List<String> expected, Location loc)
      throws JsonLfDecoder.Error {
    UnknownValue.read(this); // Consume the value from the reader.
    throw new JsonLfDecoder.Error(
        String.format("Unknown field %s (known fields are %s)", fieldName, expected), loc);
  }

  /// Used for branching and looping on objects and arrays. ///

  private boolean isStartObject() {
    return parser.currentToken() == JsonToken.START_OBJECT;
  }

  private boolean notEndObject() {
    return !parser.isClosed() && parser.currentToken() != JsonToken.END_OBJECT;
  }

  private boolean isStartArray() {
    return parser.currentToken() == JsonToken.START_ARRAY;
  }

  private boolean notEndArray() {
    return !parser.isClosed() && parser.currentToken() != JsonToken.END_ARRAY;
  }

  /// Used for consuming the structural components of objects and arrays. ///

  private void readStartObject() throws JsonLfDecoder.Error {
    expectIsAt("{", JsonToken.START_OBJECT);
    moveNext();
  }

  private void readEndObject() throws JsonLfDecoder.Error {
    expectIsAt("}", JsonToken.END_OBJECT);
    moveNext();
  }

  private void readStartArray() throws JsonLfDecoder.Error {
    expectIsAt("[", JsonToken.START_ARRAY);
    moveNext();
  }

  private void readEndArray() throws JsonLfDecoder.Error {
    expectIsAt("]", JsonToken.END_ARRAY);
    moveNext();
  }

  // Read a field name. If any expected values are provided, the field name must match one.
  private String readFieldName(String... expected) throws JsonLfDecoder.Error {
    String want = (expected.length == 0) ? "field name" : "field " + String.join(" or ", expected);
    expectIsAt(want, JsonToken.FIELD_NAME);
    String fieldName = null;
    try {
      fieldName = parser.getText();
    } catch (IOException e) {
      parseExpected("textual field name", e);
    }
    if (expected.length > 0 && !Arrays.asList(expected).contains(fieldName)) {
      parseExpected(want, null, fieldName, locationStart());
    }
    moveNext();
    return fieldName;
  }

  private <T> T readFromText(Function<String, T> interpreter, List<String> expected)
      throws JsonLfDecoder.Error {
    Location textLoc = locationStart();
    String got = Decoders.text.decode(this);
    T result = interpreter.apply(got);
    if (result == null) parseExpected("one of " + expected, null, got, textLoc);
    return result;
  }

  private Location locationStart() {
    JsonLocation loc = parser.currentTokenLocation();
    return new Location(loc.getLineNr(), loc.getColumnNr(), (int) loc.getCharOffset());
  }

  private Location locationEnd() {
    JsonLocation loc = parser.currentLocation();
    return new Location(loc.getLineNr(), loc.getColumnNr(), (int) loc.getCharOffset());
  }

  private String currentText() {
    try {
      String text = parser.getText();
      return (text == null) ? "nothing" : text;
    } catch (IOException e) {
      return "? (" + e.getMessage() + ")";
    }
  }

  private void parseExpected(String expected) throws JsonLfDecoder.Error {
    parseExpected(expected, null);
  }

  private void parseExpected(String expected, Throwable cause) throws JsonLfDecoder.Error {
    parseExpected(expected, cause, currentText(), locationStart());
  }

  private void parseExpected(String expected, Throwable cause, String actual, Location location)
      throws JsonLfDecoder.Error {
    String message = String.format("Expected %s but was %s", expected, actual);
    throw new JsonLfDecoder.Error(message, location, cause);
  }

  private void expectIsAt(String description, JsonToken... expected) throws JsonLfDecoder.Error {
    for (int i = 0; i < expected.length; i++) {
      if (parser.currentToken() == expected[i]) return;
    }
    parseExpected(description);
  }

  JsonLfReader moveNext() throws JsonLfDecoder.Error {
    try {
      parser.nextToken();
      return this;
    } catch (JsonParseException e) {
      throw new JsonLfDecoder.Error("JSON parse error", locationEnd(), e);
    } catch (IOException e) {
      throw new JsonLfDecoder.Error(String.format("Read failed"), locationEnd(), e);
    }
  }

  private static final int MAX_NUMERIC_PRECISION = 38;
}
