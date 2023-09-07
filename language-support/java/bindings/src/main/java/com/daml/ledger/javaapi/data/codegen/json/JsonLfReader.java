// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.javaapi.data.codegen.json;

import com.daml.ledger.javaapi.data.Unit;
import com.daml.ledger.javaapi.data.codegen.ContractId;
import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;
import java.io.IOException;
import java.io.Reader;
import java.io.StringReader;
import java.math.BigDecimal;
import java.time.Instant;
import java.time.LocalDate;
import java.time.format.DateTimeParseException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.TreeMap;
import java.util.function.Function;

// Utility to read LF-JSON data in a streaming fashion. Can be used by code-gen.
public class JsonLfReader {
  private static final JsonFactory jsonFactory = new JsonFactory();
  private final JsonParser parser;

  public JsonLfReader(Reader textReader) throws IOException {
    parser = jsonFactory.createParser(textReader);
    parser.nextToken();
  }

  public JsonLfReader(String text) throws IOException {
    this(new StringReader(text));
  }

  // Can override these two for different handling of these cases.
  protected void missingField(Object obj, String fieldName) throws FromJson.Error {
    throw new FromJson.Error(
        String.format(
            "Missing field %s.%s at %s", obj.getClass().getCanonicalName(), fieldName, location()));
  }

  protected void unknownFields(Object obj, List<String> fieldNames) throws FromJson.Error {
    throw new FromJson.Error(
        String.format(
            "Unknown fields %s.%s at %s",
            obj.getClass().getCanonicalName(), fieldNames.toString(), location()));
  }

  private void parseExpected(String expected) throws FromJson.Error {
    throw new FromJson.Error(
        String.format("Expected %s but was %s at %s", expected, currentText(), location()));
  }

  /// Readers for built-in LF types. ///

  public static final FromJson<Unit> unit =
      r -> {
        r.readStartObject();
        r.readEndObject();
        return Unit.getInstance();
      };

  public static final FromJson<Boolean> bool =
      r -> {
        r.expectIsAt("boolean", JsonToken.VALUE_TRUE, JsonToken.VALUE_FALSE);
        Boolean value = null;
        try {
          value = r.parser.getBooleanValue();
        } catch (IOException e) {
          r.parseExpected("true or false");
        }
        r.moveNext();
        return value;
      };

  public static final FromJson<Long> int64 =
      r -> {
        r.expectIsAt("int64", JsonToken.VALUE_NUMBER_INT, JsonToken.VALUE_STRING);
        Long value = null;
        try {
          value = Long.parseLong(r.parser.getText());
        } catch (IOException e) {
          r.parseExpected("int64");
        } catch (NumberFormatException e) {
          r.parseExpected("int64");
        }
        r.moveNext();
        return value;
      };

  public static final FromJson<BigDecimal> decimal =
      r -> {
        r.expectIsAt(
            "decimal",
            JsonToken.VALUE_NUMBER_INT,
            JsonToken.VALUE_NUMBER_FLOAT,
            JsonToken.VALUE_STRING);
        BigDecimal value = null;
        try {
          value = new BigDecimal(r.parser.getText());
        } catch (NumberFormatException e) {
          r.parseExpected("decimal");
        } catch (IOException e) {
          r.parseExpected("decimal");
        }
        r.moveNext();
        return value;
      };

  public static final FromJson<Instant> timestamp =
      r -> {
        r.expectIsAt("timestamp", JsonToken.VALUE_STRING);
        Instant value = null;
        try {
          value = Instant.parse(r.parser.getText());
        } catch (DateTimeParseException e) {
          r.parseExpected("timestamp");
        } catch (IOException e) {
          r.parseExpected("timestamp");
        }
        r.moveNext();
        return value;
      };

  public static final FromJson<LocalDate> date =
      r -> {
        r.expectIsAt("date", JsonToken.VALUE_STRING);
        LocalDate value = null;
        try {
          value = LocalDate.parse(r.parser.getText());
        } catch (DateTimeParseException e) {
          r.parseExpected("date");
        } catch (IOException e) {
          r.parseExpected("date");
        }
        r.moveNext();
        return value;
      };

  public static final FromJson<String> text =
      r -> {
        r.expectIsAt("text", JsonToken.VALUE_STRING);
        String value = null;
        try {
          value = r.parser.getText();
        } catch (IOException e) {
          r.parseExpected("valid textual value");
        }
        r.moveNext();
        return value;
      };

  public static final FromJson<String> party = text;

  public static <C extends ContractId<?>> FromJson<C> contractId(Function<String, C> constr) {
    return r -> {
      String id = text.read(r);
      return constr.apply(id);
    };
  }

  // Read an list with an unknown number of items of the same type.
  public static <T> FromJson<List<T>> list(FromJson<T> readItem) {
    return r -> {
      List<T> list = new ArrayList<>();
      r.readStartArray();
      while (r.notEndArray()) {
        T item = readItem.read(r);
        list.add(item);
      }
      r.readEndArray();
      return list;
    };
  }

  // Read a map with textual keys, and unknown number of items of the same type.
  public static <V> FromJson<Map<String, V>> textMap(FromJson<V> readValue) {
    return r -> {
      Map<String, V> map = new TreeMap<>();
      r.readStartObject();
      while (r.notEndObject()) {
        String key = r.readFieldName();
        V val = readValue.read(r);
        map.put(key, val);
      }
      r.readEndObject();
      return map;
    };
  }

  // Read a map with unknown number of items of the same types.
  public static <K, V> FromJson<Map<K, V>> genMap(FromJson<K> readKey, FromJson<V> readValue) {
    return r -> {
      Map<K, V> map = new TreeMap<>();
      // Maps are represented as an array of 2-element arrays.
      r.readStartArray();
      while (r.notEndArray()) {
        r.readStartArray();
        K key = readKey.read(r);
        V val = readValue.read(r);
        r.readEndArray();
        map.put(key, val);
      }
      r.readEndArray();
      return map;
    };
  }

  // The T type should not itself be Optional<?>. In that case use OptionalNested below.
  public static <T> FromJson<Optional<T>> optional(FromJson<T> readValue) {
    return r -> {
      if (r.parser.currentToken() == JsonToken.VALUE_NULL) {
        r.moveNext();
        return Optional.empty();
      } else {
        T some = readValue.read(r);
        if (some instanceof Optional) {
          throw new IllegalArgumentException(
              "Used `optional` to decode a "
                  + some.getClass()
                  + " but `optionalNested` must be used for the outer decoders of nested Optional");
        }
        return Optional.of(some);
      }
    };
  }

  public static <T> FromJson<Optional<Optional<T>>> optionalNested(
      FromJson<Optional<T>> readValue) {
    return r -> {
      if (r.parser.currentToken() == JsonToken.VALUE_NULL) {
        r.moveNext();
        return Optional.empty();
      } else {
        r.readStartArray();
        Optional<T> val = r.notEndArray() ? readValue.read(r) : Optional.empty();
        r.readEndArray();
        return Optional.of(val);
      }
    };
  }

  public static <E extends Enum<E>> FromJson<E> enumeration(Class<E> enumClass) {
    return r -> {
      String value = text.read(r);
      try {
        return Enum.valueOf(enumClass, value);
      } catch (IllegalArgumentException e) {
        r.parseExpected(String.format("constant of %s", enumClass.getName()));
      }
      return null;
    };
  }

  // Provides a generic way to read a variant type, by specifying each tag.
  public static <T> FromJson<T> variant(List<String> tagNames, TagReader<T> readTag) {
    return r -> {
      r.readStartObject();
      if (!r.readFieldName().equals("tag")) r.parseExpected("tag field");
      String tagName = text.read(r);
      if (!r.readFieldName().equals("value")) r.parseExpected("value field");
      T result = readTag.get(tagName).read(r);
      r.readEndObject();
      if (result == null)
        r.parseExpected(String.format("tag of %s", String.join(" or ", tagNames)));
      return result;
    };
  }

  public static interface TagReader<T> {
    FromJson<T> get(String tagName) throws FromJson.Error;
  }

  // Provides a generic way to read a record type, with a constructor arg for each field.
  // This is a little fragile, so is better used by code-gen. Specifically:
  // - The constructor must cast the elements and pass them to the T's constructor appropriately.
  // - The elements of fieldNames should all evaluate to non non-null when applied to fieldsByName.
  // - The argIndex field values should correspond to the args passed to the constructor.
  //
  // e.g.
  //     r.record(
  //        args -> new Foo((Long) args[0], (Boolean) args[1]),
  //        asList("i", "b"),
  //        fieldName -> {
  //          switch (fieldName) {
  //            case "i":
  //              return JsonLfReader.Field.required(0, r.int64());
  //            case "b":
  //              return JsonLfReader.Field.optional(1, r.bool(), false);
  //            default:
  //              return null;
  //          }
  //        }
  //     )
  public static <T> FromJson<T> record(
      Function<Object[], T> constr,
      List<String> fieldNames,
      Function<String, Field<? extends Object>> fieldsByName) {
    return r -> {
      List<String> missingFields = new ArrayList<>();
      List<String> unknownFields = new ArrayList<>();

      Object[] args = new Object[fieldNames.size()];
      if (r.isStartObject()) {
        r.readStartObject();
        while (r.notEndObject()) {
          String fieldName = r.readFieldName();
          var field = fieldsByName.apply(fieldName);
          if (field == null) unknownFields.add(fieldName);
          else args[field.argIndex] = field.fromJson.read(r);
        }
        r.readEndObject();
      } else if (r.isStartArray()) {
        r.readStartArray();
        for (String fieldName : fieldNames) {
          var field = fieldsByName.apply(fieldName);
          args[field.argIndex] = field.fromJson.read(r);
        }
        r.readEndArray();
      } else {
        r.parseExpected("object or array");
      }

      // Handle missing and unknown fields.
      for (String fieldName : fieldNames) {
        Field<? extends Object> field = fieldsByName.apply(fieldName);
        if (args[field.argIndex] != null) continue;
        if (field.defaultVal == null) missingFields.add(fieldName);
        args[field.argIndex] = field.defaultVal;
      }
      T result = constr.apply(args);
      for (String f : missingFields) r.missingField(result, f);
      if (!unknownFields.isEmpty()) r.unknownFields(result, unknownFields);

      return result;
    };
  }

  public static class Field<T> {
    final int argIndex;
    final FromJson<T> fromJson;
    final T defaultVal; // If non-null, used to populate value of missing fields.

    private Field(int argIndex, FromJson<T> fromJson, T defaultVal) {
      this.argIndex = argIndex;
      this.fromJson = fromJson;
      this.defaultVal = defaultVal;
    }

    public static <T> Field<T> optional(int argIndex, FromJson<T> fromJson, T defaultVal) {
      return new Field<T>(argIndex, fromJson, defaultVal);
    }

    public static <T> Field<T> required(int argIndex, FromJson<T> fromJson) {
      return new Field<T>(argIndex, fromJson, null);
    }
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

  private void readStartObject() throws FromJson.Error {
    expectIsAt("{", JsonToken.START_OBJECT);
    moveNext();
  }

  private void readEndObject() throws FromJson.Error {
    expectIsAt("}", JsonToken.END_OBJECT);
    moveNext();
  }

  private void readStartArray() throws FromJson.Error {
    expectIsAt("[", JsonToken.START_ARRAY);
    moveNext();
  }

  private void readEndArray() throws FromJson.Error {
    expectIsAt("]", JsonToken.END_ARRAY);
    moveNext();
  }

  private String readFieldName() throws FromJson.Error {
    expectIsAt("field name", JsonToken.FIELD_NAME);
    String fieldName = null;
    try {
      fieldName = parser.getText();
    } catch (IOException e) {
      parseExpected("textual field name");
    }
    moveNext();
    return fieldName;
  }

  private String location() {
    return parser.currentTokenLocation().offsetDescription();
  }

  private String currentText() {
    try {
      return parser.getText();
    } catch (IOException e) {
      return "? (" + e.getMessage() + ")";
    }
  }

  private void expectIsAt(String description, JsonToken... expected) throws FromJson.Error {
    for (int i = 0; i < expected.length; i++) {
      if (parser.currentToken() == expected[i]) return;
    }
    parseExpected(description);
  }

  private void moveNext() throws FromJson.Error {
    try {
      parser.nextToken();
    } catch (IOException e) {
      parseExpected("more input");
    }
  }
}
