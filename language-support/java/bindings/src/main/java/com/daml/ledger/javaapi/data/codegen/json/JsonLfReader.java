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

  public FromJson<Unit> unit() {
    return () -> {
      readStartObject();
      readEndObject();
      return Unit.getInstance();
    };
  }

  public FromJson<Boolean> bool() {
    return () -> {
      expectIsAt("boolean", JsonToken.VALUE_TRUE, JsonToken.VALUE_FALSE);
      Boolean value = null;
      try {
        value = parser.getBooleanValue();
      } catch (IOException e) {
        parseExpected("true or false");
      }
      moveNext();
      return value;
    };
  }

  public FromJson<Long> int64() {
    return () -> {
      expectIsAt("int64", JsonToken.VALUE_NUMBER_INT, JsonToken.VALUE_STRING);
      Long value = null;
      try {
        value = Long.parseLong(parser.getText());
      } catch (IOException e) {
        parseExpected("int64");
      } catch (NumberFormatException e) {
        parseExpected("int64");
      }
      moveNext();
      return value;
    };
  }

  public FromJson<BigDecimal> decimal() {
    return () -> {
      expectIsAt(
          "decimal",
          JsonToken.VALUE_NUMBER_INT,
          JsonToken.VALUE_NUMBER_FLOAT,
          JsonToken.VALUE_STRING);
      BigDecimal value = null;
      try {
        value = new BigDecimal(parser.getText());
      } catch (NumberFormatException e) {
        parseExpected("decimal");
      } catch (IOException e) {
        parseExpected("decimal");
      }
      moveNext();
      return value;
    };
  }

  public FromJson<Instant> timestamp() {
    return () -> {
      expectIsAt("timestamp", JsonToken.VALUE_STRING);
      Instant value = null;
      try {
        value = Instant.parse(parser.getText());
      } catch (DateTimeParseException e) {
        parseExpected("timestamp");
      } catch (IOException e) {
        parseExpected("timestamp");
      }
      moveNext();
      return value;
    };
  }

  public FromJson<LocalDate> date() {
    return () -> {
      expectIsAt("date", JsonToken.VALUE_STRING);
      LocalDate value = null;
      try {
        value = LocalDate.parse(parser.getText());
      } catch (DateTimeParseException e) {
        parseExpected("date");
      } catch (IOException e) {
        parseExpected("date");
      }
      moveNext();
      return value;
    };
  }

  public FromJson<String> party() {
    return text();
  }

  public <C extends ContractId<?>> FromJson<C> contractId(Function<String, C> constr) {
    return () -> {
      String id = text().read();
      return constr.apply(id);
    };
  }

  public FromJson<String> text() {
    return () -> {
      expectIsAt("text", JsonToken.VALUE_STRING);
      String value = null;
      try {
        value = parser.getText();
      } catch (IOException e) {
        parseExpected("valid textual value");
      }
      moveNext();
      return value;
    };
  }

  // Read an list with an unknown number of items of the same type.
  public <T> FromJson<List<T>> list(FromJson<T> readItem) {
    return () -> {
      List<T> list = new ArrayList<>();
      readStartArray();
      while (notEndArray()) {
        T item = readItem.read();
        list.add(item);
      }
      readEndArray();
      return list;
    };
  }

  // Read a map with textual keys, and unknown number of items of the same type.
  public <V> FromJson<Map<String, V>> textMap(FromJson<V> readValue) {
    return () -> {
      Map<String, V> map = new TreeMap<>();
      readStartObject();
      while (notEndObject()) {
        String key = readFieldName();
        V val = readValue.read();
        map.put(key, val);
      }
      ;
      readEndObject();
      return map;
    };
  }

  // Read a map with unknown number of items of the same types.
  public <K, V> FromJson<Map<K, V>> genMap(FromJson<K> readKey, FromJson<V> readValue) {
    return () -> {
      Map<K, V> map = new TreeMap<>();
      // Maps are represented as an array of 2-element arrays.
      readStartArray();
      while (notEndArray()) {
        readStartArray();
        K key = readKey.read();
        V val = readValue.read();
        readEndArray();
        map.put(key, val);
      }
      ;
      readEndArray();
      return map;
    };
  }

  // The T type should not itself be Optional<?>. In that case use OptionalNested below.
  public <T> FromJson<Optional<T>> optional(FromJson<T> readValue) {
    return () -> {
      if (parser.currentToken() == JsonToken.VALUE_NULL) {
        moveNext();
        return Optional.empty();
      } else {
        return Optional.of(readValue.read());
      }
    };
  }

  public <T> FromJson<Optional<Optional<T>>> optionalNested(FromJson<Optional<T>> readValue) {
    return () -> {
      if (parser.currentToken() == JsonToken.VALUE_NULL) {
        moveNext();
        return Optional.empty();
      } else {
        readStartArray();
        Optional<T> val = notEndArray() ? readValue.read() : Optional.empty();
        readEndArray();
        return Optional.of(val);
      }
    };
  }

  public <E extends Enum<E>> FromJson<E> enumeration(Class<E> enumClass) {
    return () -> {
      String value = text().read();
      try {
        return Enum.valueOf(enumClass, value);
      } catch (IllegalArgumentException e) {
        parseExpected(String.format("constant of %s", enumClass.getName()));
      }
      return null;
    };
  }

  // Provides a generic way to read a variant type, by specifying each tag.
  public <T> FromJson<T> variant(List<String> tagNames, TagReader<T> readTag) {
    return () -> {
      readStartObject();
      if (!readFieldName().equals("tag")) parseExpected("tag field");
      String tagName = text().read();
      if (!readFieldName().equals("value")) parseExpected("value field");
      T result = readTag.get(tagName);
      readEndObject();
      if (result == null) parseExpected(String.format("tag of %s", String.join(" or ", tagNames)));
      return result;
    };
  }

  public interface TagReader<T> {
    T get(String tagName) throws FromJson.Error;
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
  public <T> FromJson<T> record(
      Function<Object[], T> constr,
      List<String> fieldNames,
      Function<String, Field<? extends Object>> fieldsByName) {
    return () -> {
      List<String> missingFields = new ArrayList<>();
      List<String> unknownFields = new ArrayList<>();

      Object[] args = new Object[fieldNames.size()];
      if (isStartObject()) {
        readStartObject();
        while (notEndObject()) {
          String fieldName = readFieldName();
          var field = fieldsByName.apply(fieldName);
          if (field == null) unknownFields.add(fieldName);
          else args[field.argIndex] = field.fromJson.read();
        }
        readEndObject();
      } else if (isStartArray()) {
        readStartArray();
        for (String fieldName : fieldNames) {
          var field = fieldsByName.apply(fieldName);
          args[field.argIndex] = field.fromJson.read();
        }
        readEndArray();
      } else {
        parseExpected("object or array");
      }

      // Handle missing and unknown fields.
      for (String fieldName : fieldNames) {
        Field<? extends Object> field = fieldsByName.apply(fieldName);
        if (args[field.argIndex] != null) continue;
        if (field.defaultVal == null) missingFields.add(fieldName);
        args[field.argIndex] = field.defaultVal;
      }
      T result = constr.apply(args);
      for (String f : missingFields) missingField(result, f);
      if (!unknownFields.isEmpty()) unknownFields(result, unknownFields);

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
