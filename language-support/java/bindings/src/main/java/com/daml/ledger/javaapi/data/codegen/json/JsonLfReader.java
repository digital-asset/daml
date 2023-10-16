// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.javaapi.data.codegen.json;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonLocation;
import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;
import java.io.IOException;
import java.util.List;
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
        String repr = r.json.substring(from.charOffset, to.charOffset);
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

  boolean isStartObject() {
    return parser.currentToken() == JsonToken.START_OBJECT;
  }

  boolean notEndObject() {
    return !parser.isClosed() && parser.currentToken() != JsonToken.END_OBJECT;
  }

  boolean isStartArray() {
    return parser.currentToken() == JsonToken.START_ARRAY;
  }

  boolean notEndArray() {
    return !parser.isClosed() && parser.currentToken() != JsonToken.END_ARRAY;
  }

  boolean isNull() {
    return parser.currentToken() == JsonToken.VALUE_NULL;
  }

  /// Used for consuming the structural components of objects and arrays. ///

  void readStartObject() throws JsonLfDecoder.Error {
    expectIsAt("{", JsonToken.START_OBJECT);
    moveNext();
  }

  void readEndObject() throws JsonLfDecoder.Error {
    expectIsAt("}", JsonToken.END_OBJECT);
    moveNext();
  }

  void readStartArray() throws JsonLfDecoder.Error {
    expectIsAt("[", JsonToken.START_ARRAY);
    moveNext();
  }

  void readEndArray() throws JsonLfDecoder.Error {
    expectIsAt("]", JsonToken.END_ARRAY);
    moveNext();
  }

  class FieldName {
    final String name;
    final Location loc;

    private FieldName(String name, Location loc) {
      this.name = name;
      this.loc = loc;
    }
  }

  FieldName readFieldName() throws JsonLfDecoder.Error {
    expectIsAt("field", JsonToken.FIELD_NAME);
    String name = null;
    try {
      name = parser.getText();
    } catch (IOException e) {
      parseExpected("textual field name", e);
    }
    Location loc = locationStart();
    moveNext();
    return new FieldName(name, loc);
  }

  <T> T readFromText(Function<String, T> interpreter, List<String> expected)
      throws JsonLfDecoder.Error {
    expectIsAt("text", JsonToken.VALUE_STRING);
    String got = null;
    try {
      got = parser.getText();
    } catch (IOException e) {
      parseExpected("valid textual value", e);
    }
    T result = interpreter.apply(got);
    if (result == null) parseExpected("one of " + expected);
    moveNext();
    return result;
  }

  Location locationStart() {
    JsonLocation loc = parser.currentTokenLocation();
    return new Location(loc.getLineNr(), loc.getColumnNr(), (int) loc.getCharOffset());
  }

  Location locationEnd() {
    // First request the parser to retrieve the whole token.
    try {
      parser.finishToken();
    } catch (IOException e) {
    }
    JsonLocation loc = parser.currentLocation();
    return new Location(loc.getLineNr(), loc.getColumnNr(), (int) loc.getCharOffset());
  }

  String currentText(String expected) throws JsonLfDecoder.Error {
    String text = null;
    try {
      text = parser.getText();
    } catch (IOException e) {
      parseExpected(expected, e, "nothing", locationStart());
    }
    return text == null ? "nothing" : text;
  }

  void parseExpected(String expected) throws JsonLfDecoder.Error {
    parseExpected(expected, null);
  }

  void parseExpected(String expected, Throwable cause) throws JsonLfDecoder.Error {
    parseExpected(expected, cause, currentText(expected), locationStart());
  }

  void parseExpected(String expected, Throwable cause, String actual, Location location)
      throws JsonLfDecoder.Error {
    String message = String.format("Expected %s but was %s", expected, actual);
    throw new JsonLfDecoder.Error(message, location, cause);
  }

  void expectIsAt(String description, JsonToken... expected) throws JsonLfDecoder.Error {
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
}
