// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates.
// Proprietary code. All rights reserved.

package com.daml.ledger.javaapi.data.codegen;

import com.daml.ledger.javaapi.data.*;
import java.time.Instant;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * {@link ValueDecoder}s for Daml types that are not code-generated.
 *
 * @see ValueDecoder
 */
public final class PrimitiveValueDecoders {

  private static final Logger logger = LoggerFactory.getLogger(PrimitiveValueDecoders.class);

  // constructing not allowed
  private PrimitiveValueDecoders() {}

  public static final ValueDecoder<Boolean> fromBool =
      value -> value.asBool().orElseThrow(() -> mismatched(Bool.class)).getValue();
  public static final ValueDecoder<Long> fromInt64 =
      value -> value.asInt64().orElseThrow(() -> mismatched(Int64.class)).getValue();
  public static final ValueDecoder<String> fromText =
      value -> value.asText().orElseThrow(() -> mismatched(Text.class)).getValue();
  public static final ValueDecoder<Instant> fromTimestamp =
      value -> value.asTimestamp().orElseThrow(() -> mismatched(Timestamp.class)).getValue();
  public static final ValueDecoder<String> fromParty =
      value -> value.asParty().orElseThrow(() -> mismatched(Party.class)).getValue();
  public static final ValueDecoder<Unit> fromUnit =
      value -> value.asUnit().orElseThrow(() -> mismatched(Unit.class));
  public static final ValueDecoder<LocalDate> fromDate =
      value -> value.asDate().orElseThrow(() -> mismatched(Date.class)).getValue();
  public static final ValueDecoder<java.math.BigDecimal> fromNumeric =
      value -> value.asNumeric().orElseThrow().getValue();

  public static <T> ValueDecoder<List<T>> fromList(ValueDecoder<T> element) {
    return value ->
        value.asList().orElseThrow(() -> mismatched(List.class)).toList(element::decode);
  }

  public static <T> ValueDecoder<Optional<T>> fromOptional(ValueDecoder<T> element) {
    return value ->
        value
            .asOptional()
            .orElseThrow(() -> mismatched(Optional.class))
            .toOptional(element::decode);
  }

  public static <T> ValueDecoder<ContractId<T>> fromContractId(ValueDecoder<T> contractType) {
    return value ->
        contractType.fromContractId(
            value.asContractId().orElseThrow(() -> mismatched(ContractId.class)).getValue());
  }

  /**
   * Specifically for decoding the {@code DA.Internal.LF.TextMap} primitive type.
   *
   * <pre>
   * // given a ValueDecoder&lt;V> vdv
   * // this decodes a TextMap
   * Map&lt;String, V> m1 = fromTextMap(vdv).decode(value);
   *
   * // this decodes a GenMap with a Text key type
   * // which has an incompatible encoded format
   * Map&lt;String, V> m2 = fromGenMap(fromText, vdv).decode(value);
   * </pre>
   *
   * @see #fromGenMap
   */
  public static <T> ValueDecoder<Map<String, T>> fromTextMap(ValueDecoder<T> valueType) {
    return value ->
        value.asTextMap().orElseThrow(() -> mismatched(Map.class)).toMap(valueType::decode);
  }

  /**
   * Specifically for decoding the {@code DA.Internal.LF.Map} primitive type.
   *
   * @see #fromTextMap
   */
  public static <K, V> ValueDecoder<Map<K, V>> fromGenMap(
      ValueDecoder<K> keyType, ValueDecoder<V> valueType) {
    return value ->
        value
            .asGenMap()
            .orElseThrow(() -> mismatched(Map.class))
            .toMap(keyType::decode, valueType::decode);
  }

  /**
   * Can be passed to decoder-producing functions to decode <em>phantom type parameters only</em>.
   * Such type parameters appear on the left of the {@code =} in {@code data} declarations, but are
   * used for <em>no actual data</em>. This will fail at runtime if the type parameter isn't really
   * phantom.
   */
  public static <T> ValueDecoder<T> impossible() {
    return x -> {
      throw new IllegalArgumentException("Expected type to be unused, but was used for " + x);
    };
  }

  private static IllegalArgumentException mismatched(Class<?> clazz) {
    String typeName = clazz.getName();
    return new IllegalArgumentException(String.format("Expected field to be of type %s", typeName));
  }

  /**
   * <strong>INTERNAL API</strong>: this is meant for use by <a
   * href="https://docs.daml.com/app-dev/bindings-java/codegen.html">the Java code generator</a>,
   * and <em>should not be referenced directly</em>. Applications should use a code-generated {@code
   * valueDecoder} method instead.
   *
   * @hidden
   */
  public static List<com.daml.ledger.javaapi.data.DamlRecord.Field> recordCheck(
      int expectedFields, int trailingOptionalFields, Value maybeRecord) {
    var record =
        maybeRecord
            .asRecord()
            .orElseThrow(
                () -> new IllegalArgumentException("Contracts must be constructed from Records"));
    var fields = record.getFields();
    var numberOfFields = fields.size();
    if (numberOfFields == expectedFields) {
      return fields;
    }
    if (numberOfFields > expectedFields) {
      // Downgrade, check that the additional fields are empty optionals.
      for (var i = expectedFields; i < numberOfFields; i++) {
        final var field = fields.get(i);
        final var optValue = field.getValue().asOptional();
        if (optValue.isEmpty()) {
          throw new IllegalArgumentException(
              "Expected "
                  + expectedFields
                  + " arguments, got "
                  + numberOfFields
                  + " and field "
                  + i
                  + " is not optional: "
                  + field);
        }
        final var value = optValue.get();
        if (!value.isEmpty()) {
          throw new IllegalArgumentException(
              "Expected "
                  + expectedFields
                  + " arguments, got "
                  + numberOfFields
                  + " and field "
                  + i
                  + " is Optional but not empty: "
                  + field);
        }
      }
      logger.trace(
          "Downgrading record, dropping {} trailing optional fields",
          numberOfFields - expectedFields);
      return fields.subList(0, expectedFields);
    }
    if (numberOfFields < expectedFields) {
      // Upgrade, add empty optionals to the end.
      if ((expectedFields - numberOfFields) <= trailingOptionalFields) {
        final var newFields = new ArrayList<>(fields);
        for (var i = 0; i < expectedFields - numberOfFields; i++) {
          newFields.add(new com.daml.ledger.javaapi.data.DamlRecord.Field(DamlOptional.EMPTY));
        }
        logger.trace(
            "Upgrading record, appending {} empty optional fields",
            expectedFields - numberOfFields);
        return newFields;
      } else {
        throw new IllegalArgumentException(
            "Expected "
                + expectedFields
                + " arguments, got "
                + numberOfFields
                + " and only the last "
                + trailingOptionalFields
                + " of the expected type are optionals");
      }
    }
    return fields;
  }

  /**
   * <strong>INTERNAL API</strong>: this is meant for use by <a
   * href="https://docs.daml.com/app-dev/bindings-java/codegen.html">the Java code generator</a>,
   * and <em>should not be referenced directly</em>. Applications should use a code-generated {@code
   * valueDecoder} method instead.
   *
   * @hidden
   */
  public static Value variantCheck(String expectedConstructor, Value variantMaybe) {
    var variant =
        variantMaybe
            .asVariant()
            .orElseThrow(
                () ->
                    new IllegalArgumentException(
                        "Expected: Variant. Actual: " + variantMaybe.getClass().getName()));
    if (!expectedConstructor.equals(variant.getConstructor()))
      throw new IllegalArgumentException(
          "Invalid constructor. Expected: "
              + expectedConstructor
              + ". Actual: "
              + variant.getConstructor());
    return variant.getValue();
  }
}
