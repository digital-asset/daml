// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.javaapi.data.codegen;

import com.daml.ledger.javaapi.data.*;
import java.time.Instant;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.IntStream;

import com.daml.ledger.javaapi.data.DamlRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static java.util.Collections.unmodifiableList;

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
      ValueDecoder.create(
          (value, policy) -> value.asBool().orElseThrow(() -> mismatched(Bool.class)).getValue());
  public static final ValueDecoder<Long> fromInt64 =
      ValueDecoder.create(
          (value, policy) -> value.asInt64().orElseThrow(() -> mismatched(Int64.class)).getValue());
  public static final ValueDecoder<String> fromText =
      ValueDecoder.create(
          (value, policy) -> value.asText().orElseThrow(() -> mismatched(Text.class)).getValue());
  public static final ValueDecoder<Instant> fromTimestamp =
      ValueDecoder.create(
          (value, policy) ->
              value.asTimestamp().orElseThrow(() -> mismatched(Timestamp.class)).getValue());
  public static final ValueDecoder<String> fromParty =
      ValueDecoder.create(
          (value, policy) -> value.asParty().orElseThrow(() -> mismatched(Party.class)).getValue());
  public static final ValueDecoder<Unit> fromUnit =
      ValueDecoder.create(
          (value, policy) -> value.asUnit().orElseThrow(() -> mismatched(Unit.class)));
  public static final ValueDecoder<LocalDate> fromDate =
      ValueDecoder.create(
          (value, policy) -> value.asDate().orElseThrow(() -> mismatched(Date.class)).getValue());
  public static final ValueDecoder<java.math.BigDecimal> fromNumeric =
      ValueDecoder.create(
          (value, policy) ->
              value.asNumeric().orElseThrow(() -> mismatched(Numeric.class)).getValue());

  public static <T> ValueDecoder<List<T>> fromList(ValueDecoder<T> element) {
    return ValueDecoder.create(
        (value, policy) ->
            value
                .asList()
                .orElseThrow(() -> mismatched(List.class))
                .toList(currentValue -> element.decode(currentValue, policy)));
  }

  public static <T> ValueDecoder<Optional<T>> fromOptional(ValueDecoder<T> element) {
    return ValueDecoder.create(
        (value, policy) ->
            value
                .asOptional()
                .orElseThrow(() -> mismatched(Optional.class))
                .toOptional(currentValue -> element.decode(currentValue, policy)));
  }

  public static <T> ValueDecoder<ContractId<T>> fromContractId(ValueDecoder<T> contractType) {
    return ValueDecoder.create(
        (value, policy) ->
            contractType.fromContractId(
                value.asContractId().orElseThrow(() -> mismatched(ContractId.class)).getValue()));
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
    return ValueDecoder.create(
        (value, policy) ->
            value
                .asTextMap()
                .orElseThrow(() -> mismatched(Map.class))
                .toMap(currentValue -> valueType.decode(currentValue, policy)));
  }

  /**
   * Specifically for decoding the {@code DA.Internal.LF.Map} primitive type.
   *
   * @see #fromTextMap
   */
  public static <K, V> ValueDecoder<Map<K, V>> fromGenMap(
      ValueDecoder<K> keyType, ValueDecoder<V> valueType) {
    return ValueDecoder.create(
        (value, policy) ->
            value
                .asGenMap()
                .orElseThrow(() -> mismatched(Map.class))
                .toMap(
                    curentValue -> keyType.decode(curentValue, policy),
                    curentValue -> valueType.decode(curentValue, policy)));
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
  public static List<DamlRecord.Field> recordCheck(
      int expectedFields, int trailingOptionalFields, Value maybeRecord) {
    return checkAndPrepareRecord(
            expectedFields, trailingOptionalFields, maybeRecord, UnknownTrailingFieldPolicy.STRICT)
        .getExpectedFields();
  }

  /**
   * <strong>INTERNAL API</strong>: this is meant for use by <a
   * href="https://docs.daml.com/app-dev/bindings-java/codegen.html">the Java code generator</a>,
   * and <em>should not be referenced directly</em>. Applications should use a code-generated {@code
   * valueDecoder} method instead.
   *
   * @hidden
   */
  public static PreparedRecord checkAndPrepareRecord(
      int expectedFields,
      int trailingOptionalFields,
      Value maybeRecord,
      UnknownTrailingFieldPolicy policy) {
    DamlRecord record =
        maybeRecord
            .asRecord()
            .orElseThrow(
                () -> new IllegalArgumentException("Contracts must be constructed from Records"));
    List<DamlRecord.Field> fields = record.getFields();
    if (fields.size() == expectedFields) {
      return new PreparedRecord(unmodifiableList(fields));
    }
    int receivedFieldsCount = fields.size();
    if (receivedFieldsCount > expectedFields) {
      List<DamlRecord.Field> expectedFieldsList =
          unmodifiableList(fields.subList(0, expectedFields));
      List<DamlRecord.Field> unknownFields =
          readUnknownFields(fields, expectedFields, receivedFieldsCount);
      logger.trace("Found {} trailing expectedFields, policy is {}", unknownFields.size(), policy);
      return prepareRecordOrFail(expectedFieldsList, unknownFields, policy);
    } else if (receivedFieldsCount >= expectedFields - trailingOptionalFields) {
      int missingFieldsCount = expectedFields - receivedFieldsCount;
      logger.trace("Upgrading record, appending {} empty optional fields", missingFieldsCount);
      return new PreparedRecord(combineFieldsWithTrailingOptionals(fields, missingFieldsCount));
    }
    throw new IllegalArgumentException(
        String.format(
            "Expected %d fields, got %d and only the last %d of the expected type are optionals",
            expectedFields, receivedFieldsCount, trailingOptionalFields));
  }

  private static PreparedRecord prepareRecordOrFail(
      List<DamlRecord.Field> expectedFields,
      List<DamlRecord.Field> unknownFields,
      UnknownTrailingFieldPolicy policy) {
    if (!unknownFields.isEmpty() && UnknownTrailingFieldPolicy.STRICT == policy) {
      throw new IllegalArgumentException(
          String.format(
              "Unexpected non-empty %d fields were received and Strict policy is used",
              unknownFields.size()));
    }
    return new PreparedRecord(expectedFields);
  }

  private static List<DamlRecord.Field> combineFieldsWithTrailingOptionals(
      List<DamlRecord.Field> fields, int missingFieldsCount) {
    List<DamlRecord.Field> newFields = new ArrayList<>(fields.size() + missingFieldsCount);
    newFields.addAll(fields);
    for (int i = 0; i < missingFieldsCount; i++) {
      newFields.add(new DamlRecord.Field(DamlOptional.EMPTY));
    }
    return unmodifiableList(newFields);
  }

  private static List<DamlRecord.Field> readUnknownFields(
      List<DamlRecord.Field> fields, int expectedFields, int receivedFieldsCount) {
    List<DamlRecord.Field> unknownFields = fields.subList(expectedFields, receivedFieldsCount);
    if (unknownFields.isEmpty()) {
      return List.of();
    }
    for (int i = 0; i < unknownFields.size(); i++) {
      if (unknownFields.get(i).getValue().asOptional().isEmpty()) {
        throw new IllegalArgumentException(
            String.format(
                "Expected %d fields, got %d and field %d is not optional: %s",
                receivedFieldsCount, expectedFields, i, fields.get(i)));
      }
    }
    final int n = unknownFields.size();
    final int idxFromEnd =
        IntStream.range(0, n)
            .filter(i -> !unknownFields.get(n - 1 - i).getValue().asOptional().get().isEmpty())
            .findFirst()
            .orElse(-1);
    if (idxFromEnd == -1) {
      return List.of();
    } else {
      return unmodifiableList(unknownFields.subList(0, n - idxFromEnd));
    }
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
