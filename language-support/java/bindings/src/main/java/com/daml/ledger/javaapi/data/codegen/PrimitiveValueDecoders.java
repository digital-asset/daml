// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.javaapi.data.codegen;

import com.daml.ledger.javaapi.data.*;
import java.time.Instant;
import java.time.LocalDate;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;

/**
 * {@link ValueDecoder}s for Daml types that are not code-generated.
 *
 * @see ValueDecoder
 */
public final class PrimitiveValueDecoders {
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

  // TODO CL error context String field
  private static IllegalArgumentException mismatched(Class<?> clazz) {
    String typeName = clazz.getName();
    return new IllegalArgumentException(String.format("Expected field to be of type %s", typeName));
  }

  /**
   *
   * @hidden
   */
  public static List<com.daml.ledger.javaapi.data.DamlRecord.Field> recordCheck(int expectedFields, Value maybeRecord) {
    var record = maybeRecord.asRecord().orElseThrow(() -> new IllegalArgumentException("Contracts must be constructed from Records"));
    var fields = record.getFields();
    var numberOfFields = fields.size();
    if (numberOfFields != expectedFields)
      throw new IllegalArgumentException("Expected " + expectedFields + " arguments, got " + numberOfFields);
    return fields;
  }

  /**
   *
   * @hidden
   */
  public static Value variantCheck(String expectedConstructor, Value variantMaybe) {
    var variant = variantMaybe.asVariant().orElseThrow(() -> new IllegalArgumentException("Expected: Variant. Actual: " + variantMaybe.getClass().getName()));
    if (!expectedConstructor.equals(variant.getConstructor()))
      throw new IllegalArgumentException("Invalid constructor. Expected: " + expectedConstructor +  ". Actual: " + variant.getConstructor());
    return variant.getValue();
  }
}
