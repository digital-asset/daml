// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.javaapi.data;

import com.daml.ledger.api.v1.ValueOuterClass;
import java.util.Objects;
import java.util.Optional;
import org.checkerframework.checker.nullness.qual.NonNull;

public final class DamlEnum extends Value {

  private final Optional<Identifier> enumId;

  private final String constructor;

  public DamlEnum(@NonNull Identifier enumId, @NonNull String constructor) {
    this.enumId = Optional.of(enumId);
    this.constructor = constructor;
  }

  public DamlEnum(@NonNull String constructor) {
    this.enumId = Optional.empty();
    this.constructor = constructor;
  }

  public static DamlEnum fromProto(ValueOuterClass.Enum value) {
    String constructor = value.getConstructor();
    if (value.hasEnumId()) {
      Identifier variantId = Identifier.fromProto(value.getEnumId());
      return new DamlEnum(variantId, constructor);
    } else {
      return new DamlEnum(constructor);
    }
  }

  @NonNull
  public Optional<Identifier> getEnumId() {
    return enumId;
  }

  @NonNull
  public String getConstructor() {
    return constructor;
  }

  @Override
  public ValueOuterClass.Value toProto() {
    return ValueOuterClass.Value.newBuilder().setEnum(this.toProtoEnum()).build();
  }

  public ValueOuterClass.Enum toProtoEnum() {
    ValueOuterClass.Enum.Builder builder = ValueOuterClass.Enum.newBuilder();
    builder.setConstructor(this.getConstructor());
    this.getEnumId().ifPresent(identifier -> builder.setEnumId(identifier.toProto()));
    return builder.build();
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    DamlEnum value = (DamlEnum) o;
    return Objects.equals(enumId, value.enumId) && Objects.equals(constructor, value.constructor);
  }

  @Override
  public int hashCode() {
    return Objects.hash(enumId, constructor);
  }

  @Override
  public String toString() {
    return "Enum{" + "variantId=" + enumId + ", constructor='" + constructor + "'}";
  }
}
