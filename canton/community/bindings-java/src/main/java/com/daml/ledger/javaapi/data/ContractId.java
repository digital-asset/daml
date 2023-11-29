// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates.
// Proprietary code. All rights reserved.

package com.daml.ledger.javaapi.data;

import com.daml.ledger.api.v1.ValueOuterClass;
import java.util.Objects;

public final class ContractId extends Value {

  private final String value;

  public ContractId(String value) {
    this.value = value;
  }

  public String getValue() {
    return value;
  }

  @Override
  public ValueOuterClass.Value toProto() {
    return ValueOuterClass.Value.newBuilder().setContractId(this.value).build();
  }

  @Override
  public String toString() {
    return "ContractId{" + "value='" + value + '\'' + '}';
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    ContractId that = (ContractId) o;
    return Objects.equals(value, that.value);
  }

  @Override
  public int hashCode() {

    return Objects.hash(value);
  }
}
