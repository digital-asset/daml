// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.javaapi.data.codegen;

import java.util.Objects;
import java.util.Optional;
import java.util.Set;

public abstract class ContractWithKey<Id, Data, Key> extends Contract<Id, Data> {
  /** The contract's key, if it was present in the event. */
  public final Optional<Key> key;

  protected ContractWithKey(
      Id id,
      Data data,
      Optional<String> agreementText,
      Optional<Key> key,
      Set<String> signatories,
      Set<String> observers) {
    super(id, data, agreementText, signatories, observers);
    this.key = key;
  }

  @Override
  public final boolean equals(Object object) {
    return object instanceof ContractWithKey
        && super.equals(object)
        && this.key.equals(((ContractWithKey<?, ?, ?>) object).key);
  }

  @Override
  public final int hashCode() {
    return Objects.hash(
        this.id, this.data, this.agreementText, this.key, this.signatories, this.observers);
  }

  @Override
  public final String toString() {
    return String.format(
        "Contract(%s, %s, %s, %s, %s, %s)",
        this.id,
        this.data,
        this.agreementText,
        this.key,
        this.signatories,
        this.observers);
  }
}
