// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.javaapi.data.codegen;

import java.util.Objects;
import java.util.Optional;
import java.util.Set;

/**
 * A superclass for all codegen-generated Contracts.
 *
 * @param <Id> The generated contract ID class alongside the generated Contract class.
 * @param <Data> The containing template's associated record type.
 */
public abstract class Contract<Id, Data> implements com.daml.ledger.javaapi.data.Contract {
  public final Id id;

  public final Data data;

  public final Optional<String> agreementText;

  public final Set<String> signatories;

  public final Set<String> observers;

  protected Contract(
      Id id,
      Data data,
      Optional<String> agreementText,
      Set<String> signatories,
      Set<String> observers) {
    this.id = id;
    this.data = data;
    this.agreementText = agreementText;
    this.signatories = signatories;
    this.observers = observers;
  }

  // concrete 1st type param would need a self-reference type param in Contract
  protected abstract ContractCompanion<? extends Contract<Id, Data>, Id, Data> getCompanion();

  @Override
  public boolean equals(Object object) {
    if (this == object) {
      return true;
    }
    if (object == null) {
      return false;
    }
    if (!(object instanceof Contract)) {
      return false;
    }
    Contract<?, ?> other = (Contract<?, ?>) object;
    // a bespoke Contract-specific equals is unneeded, as 'data' here
    // already compares the associated record types' classes
    return this.id.equals(other.id)
        && this.data.equals(other.data)
        && this.agreementText.equals(other.agreementText)
        && this.signatories.equals(other.signatories)
        && this.observers.equals(other.observers);
  }

  @Override
  public int hashCode() {
    return Objects.hash(this.id, this.data, this.agreementText, this.signatories, this.observers);
  }

  @Override
  public String toString() {
    return String.format(
        "%s.Contract(%s, %s, %s, %s, %s)",
        getCompanion().templateClassName,
        this.id,
        this.data,
        this.agreementText,
        this.signatories,
        this.observers);
  }
}
