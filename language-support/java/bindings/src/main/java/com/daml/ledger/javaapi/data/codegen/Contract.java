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
  /** The contract ID retrieved from the event. */
  public final Id id;

  /** The contract payload, as declared after {@code template X with}. */
  public final Data data;

  /** If defined, the contract's agreement text. */
  public final Optional<String> agreementText;

  /** The party IDs of this contract's signatories. */
  public final Set<String> signatories;

  /** The party IDs of this contract's observers. */
  public final Set<String> observers;

  /**
   * <strong>INTERNAL API</strong>: this is meant for use by <a
   * href="https://docs.daml.com/app-dev/bindings-java/codegen.html">the Java code generator</a>,
   * and <em>should not be referenced directly</em>. Applications should refer to the constructors
   * of code-generated subclasses, or {@link ContractCompanion#fromCreatedEvent}, instead.
   */
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
        "Contract(%s, %s, %s, %s, %s)",
        this.id, this.data, this.agreementText, this.signatories, this.observers);
  }
}
