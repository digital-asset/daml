// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates.
// Proprietary code. All rights reserved.

package com.daml.ledger.javaapi.data.codegen;

import com.daml.ledger.javaapi.data.Identifier;
import java.util.Objects;
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

  /** The party IDs of this contract's signatories. */
  public final Set<String> signatories;

  /** The party IDs of this contract's observers. */
  public final Set<String> observers;

  /**
   * <strong>INTERNAL API</strong>: this is meant for use by <a
   * href="https://docs.daml.com/app-dev/bindings-java/codegen.html">the Java code generator</a>,
   * and <em>should not be referenced directly</em>. Applications should refer to the constructors
   * of code-generated subclasses, or {@link ContractCompanion#fromCreatedEvent}, instead.
   *
   * @hidden
   */
  protected Contract(Id id, Data data, Set<String> signatories, Set<String> observers) {
    this.id = id;
    this.data = data;
    this.signatories = signatories;
    this.observers = observers;
  }

  /** The template or interface ID for this contract or interface view. */
  public final Identifier getContractTypeId() {
    return getCompanion().TEMPLATE_ID;
  }

  // concrete 3rd type param would need a self-reference type param in Contract
  /**
   * <strong>INTERNAL API</strong>: this is meant for use by {@link Contract}, and <em>should not be
   * referenced directly</em>. Applications should refer to other methods like {@link
   * #getContractTypeId} instead.
   *
   * @hidden
   */
  protected abstract ContractTypeCompanion<? extends Contract<?, Data>, Id, ?, Data> getCompanion();

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
        && this.signatories.equals(other.signatories)
        && this.observers.equals(other.observers);
  }

  @Override
  public int hashCode() {
    return Objects.hash(this.id, this.data, this.signatories, this.observers);
  }

  @Override
  public String toString() {
    return String.format(
        "%s.Contract(%s, %s, %s, %s)",
        getCompanion().TEMPLATE_CLASS_NAME, this.id, this.data, this.signatories, this.observers);
  }
}
