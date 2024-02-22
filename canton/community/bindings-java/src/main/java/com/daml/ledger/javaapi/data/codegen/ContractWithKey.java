// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates.
// Proprietary code. All rights reserved.

package com.daml.ledger.javaapi.data.codegen;

import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import com.daml.ledger.javaapi.data.codegen.json.JsonLfEncoder;

/**
 * A superclass for all codegen-generated Contracts whose templates have a {@code key} defined.
 *
 * @param <Id> The generated contract ID class alongside the generated Contract class.
 * @param <Data> The containing template's associated record type.
 * @param <Key> The template's key type.
 */
public abstract class ContractWithKey<Id, Data, Key> extends Contract<Id, Data> {
  /** The contract's key, if it was present in the event. */
  public final Optional<Key> key;

  /**
   * <strong>INTERNAL API</strong>: this is meant for use by <a
   * href="https://docs.daml.com/app-dev/bindings-java/codegen.html">the Java code generator</a>,
   * and <em>should not be referenced directly</em>. Applications should refer to the constructors
   * of code-generated subclasses, or {@link ContractCompanion#fromCreatedEvent}, instead.
   *
   * @hidden
   */
  protected ContractWithKey(
      Id id, Data data, Optional<Key> key, Set<String> signatories, Set<String> observers) {
    super(id, data, signatories, observers);
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
    return Objects.hash(this.id, this.data, this.key, this.signatories, this.observers);
  }

  @Override
  public final String toString() {
    return String.format(
        "%s.Contract(%s, %s, %s, %s, %s)",
        getCompanion().TEMPLATE_CLASS_NAME,
        this.id,
        this.data,
        this.key,
        this.signatories,
        this.observers);
  }

  // Returns an encoder for the key if present, or null otherwise.
  public abstract JsonLfEncoder keyJsonEncoder();

  public String keyToJson() {
    JsonLfEncoder enc = keyJsonEncoder();
    if (enc == null) return null;
    return enc.intoString();
  }
}
