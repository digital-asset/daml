// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates.
// Proprietary code. All rights reserved.

package com.daml.ledger.javaapi.data.codegen;

import com.daml.ledger.javaapi.data.CreatedEvent;
import java.util.function.Function;

/**
 * This class contains information related to a result after a contract is created.
 *
 * <p>Application code <em>should not</em> instantiate or subclass;
 *
 * @param <CtId> The type of the template {@code ContractId}.
 */
public final class Created<CtId> {
  public final CtId contractId;

  private Created(CtId contractId) {
    this.contractId = contractId;
  }

  /** @hidden */
  public static <CtId> Created<CtId> fromEvent(
      Function<String, CtId> createdContractId, CreatedEvent createdEvent) {
    return new Created<>(createdContractId.apply(createdEvent.getContractId()));
  }

  @Override
  public String toString() {
    return "Created{" + "contractId=" + contractId + '}';
  }
}
