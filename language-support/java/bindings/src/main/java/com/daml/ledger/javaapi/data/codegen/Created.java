// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.javaapi.data.codegen;

import com.daml.ledger.javaapi.data.CreatedEvent;
import java.util.function.Function;

public final class Created<CtId> {
  public final CtId contractId;

  private Created(CtId contractId) {
    this.contractId = contractId;
  }

  public static <CtId> Created<CtId> fromEvent(
      Function<String, CtId> createdContractId, CreatedEvent createdEvent) {
    return new Created<>(createdContractId.apply(createdEvent.getContractId()));
  }
}
