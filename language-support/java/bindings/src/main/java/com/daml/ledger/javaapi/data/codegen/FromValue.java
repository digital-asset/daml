// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.javaapi.data.codegen;

import com.daml.ledger.javaapi.data.Value;

@FunctionalInterface
public interface FromValue<Data> {
  Data fromValue(Value value);

  default ContractId<Data> fromContractId(String contractId) {
    throw new IllegalArgumentException("Cannot create contract id for this data type");
  }
}
