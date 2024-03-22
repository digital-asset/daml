// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.javaapi.data;

import com.daml.ledger.api.v1.TransactionFilterOuterClass;
import java.util.Set;

public abstract class TransactionFilter {

  public static TransactionFilter fromProto(
      TransactionFilterOuterClass.TransactionFilter transactionFilter) {
    // at the moment, the only transaction filter supported is FiltersByParty
    return FiltersByParty.fromProto(transactionFilter);
  }

  abstract TransactionFilterOuterClass.TransactionFilter toProto();

  public abstract Set<String> getParties();
}
