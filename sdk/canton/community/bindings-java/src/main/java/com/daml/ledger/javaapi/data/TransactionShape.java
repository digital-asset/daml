// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.javaapi.data;

import com.daml.ledger.api.v2.TransactionFilterOuterClass;

public enum TransactionShape {
  ACS_DELTA,
  LEDGER_EFFECTS;

  public TransactionFilterOuterClass.TransactionShape toProto() {
    return this == ACS_DELTA
        ? TransactionFilterOuterClass.TransactionShape.TRANSACTION_SHAPE_ACS_DELTA
        : TransactionFilterOuterClass.TransactionShape.TRANSACTION_SHAPE_LEDGER_EFFECTS;
  }

  static TransactionShape fromProto(TransactionFilterOuterClass.TransactionShape proto) {
    if (proto == TransactionFilterOuterClass.TransactionShape.TRANSACTION_SHAPE_ACS_DELTA)
      return ACS_DELTA;
    if (proto == TransactionFilterOuterClass.TransactionShape.TRANSACTION_SHAPE_LEDGER_EFFECTS)
      return LEDGER_EFFECTS;
    throw new IllegalArgumentException(
        "Proto TransactionShape "
            + proto
            + " cannot be converted to javaapi.data.TransactionShape");
  }
}
