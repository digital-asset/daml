// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.javaapi.data;

import com.daml.ledger.api.v1.TransactionFilterOuterClass;

public final class NoFilter extends Filter {

  public static final NoFilter instance = new NoFilter();

  private NoFilter() {}

  @Override
  public TransactionFilterOuterClass.Filters toProto() {
    return TransactionFilterOuterClass.Filters.getDefaultInstance();
  }
}
