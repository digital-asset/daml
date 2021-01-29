// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.javaapi.data;

import java.math.BigDecimal;
import org.checkerframework.checker.nullness.qual.NonNull;

@Deprecated
public class Decimal extends Numeric {
  public Decimal(@NonNull BigDecimal value) {
    super(value);
  }
}
