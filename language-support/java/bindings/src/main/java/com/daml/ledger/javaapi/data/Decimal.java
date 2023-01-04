// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.javaapi.data;

import java.math.BigDecimal;
import org.checkerframework.checker.nullness.qual.NonNull;

// FIXME When removing this after the deprecation period is over, make Numeric final
/** @deprecated Use {@link Numeric} instead. */
@Deprecated
public final class Decimal extends Numeric {
  public Decimal(@NonNull BigDecimal value) {
    super(value);
  }
}
