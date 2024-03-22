// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates.
// Proprietary code. All rights reserved.

package com.daml.ledger.javaapi.data;

import org.checkerframework.checker.nullness.qual.NonNull;

/**
 * This interface represents events in {@link ActiveContract}s.
 *
 * @see ActiveContract
 * @see IncompleteUnassigned
 * @see IncompleteAssigned
 */
public interface ContractEntry {
  @NonNull
  CreatedEvent getCreatedEvent();
}
