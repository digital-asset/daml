// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates.
// Proprietary code. All rights reserved.

package com.daml.ledger.javaapi.data;

import org.checkerframework.checker.nullness.qual.NonNull;

/**
 * This interface represents events in {@link ActiveContractV2}s.
 *
 * @see ActiveContractV2
 * @see IncompleteUnassignedV2
 * @see IncompleteAssignedV2
 */
public interface ContractEntryV2 {
  @NonNull
  CreatedEvent getCreatedEvent();
}
