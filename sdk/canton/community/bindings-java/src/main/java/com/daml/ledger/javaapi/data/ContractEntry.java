// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

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
