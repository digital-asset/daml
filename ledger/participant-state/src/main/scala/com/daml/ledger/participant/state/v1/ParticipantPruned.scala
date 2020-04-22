// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.participant.state.v1

/**
  * Describe ledger participants agreement to pruning participant state.
  *
  * @param prunedUpToInclusiveOffset Identifies the offset up to which the participant ledger has pruned
  *                                  ledger transactions inclusively and also signifies request to prune ledger api
  *                                  server index up to specified offset inclusively.
  * @param stateRetainedUntilOffset Optional additional offset (can only be specified along along with
  *                                 prunedUpToInclusiveOffset), in case the participant ledger has chosen to retain
  *                                 additional state (no visible via the ReadService) necessary in order to preserve
  *                                 properties such as auditability and non-repudiation.
  */
final case class ParticipantPruned(
    prunedUpToInclusiveOffset: Offset,
    stateRetainedUntilOffset: Option[Offset])
