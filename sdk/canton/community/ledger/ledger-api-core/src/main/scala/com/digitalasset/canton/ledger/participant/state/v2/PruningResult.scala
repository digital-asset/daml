// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.ledger.participant.state.v2

import com.google.rpc.Status

sealed trait PruningResult extends Product with Serializable

object PruningResult {

  /** Pruning has been performed. Commits the ledger api server to prune at the same offset as passed to the
    * WriteService.
    */
  case object ParticipantPruned extends PruningResult

  /** Pruning was not performed. Indicates to ledger api server not to proceed with pruning either.
    *
    * @param grpcStatus grpcStatus created using error codes API (see [[com.daml.error.ErrorCode]]).
    *                    Examples of gRPC status codes specific to pruning:
    *
    *                   OUT_OF_RANGE: If the specified offset cannot be pruned at, but will eventually be possible to
    *                   prune at without user intervention.
    *
    *                   FAILED_PRECONDITION: If the specified offset cannot be pruned at, and enabling pruning at the
    *                   offset requires user intervention (such as submitting a command via the submission service).
    *
    *                   INTERNAL: If a severe error has been encountered, particularly indicating that pruning has only
    *                   been partially applied.
    */
  final case class NotPruned(grpcStatus: Status) extends PruningResult

}
