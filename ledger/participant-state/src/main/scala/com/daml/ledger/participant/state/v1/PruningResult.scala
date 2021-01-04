// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.participant.state.v1

import io.grpc.Status

sealed trait PruningResult extends Product with Serializable

object PruningResult {

  /**
    * Pruning has been performed. Commits the ledger api server to prune at the same offset as passed to the
    * WriteService.
    */
  case object ParticipantPruned extends PruningResult

  /**
    * Pruning was not performed. Indicates to ledger api server not to proceed with pruning either.
    *
    * @param grpcStatus grpcStatus to return as a reason according to the GRPC guidelines
    *                   (https://grpc.github.io/grpc/core/md_doc_statuscodes.html). Examples specific to pruning:
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
