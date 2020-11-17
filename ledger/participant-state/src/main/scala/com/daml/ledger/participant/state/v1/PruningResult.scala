// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.participant.state.v1

import io.grpc.Status

sealed trait PruningResult extends Product with Serializable

/**
  * Pruning has been performed. Indicates readiness to prune the ledger api server at the same offset passed to the
  * WriteService.
  */
case object ParticipantPruned extends PruningResult

/**
  * Pruning was not performed. Indicates to ledger api server not to proceed with pruning either.
  * @param grpcStatus grpcStatus to return as a reason.
  */
final case class NotPruned(grpcStatus: Status) extends PruningResult
