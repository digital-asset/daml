// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.protocol.reassignment

import cats.data.EitherT
import com.digitalasset.canton.ReassignmentCounter
import com.digitalasset.canton.lifecycle.FutureUnlessShutdown
import com.digitalasset.canton.participant.protocol.conflictdetection.ActivenessResult
import com.digitalasset.canton.participant.protocol.validation.AuthenticationError
import com.digitalasset.canton.protocol.{LfContractId, RootHash}

trait ReassignmentValidationResult {
  def rootHash: RootHash
  def reassignmentCounter: ReassignmentCounter
  def contractId: LfContractId
  def activenessResult: ActivenessResult
  def authenticationErrorO: Option[AuthenticationError]
  def metadataResultET: EitherT[FutureUnlessShutdown, ReassignmentValidationError, Unit]
  def validationErrors: Seq[ReassignmentValidationError]
  def isUnassignment: Boolean
  def isReassigningParticipant: Boolean
}
