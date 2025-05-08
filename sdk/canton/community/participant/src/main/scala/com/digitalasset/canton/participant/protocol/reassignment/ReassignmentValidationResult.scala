// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.protocol.reassignment

import cats.data.EitherT
import com.digitalasset.canton.data.ContractsReassignmentBatch
import com.digitalasset.canton.lifecycle.FutureUnlessShutdown
import com.digitalasset.canton.participant.protocol.conflictdetection.ActivenessResult
import com.digitalasset.canton.participant.protocol.validation.AuthenticationError
import com.digitalasset.canton.protocol.{ReassignmentId, RootHash}

trait ReassignmentValidationResult {
  def reassignmentId: ReassignmentId
  def rootHash: RootHash
  def contracts: ContractsReassignmentBatch
  def activenessResult: ActivenessResult
  def participantSignatureVerificationResult: Option[AuthenticationError]
  def contractAuthenticationResultF
      : EitherT[FutureUnlessShutdown, ReassignmentValidationError, Unit]
  def submitterCheckResult: Option[ReassignmentValidationError]
  def reassigningParticipantValidationResult: Seq[ReassignmentValidationError]
  def isUnassignment: Boolean
  def isReassigningParticipant: Boolean
  def activenessResultIsSuccessful: Boolean = activenessResult.isSuccessful
}
