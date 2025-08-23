// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.protocol.reassignment

import cats.data.EitherT
import com.digitalasset.canton.LfPartyId
import com.digitalasset.canton.data.ContractsReassignmentBatch
import com.digitalasset.canton.lifecycle.FutureUnlessShutdown
import com.digitalasset.canton.participant.protocol.conflictdetection.ActivenessResult
import com.digitalasset.canton.participant.protocol.reassignment.ReassignmentValidationResult.{
  CommonValidationResult,
  ReassigningParticipantValidationResult,
}
import com.digitalasset.canton.participant.protocol.validation.AuthenticationError
import com.digitalasset.canton.protocol.{ReassignmentId, RootHash}
import com.google.common.annotations.VisibleForTesting

import scala.concurrent.ExecutionContext

/** Represents the result of validating an unassignment or assignment request on a participant.
  * hostedConfirmingReassigningParties: This is an empty set if the participant is not a reassigning
  * participant. Otherwise, it represents the set of confirming parties currently hosted on this
  * participant that have at least confirmation rights.
  */
private[reassignment] trait ReassignmentValidationResult {
  def reassignmentId: ReassignmentId
  def rootHash: RootHash
  def contracts: ContractsReassignmentBatch
  def hostedConfirmingReassigningParties: Set[LfPartyId]
  def isReassigningParticipant: Boolean
  def commonValidationResult: CommonValidationResult
  def reassigningParticipantValidationResult: ReassigningParticipantValidationResult
  def activenessResultIsSuccessful: Boolean

  @VisibleForTesting
  def isSuccessful(implicit ec: ExecutionContext): FutureUnlessShutdown[Boolean] =
    for {
      contractAuthenticationResult <- commonValidationResult.contractAuthenticationResultF.value
    } yield activenessResultIsSuccessful &&
      commonValidationResult.participantSignatureVerificationResult.isEmpty &&
      reassigningParticipantValidationResult.errors.isEmpty &&
      contractAuthenticationResult.isRight &&
      commonValidationResult.submitterCheckResult.isEmpty &&
      commonValidationResult.reassignmentIdResult.isEmpty

}

private[reassignment] object ReassignmentValidationResult {

  /** When adding a new validation result:
    *
    *   - Update the `isSuccessful` method in `ReassignmentValidationResult`.
    *   - Adapt the `responsesForWellformedPayloads` method, which is responsible for creating the
    *     confirmation response in Phase 3.
    *   - Consider whether the validation should also be checked during Phase 7 and, if so, update
    *     the `checkPhase7Validations` method accordingly.
    */
  private[reassignment] trait CommonValidationResult {
    def activenessResult: ActivenessResult
    def participantSignatureVerificationResult: Option[AuthenticationError]
    def contractAuthenticationResultF
        : EitherT[FutureUnlessShutdown, ReassignmentValidationError, Unit]
    def submitterCheckResult: Option[ReassignmentValidationError]
    def reassignmentIdResult: Option[ReassignmentValidationError]
  }

  private[reassignment] trait ReassigningParticipantValidationResult {
    def errors: Seq[ReassignmentValidationError]
  }
}
