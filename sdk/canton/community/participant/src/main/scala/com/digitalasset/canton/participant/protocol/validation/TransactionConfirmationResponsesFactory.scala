// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.protocol.validation

import cats.syntax.parallel.*
import com.daml.nonempty.NonEmpty
import com.digitalasset.canton.error.TransactionError
import com.digitalasset.canton.lifecycle.FutureUnlessShutdown
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.participant.protocol.ProcessingSteps
import com.digitalasset.canton.participant.protocol.ProtocolProcessor.MalformedPayload
import com.digitalasset.canton.protocol.*
import com.digitalasset.canton.protocol.messages.*
import com.digitalasset.canton.topology.client.TopologySnapshot
import com.digitalasset.canton.topology.{ParticipantId, PhysicalSynchronizerId}
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.{LfPartyId, checked}

import scala.concurrent.ExecutionContext

class TransactionConfirmationResponsesFactory(
    participantId: ParticipantId,
    synchronizerId: PhysicalSynchronizerId,
    protected val loggerFactory: NamedLoggerFactory,
) extends NamedLogging {

  import com.digitalasset.canton.util.ShowUtil.*

  private val protocolVersion = synchronizerId.protocolVersion

  /** Takes a `transactionValidationResult` and computes the
    * [[protocol.messages.ConfirmationResponses]], to be sent to the mediator.
    */
  def createConfirmationResponses(
      requestId: RequestId,
      malformedPayloads: Seq[MalformedPayload],
      transactionValidationResult: TransactionValidationResult,
      topologySnapshot: TopologySnapshot,
  )(implicit
      traceContext: TraceContext,
      ec: ExecutionContext,
  ): FutureUnlessShutdown[Option[ConfirmationResponses]] = {

    def hostedConfirmingPartiesOfView(
        viewValidationResult: ViewValidationResult
    ): FutureUnlessShutdown[Set[LfPartyId]] = {
      val confirmingParties =
        viewValidationResult.view.viewCommonData.viewConfirmationParameters.confirmers
      topologySnapshot.canConfirm(participantId, confirmingParties)
    }

    def verdictsForView(
        viewValidationResult: ViewValidationResult,
        hostedConfirmingParties: Set[LfPartyId],
    )(implicit
        traceContext: TraceContext
    ): Option[LocalVerdict] = {
      val viewHash = viewValidationResult.view.unwrap.viewHash

      val ViewActivenessResult(
        inactive,
        alreadyLocked,
        existing,
      ) =
        viewValidationResult.activenessResult

      if (inactive.nonEmpty)
        logger.info(
          show"View $viewHash of request $requestId rejected due to inactive contract(s) $inactive"
        )
      if (alreadyLocked.nonEmpty)
        logger.info(
          show"View $viewHash of request $requestId rejected due to contention on contract(s) $alreadyLocked"
        )

      if (hostedConfirmingParties.isEmpty) {
        // The participant does not host a confirming party.
        // Therefore, no rejection needs to be computed.
        None
      } else if (existing.nonEmpty) {
        // The transaction would recreate existing contracts. Reject.
        Some(
          logged(
            requestId,
            LocalRejectError.MalformedRejects.CreatesExistingContracts
              .Reject(existing.toSeq.map(_.coid)),
          ).toLocalReject(protocolVersion)
        )
      } else {
        def stakeholderOfUsedContractIsHostedConfirmingParty(coid: LfContractId): Boolean =
          viewValidationResult.view.viewParticipantData.coreInputs
            .get(coid)
            .exists(_.stakeholders.intersect(hostedConfirmingParties).nonEmpty)

        // All informees are stakeholders of created contracts in the core of the view.
        // It therefore suffices to deal with input contracts by stakeholder.
        val createdAbsolute =
          viewValidationResult.view.viewParticipantData.createdCore
            .map(_.contract.contractId)
            .toSet
        val lockedForActivation = alreadyLocked intersect createdAbsolute

        val inactiveInputs = inactive.filter(stakeholderOfUsedContractIsHostedConfirmingParty)
        val lockedInputs = alreadyLocked.filter(stakeholderOfUsedContractIsHostedConfirmingParty)

        if (inactiveInputs.nonEmpty) {
          // The transaction uses an inactive contract. Reject.
          Some(
            logged(
              requestId,
              LocalRejectError.ConsistencyRejections.InactiveContracts
                .Reject(inactiveInputs.toSeq.map(_.coid)),
            ).toLocalReject(protocolVersion)
          )
        } else if (lockedInputs.nonEmpty | lockedForActivation.nonEmpty) {
          // The transaction would create / use a locked contract. Reject.
          val allLocked = lockedForActivation ++ lockedInputs
          Some(
            logged(
              requestId,
              LocalRejectError.ConsistencyRejections.LockedContracts
                .Reject(allLocked.toSeq.map(_.coid)),
            ).toLocalReject(protocolVersion)
          )
        } else {
          // Everything ok from the perspective of conflict detection.
          None
        }
      }
    }

    def responsesForWellformedPayloads(
        transactionValidationResult: TransactionValidationResult
    ): FutureUnlessShutdown[Option[ConfirmationResponses]] = {
      for {
        modelConformanceResultE <- transactionValidationResult.modelConformanceResultET.value

        // Rejections due to a failed model conformance check
        // Aborts are logged by the Engine callback when the abort happens
        modelConformanceRejections =
          modelConformanceResultE.swap.toSeq.flatMap(error =>
            error.nonAbortErrors.map(cause =>
              logged(
                requestId,
                LocalRejectError.MalformedRejects.ModelConformance.Reject(cause.toString),
              ).toLocalReject(protocolVersion)
            )
          )

        responses <- transactionValidationResult.viewValidationResults.toSeq
          .parTraverseFilter { case (viewPosition, viewValidationResult) =>
            for {
              hostedConfirmingParties <-
                hostedConfirmingPartiesOfView(viewValidationResult)

            } yield {

              // Rejections due to a failed internal consistency check
              val internalConsistencyRejections =
                transactionValidationResult.internalConsistencyResultE.swap.toOption.map(cause =>
                  logged(
                    requestId,
                    LocalRejectError.MalformedRejects.ModelConformance.Reject(cause.toString),
                  ).toLocalReject(protocolVersion)
                )

              // Rejections due to a failed authentication check
              val authenticationRejections =
                transactionValidationResult.authenticationResult
                  .get(viewPosition)
                  .map(err =>
                    logged(
                      requestId,
                      LocalRejectError.MalformedRejects.MalformedRequest.Reject(err.message),
                    ).toLocalReject(protocolVersion)
                  )

              // Rejections due to a transaction detected as a replay
              val replayRejections =
                transactionValidationResult.replayCheckResult
                  .map(err =>
                    logged(
                      requestId,
                      // Conceptually, a normal LocalReject for the admin party should suffice for rejecting replays.
                      // However, we nevertheless use a `Malformed` rejection here so that the rejection preference sorting
                      // ensures that this rejection or something at least as strong will make it to the mediator.
                      LocalRejectError.MalformedRejects.MalformedRequest.Reject(
                        err.format(viewPosition)
                      ),
                    ).toLocalReject(protocolVersion)
                  )

              // Rejections due to a failed authorization check
              val authorizationRejections =
                transactionValidationResult.authorizationResult
                  .get(viewPosition)
                  .map(cause =>
                    logged(
                      requestId,
                      LocalRejectError.MalformedRejects.MalformedRequest.Reject(cause),
                    ).toLocalReject(protocolVersion)
                  )

              // Rejections due to a failed time validation
              val timeValidationRejections =
                transactionValidationResult.timeValidationResultE.swap.toOption
                  .map {
                    case TimeValidator.LedgerTimeRecordTimeDeltaTooLargeError(
                          ledgerTime,
                          recordTime,
                          maxDelta,
                        ) =>
                      LocalRejectError.TimeRejects.LedgerTime.Reject(
                        s"ledgerTime=$ledgerTime, recordTime=$recordTime, maxDelta=$maxDelta"
                      )
                    case TimeValidator.PreparationTimeRecordTimeDeltaTooLargeError(
                          preparationTime,
                          recordTime,
                          maxDelta,
                        ) =>
                      LocalRejectError.TimeRejects.PreparationTime.Reject(
                        s"preparationTime=$preparationTime, recordTime=$recordTime, maxDelta=$maxDelta"
                      )
                  }
                  .map(logged(requestId, _))
                  .map(_.toLocalReject(protocolVersion))

              val contractConsistencyRejections =
                transactionValidationResult.contractConsistencyResultE.swap.toOption.map(err =>
                  logged(
                    requestId,
                    LocalRejectError.MalformedRejects.MalformedRequest.Reject(err.toString),
                  ).toLocalReject(protocolVersion)
                )

              // Approve if the consistency check succeeded, reject otherwise.
              val consistencyVerdicts =
                verdictsForView(viewValidationResult, hostedConfirmingParties)

              val localVerdicts: Seq[LocalVerdict] =
                consistencyVerdicts.toList ++ timeValidationRejections ++ contractConsistencyRejections ++
                  authenticationRejections ++ authorizationRejections ++
                  modelConformanceRejections ++ internalConsistencyRejections ++
                  replayRejections

              val localVerdictAndPartiesO = localVerdicts
                .collectFirst[(LocalVerdict, Set[LfPartyId])] {
                  case malformed: LocalReject if malformed.isMalformed => malformed -> Set.empty
                  case localReject: LocalReject if hostedConfirmingParties.nonEmpty =>
                    localReject -> hostedConfirmingParties
                }
                .orElse(
                  Option.when(hostedConfirmingParties.nonEmpty)(
                    LocalApprove(protocolVersion) -> hostedConfirmingParties
                  )
                )

              localVerdictAndPartiesO.map { case (localVerdict, parties) =>
                checked(
                  ConfirmationResponse
                    .tryCreate(
                      Some(viewPosition),
                      localVerdict,
                      parties,
                    )
                )
              }
            }
          }
      } yield {
        checked(
          NonEmpty
            .from(responses)
            .map(
              ConfirmationResponses
                .tryCreate(
                  requestId,
                  transactionValidationResult.updateId.toRootHash,
                  synchronizerId,
                  participantId,
                  _,
                  protocolVersion,
                )
            )
        )
      }
    }

    if (malformedPayloads.nonEmpty) {
      FutureUnlessShutdown.pure(
        ProcessingSteps.constructResponsesForMalformedPayloads(
          requestId = requestId,
          rootHash = transactionValidationResult.updateId.toRootHash,
          malformedPayloads = malformedPayloads,
          synchronizerId = synchronizerId,
          participantId = participantId,
          protocolVersion = protocolVersion,
        )
      )
    } else {
      responsesForWellformedPayloads(transactionValidationResult)
    }
  }

  private def logged[T <: TransactionError](requestId: RequestId, err: T)(implicit
      traceContext: TraceContext
  ): T = {
    err.logWithContext(Map("requestId" -> requestId.toString))
    err
  }

}
