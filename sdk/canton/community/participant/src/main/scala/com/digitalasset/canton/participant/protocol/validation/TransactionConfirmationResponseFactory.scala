// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.protocol.validation

import cats.syntax.parallel.*
import com.digitalasset.canton.error.TransactionError
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.participant.protocol.ProtocolProcessor.MalformedPayload
import com.digitalasset.canton.protocol.*
import com.digitalasset.canton.protocol.messages.*
import com.digitalasset.canton.topology.client.TopologySnapshot
import com.digitalasset.canton.topology.{DomainId, ParticipantId}
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.util.FutureInstances.*
import com.digitalasset.canton.version.ProtocolVersion
import com.digitalasset.canton.{LfPartyId, checked}

import scala.concurrent.{ExecutionContext, Future}

class TransactionConfirmationResponseFactory(
    participantId: ParticipantId,
    domainId: DomainId,
    protocolVersion: ProtocolVersion,
    protected val loggerFactory: NamedLoggerFactory,
) extends NamedLogging {

  import com.digitalasset.canton.util.ShowUtil.*

  /** Takes a `transactionValidationResult` and computes the [[protocol.messages.ConfirmationResponse]], to be sent to the mediator.
    */
  def createConfirmationResponses(
      requestId: RequestId,
      malformedPayloads: Seq[MalformedPayload],
      transactionValidationResult: TransactionValidationResult,
      topologySnapshot: TopologySnapshot,
  )(implicit
      traceContext: TraceContext,
      ec: ExecutionContext,
  ): Future[Seq[ConfirmationResponse]] = {

    def hostedConfirmingPartiesOfView(
        viewValidationResult: ViewValidationResult
    ): Future[Set[LfPartyId]] = {
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
        logger.debug(
          show"View $viewHash of request $requestId rejected due to inactive contract(s) $inactive"
        )
      if (alreadyLocked.nonEmpty)
        logger.debug(
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
    ): Future[Seq[ConfirmationResponse]] =
      transactionValidationResult.viewValidationResults.toSeq.parTraverseFilter {
        case (viewPosition, viewValidationResult) =>
          for {
            hostedConfirmingParties <- hostedConfirmingPartiesOfView(viewValidationResult)
            modelConformanceResultE <- transactionValidationResult.modelConformanceResultET.value
          } yield {

            // Rejections due to a failed model conformance check
            // Aborts are logged by the Engine callback when the abort happens
            val modelConformanceRejections =
              modelConformanceResultE.swap.toSeq.flatMap(error =>
                error.nonAbortErrors.map(cause =>
                  logged(
                    requestId,
                    LocalRejectError.MalformedRejects.ModelConformance.Reject(cause.toString),
                  ).toLocalReject(protocolVersion)
                )
              )

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
                    LocalRejectError.MalformedRejects.MalformedRequest.Reject(err),
                  ).toLocalReject(protocolVersion)
                )

            // Rejections due to a transaction detected as a replay
            val replayRejections =
              transactionValidationResult.replayCheckResult
                .map(err =>
                  logged(
                    requestId,
                    // TODO(i13513): Check whether a `Malformed` code is appropriate
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
                  case TimeValidator.SubmissionTimeRecordTimeDeltaTooLargeError(
                        submissionTime,
                        recordTime,
                        maxDelta,
                      ) =>
                    LocalRejectError.TimeRejects.SubmissionTime.Reject(
                      s"submissionTime=$submissionTime, recordTime=$recordTime, maxDelta=$maxDelta"
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
            val consistencyVerdicts = verdictsForView(viewValidationResult, hostedConfirmingParties)

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
                    requestId,
                    participantId,
                    Some(viewPosition),
                    localVerdict,
                    transactionValidationResult.transactionId.toRootHash,
                    parties,
                    domainId,
                    protocolVersion,
                  )
              )
            }
          }
      }

    if (malformedPayloads.nonEmpty) {
      Future.successful(
        Seq(
          createConfirmationResponsesForMalformedPayloads(
            requestId,
            transactionValidationResult.transactionId.toRootHash,
            malformedPayloads,
          )
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

  def createConfirmationResponsesForMalformedPayloads(
      requestId: RequestId,
      rootHash: RootHash,
      malformedPayloads: Seq[MalformedPayload],
  )(implicit traceContext: TraceContext): ConfirmationResponse =
    checked(
      ConfirmationResponse
        .tryCreate(
          requestId,
          participantId,
          // We don't have to specify a viewPosition.
          // The mediator will interpret this as a rejection
          // for all views and on behalf of all declared confirming parties hosted by the participant.
          None,
          logged(
            requestId,
            LocalRejectError.MalformedRejects.Payloads
              .Reject(malformedPayloads.toString),
          ).toLocalReject(protocolVersion),
          rootHash,
          Set.empty,
          domainId,
          protocolVersion,
        )
    )
}
