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

class ConfirmationResponseFactory(
    participantId: ParticipantId,
    domainId: DomainId,
    protocolVersion: ProtocolVersion,
    protected val loggerFactory: NamedLoggerFactory,
) extends NamedLogging {

  import com.digitalasset.canton.util.ShowUtil.*

  private val verdictProtocolVersion =
    LocalVerdict.protocolVersionRepresentativeFor(protocolVersion)

  def createConfirmationResponses(
      requestId: RequestId,
      malformedPayloads: Seq[MalformedPayload],
      transactionValidationResult: TransactionValidationResult,
      topologySnapshot: TopologySnapshot,
  )(implicit traceContext: TraceContext, ec: ExecutionContext): Future[Seq[MediatorResponse]] = {

    def hostedConfirmingPartiesOfView(
        viewValidationResult: ViewValidationResult
    ): Future[Set[LfPartyId]] = {
      viewValidationResult.view.viewCommonData.viewConfirmationParameters.confirmers.toList
        .parTraverseFilter { cp =>
          topologySnapshot
            .canConfirm(participantId, cp.party, cp.requiredTrustLevel)
            .map(if (_) Some(cp.party) else None)
        }
        .map(_.toSet)
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
        duplicateKeys,
        inconsistentKeys,
        alreadyLockedKeys,
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
      if (duplicateKeys.nonEmpty)
        logger.debug(
          show"View $viewHash of request $requestId rejected due to duplicate keys $duplicateKeys"
        )
      if (inconsistentKeys.nonEmpty)
        logger.debug(
          show"View $viewHash of request $requestId rejected due to inconsistent keys $inconsistentKeys"
        )
      if (alreadyLockedKeys.nonEmpty)
        logger.debug(
          show"View $viewHash of request $requestId rejected due to contention on key(s) $alreadyLockedKeys"
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
            LocalReject.MalformedRejects.CreatesExistingContracts
              .Reject(existing.toSeq.map(_.coid))(verdictProtocolVersion),
          )
        )
      } else {
        def stakeholderOfUsedContractIsHostedConfirmingParty(coid: LfContractId): Boolean =
          viewValidationResult.view.viewParticipantData.coreInputs
            .get(coid)
            .exists(_.stakeholders.intersect(hostedConfirmingParties).nonEmpty)

        def extractKeysWithMaintainerBeingHostedConfirmingParty(
            keysWithMaintainers: Map[LfGlobalKey, Set[LfPartyId]]
        ): Seq[LfGlobalKey] = keysWithMaintainers.collect {
          case (key, maintainers) if maintainers.intersect(hostedConfirmingParties).nonEmpty => key
        }.toSeq

        // All informees are stakeholders of created contracts in the core of the view.
        // It therefore suffices to deal with input contracts by stakeholder.
        val createdAbsolute =
          viewValidationResult.view.viewParticipantData.createdCore
            .map(_.contract.contractId)
            .toSet
        val lockedForActivation = alreadyLocked intersect createdAbsolute

        val inactiveInputs = inactive.filter(stakeholderOfUsedContractIsHostedConfirmingParty)
        val lockedInputs = alreadyLocked.filter(stakeholderOfUsedContractIsHostedConfirmingParty)
        val lockedKeys = extractKeysWithMaintainerBeingHostedConfirmingParty(alreadyLockedKeys)
        val duplicateKeysForParty = extractKeysWithMaintainerBeingHostedConfirmingParty(
          duplicateKeys
        )
        val inconsistentKeysForParty = extractKeysWithMaintainerBeingHostedConfirmingParty(
          inconsistentKeys
        )

        if (inactiveInputs.nonEmpty) {
          // The transaction uses an inactive contract. Reject.
          Some(
            LocalReject.ConsistencyRejections.InactiveContracts
              .Reject(inactiveInputs.toSeq.map(_.coid))(verdictProtocolVersion)
          )
        } else if (duplicateKeysForParty.nonEmpty) {
          // The transaction would assign several contracts to the same key. Reject.
          Some(
            LocalReject.ConsistencyRejections.DuplicateKey
              .Reject(duplicateKeysForParty.map(_.toString()))(verdictProtocolVersion)
          )
        } else if (inconsistentKeysForParty.nonEmpty) {
          // The key lookups / creations / exercise by key have inconsistent key resolutions. Reject.
          Some(
            LocalReject.ConsistencyRejections.InconsistentKey
              .Reject(inconsistentKeysForParty.map(_.toString()))(verdictProtocolVersion)
          )
        } else if (lockedInputs.nonEmpty | lockedForActivation.nonEmpty) {
          // The transaction would create / use a locked contract. Reject.
          val allLocked = lockedForActivation ++ lockedInputs
          Some(
            LocalReject.ConsistencyRejections.LockedContracts
              .Reject(allLocked.toSeq.map(_.coid))(verdictProtocolVersion)
          )
        } else if (lockedKeys.nonEmpty) {
          // The transaction would resolve a locked key. Reject.
          Some(
            LocalReject.ConsistencyRejections.LockedKeys
              .Reject(lockedKeys.map(_.toString()))(verdictProtocolVersion)
          )
        } else {
          // Everything ok from the perspective of conflict detection.
          None
        }
      }
    }

    def responsesForWellformedPayloads(
        transactionValidationResult: TransactionValidationResult
    ): Future[Seq[MediatorResponse]] =
      transactionValidationResult.viewValidationResults.toSeq.parTraverseFilter {
        case (viewPosition, viewValidationResult) =>
          for {
            hostedConfirmingParties <- hostedConfirmingPartiesOfView(viewValidationResult)
          } yield {

            // Rejections due to a failed model conformance check
            val modelConformanceRejections =
              transactionValidationResult.modelConformanceResultE.swap.toSeq.flatMap(error =>
                error.errors.map(cause =>
                  logged(
                    requestId,
                    LocalReject.MalformedRejects.ModelConformance.Reject(cause.toString)(
                      verdictProtocolVersion
                    ),
                  )
                )
              )

            // Rejections due to a failed internal consistency check
            val internalConsistencyRejections =
              transactionValidationResult.internalConsistencyResultE.swap.toOption.map(cause =>
                logged(
                  requestId,
                  LocalReject.MalformedRejects.ModelConformance.Reject(cause.toString)(
                    verdictProtocolVersion
                  ),
                )
              )

            // Rejections due to a failed authentication check
            val authenticationRejections =
              transactionValidationResult.authenticationResult
                .get(viewPosition)
                .map(err =>
                  logged(
                    requestId,
                    LocalReject.MalformedRejects.MalformedRequest.Reject(err)(
                      verdictProtocolVersion
                    ),
                  )
                )

            // Rejections due to a transaction detected as a replay
            val replayRejections =
              transactionValidationResult.replayCheckResult
                .map(err =>
                  logged(
                    requestId,
                    // TODO(i13513): Check whether a `Malformed` code is appropriate
                    LocalReject.MalformedRejects.MalformedRequest.Reject(err.format(viewPosition))(
                      verdictProtocolVersion
                    ),
                  )
                )

            // Rejections due to a failed authorization check
            val authorizationRejections =
              transactionValidationResult.authorizationResult
                .get(viewPosition)
                .map(cause =>
                  logged(
                    requestId,
                    LocalReject.MalformedRejects.MalformedRequest.Reject(cause)(
                      verdictProtocolVersion
                    ),
                  )
                )

            // Rejections due to a failed time validation
            val timeValidationRejections =
              transactionValidationResult.timeValidationResultE.swap.toOption.map {
                case TimeValidator.LedgerTimeRecordTimeDeltaTooLargeError(
                      ledgerTime,
                      recordTime,
                      maxDelta,
                    ) =>
                  LocalReject.TimeRejects.LedgerTime.Reject(
                    s"ledgerTime=$ledgerTime, recordTime=$recordTime, maxDelta=$maxDelta"
                  )(verdictProtocolVersion)
                case TimeValidator.SubmissionTimeRecordTimeDeltaTooLargeError(
                      submissionTime,
                      recordTime,
                      maxDelta,
                    ) =>
                  LocalReject.TimeRejects.SubmissionTime.Reject(
                    s"submissionTime=$submissionTime, recordTime=$recordTime, maxDelta=$maxDelta"
                  )(verdictProtocolVersion)
              }

            val contractConsistencyRejections =
              transactionValidationResult.contractConsistencyResultE.swap.toOption.map { err =>
                LocalReject.MalformedRejects.MalformedRequest.Reject(err.toString, protocolVersion)
              }

            // Approve if the consistency check succeeded, reject otherwise.
            val consistencyVerdicts = verdictsForView(viewValidationResult, hostedConfirmingParties)

            val localVerdicts: Seq[LocalVerdict] =
              consistencyVerdicts.toList ++ timeValidationRejections ++ contractConsistencyRejections ++
                authenticationRejections ++ authorizationRejections ++
                modelConformanceRejections ++ internalConsistencyRejections ++
                replayRejections

            val localVerdictAndPartiesO = localVerdicts
              .collectFirst[(LocalVerdict, Set[LfPartyId])] {
                case malformed: Malformed => malformed -> Set.empty
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
                MediatorResponse
                  .tryCreate(
                    requestId,
                    participantId,
                    Some(viewValidationResult.view.view.viewHash),
                    Some(viewPosition),
                    localVerdict,
                    Some(transactionValidationResult.transactionId.toRootHash),
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
      malformedPayloads: Seq[MalformedPayload],
  )(implicit traceContext: TraceContext): MediatorResponse =
    checked(
      MediatorResponse
        .tryCreate(
          requestId,
          participantId,
          // We don't have to specify a viewHash or viewPosition.
          // The mediator will interpret this as a rejection
          // for all views and on behalf of all declared confirming parties hosted by the participant.
          None,
          None,
          logged(
            requestId,
            LocalReject.MalformedRejects.Payloads
              .Reject(malformedPayloads.toString)(verdictProtocolVersion),
          ),
          None,
          Set.empty,
          domainId,
          protocolVersion,
        )
    )
}
