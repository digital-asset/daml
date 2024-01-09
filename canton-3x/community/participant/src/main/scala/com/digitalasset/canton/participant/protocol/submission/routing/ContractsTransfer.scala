// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.protocol.submission.routing

import cats.data.EitherT
import cats.syntax.parallel.*
import com.digitalasset.canton.data.TransferSubmitterMetadata
import com.digitalasset.canton.ledger.participant.state.v2.SubmitterInfo
import com.digitalasset.canton.lifecycle.FutureUnlessShutdown
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.participant.protocol.submission.routing.ContractsTransfer.TransferArgs
import com.digitalasset.canton.participant.sync.TransactionRoutingError.AutomaticTransferForTransactionFailure
import com.digitalasset.canton.participant.sync.{ConnectedDomainsLookup, TransactionRoutingError}
import com.digitalasset.canton.protocol.*
import com.digitalasset.canton.topology.ParticipantId
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.util.FutureInstances.*
import com.digitalasset.canton.version.Transfer.{SourceProtocolVersion, TargetProtocolVersion}

import scala.concurrent.{ExecutionContext, Future}

private[routing] class ContractsTransfer(
    connectedDomains: ConnectedDomainsLookup,
    submittingParticipant: ParticipantId,
    protected val loggerFactory: NamedLoggerFactory,
)(implicit ec: ExecutionContext)
    extends NamedLogging {
  def transfer(
      domainRankTarget: DomainRank,
      submitterInfo: SubmitterInfo,
  )(implicit traceContext: TraceContext): EitherT[Future, TransactionRoutingError, Unit] = {
    if (domainRankTarget.transfers.nonEmpty) {
      logger.info(
        s"Automatic transaction transfer into domain ${domainRankTarget.domainId}"
      )
      domainRankTarget.transfers.toSeq.parTraverse_ { case (cid, (lfParty, sourceDomainId)) =>
        perform(
          TransferArgs(
            SourceDomainId(sourceDomainId),
            TargetDomainId(domainRankTarget.domainId),
            TransferSubmitterMetadata(
              lfParty,
              submitterInfo.applicationId,
              submittingParticipant.toLf,
              submitterInfo.commandId,
              submitterInfo.submissionId,
              workflowId = None,
            ),
            cid,
            traceContext,
          )
        )
      }
    } else {
      EitherT.pure[Future, TransactionRoutingError](())
    }
  }

  private def perform(
      args: TransferArgs
  ): EitherT[Future, TransactionRoutingError, Unit] = {
    val TransferArgs(
      sourceDomain,
      targetDomain,
      submitterMetadata,
      contractId,
      _traceContext,
    ) = args
    implicit val traceContext = _traceContext

    val transfer = for {
      sourceSyncDomain <- EitherT.fromEither[Future](
        connectedDomains.get(sourceDomain.unwrap).toRight("Not connected to the source domain")
      )

      targetSyncDomain <- EitherT.fromEither[Future](
        connectedDomains.get(targetDomain.unwrap).toRight("Not connected to the target domain")
      )

      _unit <- EitherT
        .cond[Future](sourceSyncDomain.ready, (), "The source domain is not ready for submissions")

      outResult <- sourceSyncDomain
        .submitTransferOut(
          submitterMetadata,
          contractId,
          targetDomain,
          TargetProtocolVersion(targetSyncDomain.staticDomainParameters.protocolVersion),
        )
        .mapK(FutureUnlessShutdown.outcomeK)
        .semiflatMap(Predef.identity)
        .leftMap(_.toString)
        .onShutdown(Left("Application is shutting down"))
      outStatus <- EitherT.right[String](outResult.transferOutCompletionF)
      _outApprove <- EitherT.cond[Future](
        outStatus.code == com.google.rpc.Code.OK_VALUE,
        (),
        s"The transfer out for ${outResult.transferId} failed with status $outStatus",
      )

      _unit <- EitherT
        .cond[Future](targetSyncDomain.ready, (), "The target domain is not ready for submission")

      inResult <- targetSyncDomain
        .submitTransferIn(
          submitterMetadata,
          outResult.transferId,
          SourceProtocolVersion(sourceSyncDomain.staticDomainParameters.protocolVersion),
        )
        .leftMap[String](err => s"Transfer in failed with error ${err}")
        .flatMap { s =>
          EitherT(s.map(Right(_)).onShutdown(Left("Application is shutting down")))
        }

      inStatus <- EitherT.right[String](inResult.transferInCompletionF)
      _inApprove <- EitherT.cond[Future](
        inStatus.code == com.google.rpc.Code.OK_VALUE,
        (),
        s"The transfer in for ${outResult.transferId} failed with verdict $inStatus",
      )
    } yield ()

    transfer.leftMap[TransactionRoutingError](str =>
      AutomaticTransferForTransactionFailure.Failed(str)
    )
  }
}

private[routing] object ContractsTransfer {
  final case class TransferArgs(
      sourceDomain: SourceDomainId,
      targetDomain: TargetDomainId,
      submitterMetadata: TransferSubmitterMetadata,
      contract: LfContractId,
      traceContext: TraceContext,
  )
}
