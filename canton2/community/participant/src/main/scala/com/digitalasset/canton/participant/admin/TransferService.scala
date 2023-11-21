// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.admin

import cats.data.EitherT
import com.digitalasset.canton.data.{CantonTimestamp, TransferSubmitterMetadata}
import com.digitalasset.canton.lifecycle.FutureUnlessShutdown
import com.digitalasset.canton.participant.protocol.transfer.{
  TransferData,
  TransferSubmissionHandle,
}
import com.digitalasset.canton.participant.store.TransferLookup
import com.digitalasset.canton.protocol.{LfContractId, SourceDomainId, TargetDomainId, TransferId}
import com.digitalasset.canton.topology.DomainId
import com.digitalasset.canton.tracing.{TraceContext, Traced}
import com.digitalasset.canton.version.ProtocolVersion
import com.digitalasset.canton.version.Transfer.{SourceProtocolVersion, TargetProtocolVersion}
import com.digitalasset.canton.{DomainAlias, LfPartyId}

import scala.concurrent.{ExecutionContext, Future}

class TransferService(
    domainIdOfAlias: DomainAlias => Option[DomainId],
    submissionHandles: DomainId => Option[TransferSubmissionHandle],
    transferLookups: TargetDomainId => Option[TransferLookup],
    protocolVersionFor: Traced[DomainId] => Option[ProtocolVersion],
)(implicit ec: ExecutionContext) {

  private[admin] def transferOut(
      submitterMetadata: TransferSubmitterMetadata,
      contractId: LfContractId,
      sourceDomain: DomainAlias,
      targetDomain: DomainAlias,
  )(implicit traceContext: TraceContext): EitherT[Future, String, TransferId] =
    for {
      submissionHandle <- EitherT.fromEither[Future](submissionHandleFor(sourceDomain))
      targetDomainId <- EitherT.fromEither[Future](domainIdFor(targetDomain)).map(TargetDomainId(_))

      rawTargetProtocolVersion <- EitherT.fromEither[Future](
        protocolVersionFor(targetDomainId.unwrap, "target")
      )
      targetProtocolVersion = TargetProtocolVersion(rawTargetProtocolVersion)

      transferId <- submissionHandle
        .submitTransferOut(
          submitterMetadata,
          contractId,
          targetDomainId,
          targetProtocolVersion,
        )
        .mapK(FutureUnlessShutdown.outcomeK)
        .semiflatMap(Predef.identity)
        .leftMap(_.toString)
        .onShutdown(Left("Application is shutting down"))
        .biflatMap(
          error => EitherT.leftT[Future, TransferId](error),
          result =>
            EitherT(
              result.transferOutCompletionF.map(status =>
                Either.cond(
                  status.code == com.google.rpc.Code.OK_VALUE,
                  result.transferId,
                  s"Transfer-out failed with status $status",
                )
              )
            ),
        )
    } yield transferId

  private def protocolVersionFor(domain: DomainId, kind: String)(implicit
      traceContext: TraceContext
  ): Either[String, ProtocolVersion] =
    protocolVersionFor(Traced(domain))
      .toRight(s"Unable to get protocol version of $kind domain")

  def transferIn(
      submitterMetadata: TransferSubmitterMetadata,
      targetDomain: DomainAlias,
      transferId: TransferId,
  )(implicit
      traceContext: TraceContext
  ): EitherT[Future, String, Unit] =
    for {
      submisisonHandle <- EitherT.fromEither[Future](submissionHandleFor(targetDomain))
      rawSourceProtocolVersion <- EitherT.fromEither[Future](
        protocolVersionFor(transferId.sourceDomain.unwrap, "source")
      )
      sourceProtocolVersion = SourceProtocolVersion(rawSourceProtocolVersion)
      result <- submisisonHandle
        .submitTransferIn(
          submitterMetadata,
          transferId,
          sourceProtocolVersion,
        )
        .mapK(FutureUnlessShutdown.outcomeK)
        .semiflatMap(Predef.identity)
        .leftMap(_.toString)
        .onShutdown(Left("Application is shutting down"))
      _ <- EitherT(
        result.transferInCompletionF.map(status =>
          Either.cond(
            status.code == com.google.rpc.Code.OK_VALUE,
            (),
            s"Transfer-in failed with status $status. ID: $transferId",
          )
        )
      )
    } yield ()

  def transferSearch(
      targetDomainAlias: DomainAlias,
      filterSourceDomainAlias: Option[DomainAlias],
      filterTimestamp: Option[CantonTimestamp],
      filterSubmitter: Option[LfPartyId],
      limit: Int,
  )(implicit traceContext: TraceContext): EitherT[Future, String, Seq[TransferData]] = {
    for {
      rawTargetDomain <- EitherT.fromEither[Future](domainIdFor(targetDomainAlias))
      targetDomain = TargetDomainId(rawTargetDomain)

      transferLookup <- EitherT.fromEither[Future](
        transferLookups(targetDomain).toRight(s"Unknown domain alias $targetDomainAlias")
      )

      filterDomain <- EitherT.fromEither[Future](filterSourceDomainAlias match {
        case None => Right(None)
        case Some(alias) =>
          domainIdOfAlias(alias)
            .toRight(s"Unknown domain alias `$alias`")
            .map(id => Some(SourceDomainId(id)))
      })
      result <- EitherT.liftF(
        transferLookup.find(filterDomain, filterTimestamp, filterSubmitter, limit)
      )
    } yield result
  }

  private[this] def domainIdFor(alias: DomainAlias): Either[String, DomainId] =
    domainIdOfAlias(alias).toRight(s"Unknown domain alias $alias")

  private[this] def submissionHandleFor(
      alias: DomainAlias
  ): Either[String, TransferSubmissionHandle] =
    (for {
      domainId <- domainIdOfAlias(alias)
      sync <- submissionHandles(domainId)
    } yield sync).toRight(s"Unknown domain alias $alias")
}
