// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.protocol.submission.routing

import cats.data.EitherT
import cats.syntax.either.*
import cats.syntax.traverse.*
import com.daml.lf.data.Ref.Party
import com.daml.lf.engine.Blinding
import com.digitalasset.canton.ledger.participant.state.v2.SubmitterInfo
import com.digitalasset.canton.participant.sync.TransactionRoutingError
import com.digitalasset.canton.participant.sync.TransactionRoutingError.MalformedInputErrors
import com.digitalasset.canton.participant.sync.TransactionRoutingError.MalformedInputErrors.{
  InvalidDomainAlias,
  InvalidDomainId,
}
import com.digitalasset.canton.protocol.{LfContractId, LfVersionedTransaction}
import com.digitalasset.canton.topology.DomainId
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.{DomainAlias, LfPackageId, LfPartyId, LfWorkflowId}

import scala.concurrent.{ExecutionContext, Future}

/** Bundle together some data needed to route the transaction.
  *
  * @param requiredPackagesPerParty Required packages per informee of the transaction
  * @param submitters Submitters of the transaction.
  * @param inputContractsDomainData Information about the input contracts
  * @param prescribedDomainO If non-empty, thInvalidWorkflowIde prescribed domain will be chosen for routing.
  *                          In case this domain is not admissible, submission will fail.
  */
private[routing] final case class TransactionData private (
    transaction: LfVersionedTransaction,
    requiredPackagesPerParty: Map[LfPartyId, Set[LfPackageId]],
    submitters: Set[LfPartyId],
    inputContractsDomainData: ContractsDomainData,
    prescribedDomainO: Option[DomainId],
) {
  val informees: Set[LfPartyId] = requiredPackagesPerParty.keySet
  val version = transaction.version
}

private[routing] object TransactionData {
  def create(
      submitters: Set[LfPartyId],
      transaction: LfVersionedTransaction,
      workflowIdO: Option[LfWorkflowId],
      domainStateProvider: DomainStateProvider,
      domainIdResolver: DomainAlias => Option[DomainId],
      contractRoutingParties: Map[LfContractId, Set[Party]],
      submitterDomainId: Option[DomainId],
  )(implicit
      ec: ExecutionContext,
      traceContext: TraceContext,
  ): EitherT[Future, TransactionRoutingError, TransactionData] = {
    for {
      prescribedDomainO <- EitherT.fromEither[Future](
        submitterDomainId
          .map(domainId => Right(Some(domainId))) // submitter domain takes precedence
          .getOrElse(toDomainId(workflowIdO, domainIdResolver))
      )
      contractsDomainData <- EitherT.liftF(
        ContractsDomainData.create(domainStateProvider, contractRoutingParties)
      )
    } yield TransactionData(
      transaction = transaction,
      requiredPackagesPerParty = Blinding.partyPackages(transaction),
      submitters = submitters,
      inputContractsDomainData = contractsDomainData,
      prescribedDomainO = prescribedDomainO,
    )
  }

  def create(
      submitterInfo: SubmitterInfo,
      transaction: LfVersionedTransaction,
      workflowIdO: Option[LfWorkflowId],
      domainStateProvider: DomainStateProvider,
      domainIdResolver: DomainAlias => Option[DomainId],
      contractRoutingParties: Map[LfContractId, Set[Party]],
      submitterDomainId: Option[DomainId],
  )(implicit
      ec: ExecutionContext,
      traceContext: TraceContext,
  ): EitherT[Future, TransactionRoutingError, TransactionData] = {
    for {
      submitters <- EitherT.fromEither[Future](
        submitterInfo.actAs
          .traverse(submitter =>
            LfPartyId
              .fromString(submitter)
              .leftMap[TransactionRoutingError](MalformedInputErrors.InvalidSubmitter.Error)
          )
          .map(_.toSet)
      )

      transactionData <- create(
        submitters,
        transaction,
        workflowIdO,
        domainStateProvider,
        domainIdResolver,
        contractRoutingParties,
        submitterDomainId,
      )
    } yield transactionData
  }

  private[routing] def toDomainId(
      maybeWorkflowId: Option[LfWorkflowId],
      domainIdResolver: DomainAlias => Option[DomainId],
  ): Either[TransactionRoutingError, Option[DomainId]] =
    maybeWorkflowId match {
      case Some(workflowId) if workflowId contains DomainIdMarker =>
        asDomainId(workflowId).map(Some(_))

      case Some(workflowId) =>
        asDomainAlias(workflowId).map(domainIdResolver)

      case None =>
        Right(None)
    }

  private def asDomainAlias(
      workflowId: LfWorkflowId
  ): Either[TransactionRoutingError, DomainAlias] =
    DomainAlias
      .create(workflowId)
      .leftMap(InvalidDomainAlias.Error)

  private val DomainIdMarker: String = "domain-id:"

  private def asDomainId(
      workflowId: LfWorkflowId
  ): Either[TransactionRoutingError, DomainId] = {
    val markerIndex = workflowId.indexOf(DomainIdMarker)
    DomainId
      .fromString(
        // whole postfix after the marker is the domain id
        workflowId.substring(markerIndex + DomainIdMarker.length)
      )
      .leftMap(InvalidDomainId.Error)
  }
}
