// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.sync

import com.daml.nonempty.NonEmpty
import com.digitalasset.canton.LfPartyId
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.platform.apiserver.execution.AuthorityResolver
import com.digitalasset.canton.topology.DomainId
import com.digitalasset.canton.topology.client.PartyTopologySnapshotClient.AuthorityOfDelegation
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.util.ErrorUtil
import com.digitalasset.canton.util.ShowUtil.*

import scala.concurrent.{ExecutionContext, Future}
import scala.util.control.NonFatal

class CantonAuthorityResolver(
    connectedDomainsLookup: ConnectedDomainsLookup,
    protected val loggerFactory: NamedLoggerFactory,
)(implicit ec: ExecutionContext)
    extends AuthorityResolver
    with NamedLogging {

  private def isEnoughAuthorised(
      authority: AuthorityOfDelegation,
      holdingAuthority: Set[LfPartyId],
  ): Boolean = {
    val authorized = authority.expected.intersect(holdingAuthority).size
    authorized >= authority.threshold.unwrap
  }

  private def checkAuthority(
      holdingAuthority: Set[LfPartyId],
      requestingAuthority: NonEmpty[Set[LfPartyId]],
      authorities: Map[LfPartyId, AuthorityOfDelegation],
  ): AuthorityResolver.AuthorityResponse = {
    val notAuthorized = requestingAuthority.filterNot { party =>
      authorities.get(party).exists(isEnoughAuthorised(_, holdingAuthority))
    }
    if (notAuthorized.isEmpty) AuthorityResolver.AuthorityResponse.Authorized
    else
      AuthorityResolver.AuthorityResponse.MissingAuthorisation(notAuthorized)
  }

  private def checkAuthority(
      holdingAuthority: Set[LfPartyId],
      requestingAuthority: NonEmpty[Set[LfPartyId]],
      domain: SyncDomain,
  )(implicit
      traceContext: TraceContext
  ): Future[AuthorityResolver.AuthorityResponse] =
    domain
      .authorityOfInSnapshotApproximation(requestingAuthority)
      .map(_.response)
      .map(checkAuthority(holdingAuthority, requestingAuthority, _))
      .recoverWith { case NonFatal(e) =>
        logger.debug(show"Failed to request authority in domain ${domain.domainId}", e)
        ErrorUtil.internalErrorAsync(e)
      }

  private def checkAuthorityInDomain(
      holdingAuthority: Set[LfPartyId],
      requestingAuthority: NonEmpty[Set[LfPartyId]],
      domainId: DomainId,
  )(implicit
      traceContext: TraceContext
  ): Future[AuthorityResolver.AuthorityResponse] =
    connectedDomainsLookup
      .get(domainId)
      .map(checkAuthority(holdingAuthority, requestingAuthority, _))
      .getOrElse(
        // TODO(i12742): This should not happen by design, i.e. engine should be acting on top of connected domain
        ErrorUtil.internalErrorAsync(new Exception(show"$domainId is unrecognized"))
      )

  private def lookupAuthorityInConnectedDomains(
      holdingAuthority: Set[LfPartyId],
      requestingAuthority: NonEmpty[Set[LfPartyId]],
  )(implicit
      traceContext: TraceContext
  ): Future[AuthorityResolver.AuthorityResponse] =
    NonEmpty.from(connectedDomainsLookup.snapshot.values.toSeq) match {
      case Some(domains) =>
        val authorityLookups = domains.map(
          checkAuthority(holdingAuthority, requestingAuthority, _)
        )
        Future
          .find[AuthorityResolver.AuthorityResponse](authorityLookups)(
            _ == AuthorityResolver.AuthorityResponse.Authorized
          )
          .flatMap {
            case Some(authorized) => Future.successful(authorized)
            case None => authorityLookups.head1
          }
      case None =>
        // TODO(i12742): This should not happen by design, i.e. engine should be acting on top of connected domain
        ErrorUtil.internalErrorAsync(new Exception("No connected domains"))
    }

  override def resolve(
      request: AuthorityResolver.AuthorityRequest
  )(implicit
      traceContext: TraceContext
  ): Future[AuthorityResolver.AuthorityResponse] =
    NonEmpty.from(request.requesting) match {
      case Some(requestingAuthority) =>
        request.domainId match {
          // TODO(i12742): `domainId` should be mandatory, but is currently missing because there is no Ledger API support.
          case Some(domainId) =>
            checkAuthorityInDomain(request.holding, requestingAuthority, domainId)
          case _ =>
            lookupAuthorityInConnectedDomains(request.holding, requestingAuthority)
        }
      case None =>
        // TODO(i12742): Consider NonEmpty request for the authorisation
        ErrorUtil.internalErrorAsync(
          new Exception("Empty authority requesting parties")
        )
    }
}

object CantonAuthorityResolver {
  def topologyUnawareAuthorityResolver = new AuthorityResolver.TopologyUnawareAuthorityResolver
}
