// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.http

import com.daml.lf.data.Ref
import com.daml.http.EndpointsCompanion.{Error, InvalidUserInput, Unauthorized}
import com.daml.http.util.FutureUtil._
import com.daml.jwt.domain.Jwt
import com.daml.ledger.api
import LedgerClientJwt.Grpc
import scalaz.std.option._
import scalaz.std.scalaFuture._
import scalaz.std.string._
import scalaz.syntax.traverse._
import scalaz.{EitherT, OneAnd, \/}

import scala.concurrent.{ExecutionContext, Future}

class PartiesService(
    listAllParties: LedgerClientJwt.ListKnownParties,
    getParties: LedgerClientJwt.GetParties,
    allocateParty: LedgerClientJwt.AllocateParty,
)(implicit ec: ExecutionContext) {

  import PartiesService._

  def allocate(
      jwt: Jwt,
      request: domain.AllocatePartyRequest,
  ): Future[Error \/ domain.PartyDetails] = {
    val et: ET[domain.PartyDetails] = for {
      idHint <- either(
        request.identifierHint.traverse(toLedgerApi)
      ): ET[Option[Ref.Party]]

      apiParty <- rightT(
        allocateParty(jwt, idHint, request.displayName)
      ): ET[api.domain.PartyDetails]

      domainParty = domain.PartyDetails.fromLedgerApi(apiParty)

    } yield domainParty

    et.run
  }

  def allParties(jwt: Jwt): Future[List[domain.PartyDetails]] = {
    listAllParties(jwt).map(ps => ps.map(p => domain.PartyDetails.fromLedgerApi(p)))
  }

  def parties(
      jwt: Jwt,
      identifiers: OneAnd[Set, domain.Party],
  ): Future[Error \/ (Set[domain.PartyDetails], Set[domain.Party])] = {
    val et: ET[(Set[domain.PartyDetails], Set[domain.Party])] = for {
      apiPartyIds <- either(toLedgerApiPartySet(identifiers)): ET[OneAnd[Set, Ref.Party]]
      apiPartyDetails <- eitherT(getParties(jwt, apiPartyIds)).leftMap {
        e: Grpc.Error[Grpc.Category.PermissionDenied] => Unauthorized(e.message)
      }: ET[List[api.domain.PartyDetails]]
      domainPartyDetails = apiPartyDetails.iterator
        .map(domain.PartyDetails.fromLedgerApi)
        .toSet: Set[domain.PartyDetails]
    } yield (domainPartyDetails, findUnknownParties(domainPartyDetails, identifiers))

    et.run
  }

  private def findUnknownParties(
      found: Set[domain.PartyDetails],
      requested: OneAnd[Set, domain.Party],
  ): Set[domain.Party] = {
    import scalaz.std.iterable._
    import scalaz.syntax.foldable._

    val requestedSet: Set[domain.Party] = requested.toSet

    if (found.size == requestedSet.size) Set.empty
    else requestedSet -- found.map(_.identifier)
  }
}

object PartiesService {
  import com.daml.http.util.ErrorOps._

  private type ET[A] = EitherT[Future, Error, A]

  def toLedgerApiPartySet(
      ps: OneAnd[Set, domain.Party]
  ): InvalidUserInput \/ OneAnd[Set, Ref.Party] = {
    import scalaz.std.list._
    val nel: OneAnd[List, domain.Party] = OneAnd(ps.head, ps.tail.toList)
    val enel: InvalidUserInput \/ OneAnd[List, Ref.Party] = nel.traverse(toLedgerApi)
    enel.map(xs => OneAnd(xs.head, xs.tail.toSet))
  }

  def toLedgerApi(p: domain.Party): InvalidUserInput \/ Ref.Party =
    \/.fromEither(Ref.Party.fromString(domain.Party.unwrap(p)))
      .liftErrS("PartiesService.toLedgerApi")(InvalidUserInput)

}
