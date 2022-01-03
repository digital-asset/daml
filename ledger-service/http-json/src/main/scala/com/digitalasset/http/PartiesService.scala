// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.http

import com.daml.lf.data.Ref
import com.daml.http.EndpointsCompanion.{Error, InvalidUserInput, Unauthorized}
import com.daml.http.util.FutureUtil._
import com.daml.scalautil.nonempty._
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

  def allParties(jwt: Jwt): Future[Error \/ List[domain.PartyDetails]] =
    listAllParties(jwt).map(
      _ bimap (handleGrpcError, (_ map domain.PartyDetails.fromLedgerApi))
    )

  def parties(
      jwt: Jwt,
      identifiers: domain.PartySet,
  ): Future[Error \/ (Set[domain.PartyDetails], Set[domain.Party])] = {
    val et: ET[(Set[domain.PartyDetails], Set[domain.Party])] = for {
      apiPartyIds <- either(toLedgerApiPartySet(identifiers)): ET[OneAnd[Set, Ref.Party]]
      apiPartyDetails <- eitherT(getParties(jwt, apiPartyIds))
        .leftMap(handleGrpcError): ET[List[api.domain.PartyDetails]]
      domainPartyDetails = apiPartyDetails.iterator
        .map(domain.PartyDetails.fromLedgerApi)
        .toSet: Set[domain.PartyDetails]
    } yield (domainPartyDetails, findUnknownParties(domainPartyDetails, identifiers))

    et.run
  }

  private def findUnknownParties(
      found: Set[domain.PartyDetails],
      requested: domain.PartySet,
  ): Set[domain.Party] =
    if (found.size == requested.size) Set.empty
    else requested -- found.map(_.identifier)
}

object PartiesService {
  import com.daml.http.util.ErrorOps._

  private type ET[A] = EitherT[Future, Error, A]

  private def handleGrpcError(e: Grpc.Error[Grpc.Category.PermissionDenied]): Error =
    Unauthorized(e.message)

  def toLedgerApiPartySet(
      ps: domain.PartySet
  ): InvalidUserInput \/ OneAnd[Set, Ref.Party] = {
    import scalaz.std.list._
    val enel: InvalidUserInput \/ NonEmptyF[List, Ref.Party] = ps.toList.toF traverse toLedgerApi
    enel.map { case x +-: xs => OneAnd(x, xs.toSet) }
  }

  def toLedgerApi(p: domain.Party): InvalidUserInput \/ Ref.Party =
    \/.fromEither(Ref.Party.fromString(domain.Party.unwrap(p)))
      .liftErrS("PartiesService.toLedgerApi")(InvalidUserInput)

}
