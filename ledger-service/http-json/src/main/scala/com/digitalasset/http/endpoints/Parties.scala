// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.http
package endpoints

import akka.http.scaladsl.model._
import Endpoints.ET
import com.daml.jwt.domain.Jwt
import util.Collections.toNonEmptySet
import util.FutureUtil.{either, eitherT}
import util.Logging.{InstanceUUID, RequestID}
import scalaz.std.scalaFuture._
import scalaz.{EitherT, NonEmptyList}

import scala.concurrent.ExecutionContext
import com.daml.logging.LoggingContextOf
import com.daml.metrics.Metrics

private[http] final class Parties(
    routeSetup: RouteSetup,
    partiesService: PartiesService,
)(implicit ec: ExecutionContext) {
  import Parties._
  import routeSetup._

  def allParties(req: HttpRequest)(implicit
      lc: LoggingContextOf[InstanceUUID with RequestID]
  ): ET[domain.SyncResponse[List[domain.PartyDetails]]] =
    proxyWithoutCommand((jwt, _) => partiesService.allParties(jwt))(req)
      .flatMap(pd => either(pd map (domain.OkResponse(_))))

  def parties(jwt: Jwt, parties: NonEmptyList[domain.Party])(implicit
      lc: LoggingContextOf[InstanceUUID with RequestID]
  ): ET[domain.SyncResponse[List[domain.PartyDetails]]] =
    for {
      ps <- eitherT(partiesService.parties(jwt, toNonEmptySet(parties)))
    } yield partiesResponse(parties = ps._1.toList, unknownParties = ps._2.toList)

  def allocateParty(jwt: Jwt, request: domain.AllocatePartyRequest)(implicit
      lc: LoggingContextOf[InstanceUUID with RequestID],
      metrics: Metrics,
  ): ET[domain.SyncResponse[domain.PartyDetails]] =
    for {
      _ <- EitherT.pure(metrics.daml.HttpJsonApi.allocatePartyThroughput.mark())
      res <- eitherT(partiesService.allocate(jwt, request))
    } yield domain.OkResponse(res)
}

private[endpoints] object Parties {
  private def partiesResponse(
      parties: List[domain.PartyDetails],
      unknownParties: List[domain.Party],
  ): domain.SyncResponse[List[domain.PartyDetails]] = {

    val warnings: Option[domain.UnknownParties] =
      if (unknownParties.isEmpty) None
      else Some(domain.UnknownParties(unknownParties))

    domain.OkResponse(parties, warnings)
  }
}
