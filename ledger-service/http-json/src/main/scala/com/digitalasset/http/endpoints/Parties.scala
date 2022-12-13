// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.http
package endpoints

import Endpoints.ET
import com.daml.jwt.domain.Jwt
import util.Collections.toNonEmptySet
import util.FutureUtil.eitherT
import util.Logging.{InstanceUUID, RequestID}
import scalaz.std.scalaFuture._
import scalaz.{EitherT, NonEmptyList}

import scala.concurrent.ExecutionContext
import com.daml.http.metrics.HttpJsonApiMetrics
import com.daml.logging.LoggingContextOf

private[http] final class Parties(partiesService: PartiesService)(implicit ec: ExecutionContext) {
  import Parties._

  def allParties(jwt: Jwt)(implicit
      lc: LoggingContextOf[InstanceUUID with RequestID]
  ): ET[domain.SyncResponse[List[domain.PartyDetails]]] = for {
    res <- eitherT(partiesService.allParties(jwt))
  } yield domain.OkResponse(res)

  def parties(jwt: Jwt, parties: NonEmptyList[domain.Party])(implicit
      lc: LoggingContextOf[InstanceUUID with RequestID]
  ): ET[domain.SyncResponse[List[domain.PartyDetails]]] =
    for {
      ps <- eitherT(partiesService.parties(jwt, toNonEmptySet(parties)))
    } yield partiesResponse(parties = ps._1.toList, unknownParties = ps._2.toList)

  def allocateParty(jwt: Jwt, request: domain.AllocatePartyRequest)(implicit
      lc: LoggingContextOf[InstanceUUID with RequestID],
      metrics: HttpJsonApiMetrics,
  ): ET[domain.SyncResponse[domain.PartyDetails]] =
    for {
      _ <- EitherT.pure(metrics.allocatePartyThroughput.mark())
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
