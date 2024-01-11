// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.http.endpoints

import com.digitalasset.canton.http.Endpoints.ET
import com.daml.jwt.domain.Jwt
import com.digitalasset.canton.http.util.Collections.toNonEmptySet
import com.digitalasset.canton.http.util.FutureUtil.eitherT
import com.digitalasset.canton.http.util.Logging.{InstanceUUID, RequestID}
import scalaz.std.scalaFuture.*
import scalaz.NonEmptyList

import scala.concurrent.ExecutionContext
import com.daml.logging.LoggingContextOf
import com.digitalasset.canton.http.{PartiesService, domain}

private[http] final class Parties(partiesService: PartiesService)(implicit ec: ExecutionContext) {
  import Parties.*

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
      lc: LoggingContextOf[InstanceUUID with RequestID]
  ): ET[domain.SyncResponse[domain.PartyDetails]] =
    for {
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
