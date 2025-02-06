// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.http.endpoints

import com.daml.jwt.Jwt
import com.daml.logging.LoggingContextOf
import com.digitalasset.canton.http.Endpoints.ET
import com.digitalasset.canton.http.util.Collections.toNonEmptySet
import com.digitalasset.canton.http.util.FutureUtil.eitherT
import com.digitalasset.canton.http.util.Logging.{InstanceUUID, RequestID}
import com.digitalasset.canton.http.{
  AllocatePartyRequest,
  OkResponse,
  PartiesService,
  Party,
  PartyDetails,
  SyncResponse,
  UnknownParties,
}
import org.apache.pekko.stream.Materializer
import scalaz.NonEmptyList
import scalaz.std.scalaFuture.*

import scala.concurrent.ExecutionContext

private[http] final class Parties(partiesService: PartiesService)(implicit ec: ExecutionContext) {
  import Parties.*

  def allParties(jwt: Jwt)(implicit
      lc: LoggingContextOf[InstanceUUID with RequestID],
      mat: Materializer,
  ): ET[SyncResponse[List[PartyDetails]]] = for {
    res <- eitherT(partiesService.allParties(jwt))
  } yield OkResponse(res)

  def parties(jwt: Jwt, parties: NonEmptyList[Party])(implicit
      lc: LoggingContextOf[InstanceUUID with RequestID]
  ): ET[SyncResponse[List[PartyDetails]]] =
    for {
      ps <- eitherT(partiesService.parties(jwt, toNonEmptySet(parties)))
    } yield partiesResponse(parties = ps._1.toList, unknownParties = ps._2.toList)

  def allocateParty(jwt: Jwt, request: AllocatePartyRequest)(implicit
      lc: LoggingContextOf[InstanceUUID with RequestID]
  ): ET[SyncResponse[PartyDetails]] =
    for {
      res <- eitherT(partiesService.allocate(jwt, request))
    } yield OkResponse(res)
}

private[endpoints] object Parties {
  private def partiesResponse(
      parties: List[PartyDetails],
      unknownParties: List[Party],
  ): SyncResponse[List[PartyDetails]] = {

    val warnings: Option[UnknownParties] =
      if (unknownParties.isEmpty) None
      else Some(UnknownParties(unknownParties))

    OkResponse(parties, warnings)
  }
}
