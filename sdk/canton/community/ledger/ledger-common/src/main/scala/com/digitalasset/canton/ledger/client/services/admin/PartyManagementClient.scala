// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.ledger.client.services.admin

import com.daml.ledger.api.v2.admin.party_management_service.PartyManagementServiceGrpc.PartyManagementServiceStub
import com.daml.ledger.api.v2.admin.party_management_service.{
  AllocatePartyRequest,
  GetParticipantIdRequest,
  GetPartiesRequest,
  ListKnownPartiesRequest,
  PartyDetails as ApiPartyDetails,
}
import com.digitalasset.canton.ledger.api.{
  IdentityProviderId,
  ObjectMeta,
  ParticipantId,
  PartyDetails,
}
import com.digitalasset.canton.ledger.client.LedgerClient
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.daml.lf.data.Ref
import com.digitalasset.daml.lf.data.Ref.Party
import scalaz.OneAnd

import scala.concurrent.{ExecutionContext, Future}

object PartyManagementClient {

  private def details(d: ApiPartyDetails): PartyDetails =
    PartyDetails(
      Party.assertFromString(d.party),
      d.isLocal,
      ObjectMeta.empty,
      IdentityProviderId(d.identityProviderId),
    )

  private val getParticipantIdRequest = GetParticipantIdRequest()

  private def listKnownPartiesRequest(pageToken: String, pageSize: Int) =
    ListKnownPartiesRequest(
      pageToken = pageToken,
      pageSize = pageSize,
      identityProviderId = "",
    )

  private def getPartiesRequest(parties: OneAnd[Set, Ref.Party]) = {
    import scalaz.std.iterable.*
    import scalaz.syntax.foldable.*
    GetPartiesRequest(
      parties = parties.toList,
      identityProviderId = "",
    )
  }
}

final class PartyManagementClient(
    service: PartyManagementServiceStub,
    getDefaultToken: () => Option[String] = () => None,
)(implicit
    ec: ExecutionContext
) {

  def getParticipantId(
      token: Option[String] = None
  )(implicit traceContext: TraceContext): Future[ParticipantId] =
    LedgerClient
      .stubWithTracing(service, token.orElse(getDefaultToken()))
      .getParticipantId(PartyManagementClient.getParticipantIdRequest)
      .map(r => ParticipantId(Ref.ParticipantId.assertFromString(r.participantId)))

  def listKnownParties(
      token: Option[String] = None,
      pageToken: String = "",
      pageSize: Int = 1000,
  )(implicit traceContext: TraceContext): Future[(List[PartyDetails], String)] =
    LedgerClient
      .stubWithTracing(service, token.orElse(getDefaultToken()))
      .listKnownParties(PartyManagementClient.listKnownPartiesRequest(pageToken, pageSize))
      .map(resp =>
        (resp.partyDetails.view.map(PartyManagementClient.details).toList, resp.nextPageToken)
      )

  def getParties(
      parties: OneAnd[Set, Ref.Party],
      token: Option[String] = None,
  )(implicit traceContext: TraceContext): Future[List[PartyDetails]] =
    LedgerClient
      .stubWithTracing(service, token.orElse(getDefaultToken()))
      .getParties(PartyManagementClient.getPartiesRequest(parties))
      .map(_.partyDetails.view.map(PartyManagementClient.details).toList)

  def allocateParty(
      hint: Option[String],
      synchronizerId: Option[String] = None,
      token: Option[String] = None,
  )(implicit traceContext: TraceContext): Future[PartyDetails] =
    LedgerClient
      .stubWithTracing(service, token.orElse(getDefaultToken()))
      .allocateParty(
        AllocatePartyRequest(
          partyIdHint = hint.getOrElse(""),
          localMetadata = None,
          identityProviderId = "",
          synchronizerId = synchronizerId.getOrElse(""),
          userId = "",
        )
      )
      .map(_.partyDetails.getOrElse(sys.error("No PartyDetails in response.")))
      .map(PartyManagementClient.details)

  /** Utility method for json services
    */
  def serviceStub(token: Option[String] = None)(implicit
      traceContext: TraceContext
  ): PartyManagementServiceStub =
    LedgerClient.stubWithTracing(service, token.orElse(getDefaultToken()))
}
