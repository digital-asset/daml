// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.daml.ledger.client

import com.daml.ledger.api.v2.admin.party_management_service.PartyManagementServiceGrpc.PartyManagementServiceStub
import com.daml.ledger.api.v2.admin.party_management_service.{
  AllocatePartyRequest,
  ListKnownPartiesRequest,
  PartyDetails => ApiPartyDetails,
}
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.daml.lf.data.Ref.Party

import scala.concurrent.{ExecutionContext, Future}

object PartyManagementClient {

  private def details(d: ApiPartyDetails): PartyDetails =
    PartyDetails(
      Party.assertFromString(d.party),
      d.isLocal,
      ObjectMeta.empty,
      IdentityProviderId(d.identityProviderId),
    )

  private def listKnownPartiesRequest(pageToken: String, pageSize: Int, filterParty: String) =
    ListKnownPartiesRequest(
      pageToken = pageToken,
      pageSize = pageSize,
      identityProviderId = "",
      filterParty = filterParty,
    )
}

final class PartyManagementClient(
    service: PartyManagementServiceStub,
    getDefaultToken: () => Option[String] = () => None,
)(implicit
    ec: ExecutionContext
) {

  def listKnownParties(
      token: Option[String] = None,
      pageToken: String = "",
      pageSize: Int = 1000,
      filterParty: String = "",
  )(implicit traceContext: TraceContext): Future[(List[PartyDetails], String)] =
    LedgerClient
      .stubWithTracing(service, token.orElse(getDefaultToken()))
      .listKnownParties(
        PartyManagementClient
          .listKnownPartiesRequest(pageToken, pageSize, filterParty = filterParty)
      )
      .map(resp =>
        (resp.partyDetails.view.map(PartyManagementClient.details).toList, resp.nextPageToken)
      )

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
}
