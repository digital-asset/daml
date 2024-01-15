// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.client.services.admin

import com.daml.lf.data.Ref
import com.daml.lf.data.Ref.Party
import com.daml.ledger.api.domain.{IdentityProviderId, ObjectMeta, ParticipantId, PartyDetails}
import com.daml.ledger.api.v1.admin.party_management_service.PartyManagementServiceGrpc.PartyManagementServiceStub
import com.daml.ledger.api.v1.admin.party_management_service.{
  AllocatePartyRequest,
  GetParticipantIdRequest,
  GetPartiesRequest,
  ListKnownPartiesRequest,
  UpdatePartyDetailsRequest,
  PartyDetails => ApiPartyDetails,
}
import com.daml.ledger.api.v1.admin.object_meta.{ObjectMeta => ApiObjectMeta}
import com.daml.ledger.client.LedgerClient
import com.google.protobuf.field_mask.FieldMask
import scalaz.OneAnd

import java.util.concurrent.{Executors, ScheduledExecutorService}
import scala.concurrent.duration.{DurationInt, FiniteDuration}
import scala.concurrent.{ExecutionContext, Future, Promise}

object PartyManagementClient {

  /** A scheduled service executor used by [[sleep]]. */
  private val scheduledExecutorService: ScheduledExecutorService =
    Executors.newScheduledThreadPool(1)

  /** Returns a future that completes after the provided delay. */
  private def sleep(delay: FiniteDuration): Future[Unit] = {
    val promise = Promise[Unit]()
    val _ = scheduledExecutorService.schedule(() => promise.success(()), delay.length, delay.unit)
    promise.future
  }

  private def details(proto: ApiPartyDetails): PartyDetails =
    PartyDetails(
      Party.assertFromString(proto.party),
      if (proto.displayName.isEmpty) None else Some(proto.displayName),
      proto.isLocal,
      fromProtoObjectMeta(proto.localMetadata),
      IdentityProviderId(proto.identityProviderId),
    )

  private def toProtoPartyDetails(domain: PartyDetails): ApiPartyDetails =
    ApiPartyDetails(
      domain.party,
      domain.displayName.getOrElse(""),
      domain.isLocal,
      toProtoObjectMeta(domain.metadata),
      domain.identityProviderId.toRequestString,
    )

  private val getParticipantIdRequest = GetParticipantIdRequest()

  private def fromProtoObjectMeta(protoObjectMeta: Option[ApiObjectMeta]): ObjectMeta = {
    protoObjectMeta match {
      case None => ObjectMeta.empty
      case Some(ApiObjectMeta("", _)) => ObjectMeta.empty
      case Some(ApiObjectMeta(resourceVersion, annotations)) =>
        ObjectMeta(Some(resourceVersion.toLong), annotations)
    }
  }

  private def toProtoObjectMeta(objectMeta: ObjectMeta): Option[ApiObjectMeta] = {
    objectMeta match {
      case ObjectMeta(None, _) => None
      case ObjectMeta(Some(resourceVersion), annotations) =>
        Some(ApiObjectMeta(resourceVersion.toString, annotations))
    }
  }

  private def getPartiesRequest(
      parties: OneAnd[Set, Ref.Party],
      identityProviderId: IdentityProviderId,
  ) = {
    import scalaz.std.iterable._
    import scalaz.syntax.foldable._
    GetPartiesRequest(parties.toList, identityProviderId.toRequestString)
  }
}

final class PartyManagementClient(service: PartyManagementServiceStub)(implicit
    ec: ExecutionContext
) {

  def getParticipantId(token: Option[String] = None): Future[ParticipantId] =
    LedgerClient
      .stub(service, token)
      .getParticipantId(PartyManagementClient.getParticipantIdRequest)
      .map(r => ParticipantId(Ref.ParticipantId.assertFromString(r.participantId)))

  def listKnownParties(
      token: Option[String] = None,
      identityProviderId: IdentityProviderId = IdentityProviderId.Default,
  ): Future[List[PartyDetails]] =
    LedgerClient
      .stub(service, token)
      .listKnownParties(ListKnownPartiesRequest(identityProviderId.toRequestString))
      .map(_.partyDetails.view.map(PartyManagementClient.details).toList)

  def getParties(
      parties: OneAnd[Set, Ref.Party],
      token: Option[String] = None,
      identityProviderId: IdentityProviderId = IdentityProviderId.Default,
  ): Future[List[PartyDetails]] =
    LedgerClient
      .stub(service, token)
      .getParties(PartyManagementClient.getPartiesRequest(parties, identityProviderId))
      .map(_.partyDetails.view.map(PartyManagementClient.details).toList)

  def allocateParty(
      hint: Option[String],
      displayName: Option[String],
      token: Option[String] = None,
      identityProviderId: IdentityProviderId = IdentityProviderId.Default,
  ): Future[PartyDetails] =
    LedgerClient
      .stub(service, token)
      .allocateParty(
        new AllocatePartyRequest(
          partyIdHint = hint.getOrElse(""),
          displayName = displayName.getOrElse(""),
          identityProviderId = identityProviderId.toRequestString,
        )
      )
      .map(_.partyDetails.getOrElse(sys.error("No PartyDetails in response.")))
      .map(PartyManagementClient.details)
      // TODO(https://github.com/DACH-NY/canton/issues/16401): remove the sleep call once we have a
      //  better way of synchronizing after a party allocation.
      .flatMap(details => PartyManagementClient.sleep(250.millis).map(_ => details))

  def updatePartyDetails(
      partyDetails: Option[PartyDetails],
      updateMask: Option[FieldMask],
      token: Option[String] = None,
  ): Future[PartyDetails] =
    LedgerClient
      .stub(service, token)
      .updatePartyDetails(
        UpdatePartyDetailsRequest(
          partyDetails = partyDetails.map(PartyManagementClient.toProtoPartyDetails),
          updateMask,
        )
      )
      .map(_.partyDetails.getOrElse(sys.error("No PartyDetails in response.")))
      .map(PartyManagementClient.details)
}
