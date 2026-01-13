// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.api.testtool.suites.v2_1

import com.daml.ledger.api.testtool.infrastructure.Allocation.{
  NoParties,
  Participant,
  Participants,
  allocate,
}
import com.daml.ledger.api.testtool.infrastructure.LedgerTestSuite
import com.daml.ledger.api.testtool.infrastructure.participant.ParticipantTestContext
import com.daml.ledger.api.v2.admin.object_meta.ObjectMeta
import com.daml.ledger.api.v2.admin.party_management_service.*
import com.google.protobuf.field_mask.FieldMask

import scala.concurrent.{ExecutionContext, Future}

trait PartyManagementITBase extends LedgerTestSuite {
  def updateRequest(
      party: String,
      isLocal: Boolean = false,
      annotations: Map[String, String] = Map.empty,
      resourceVersion: String = "",
      updatePaths: Seq[String],
  ): UpdatePartyDetailsRequest =
    UpdatePartyDetailsRequest(
      partyDetails = Some(
        PartyDetails(
          party = party,
          isLocal = isLocal,
          localMetadata =
            Some(ObjectMeta(resourceVersion = resourceVersion, annotations = annotations)),
          identityProviderId = "",
        )
      ),
      updateMask = Some(FieldMask(updatePaths)),
    )

  def extractUpdatedAnnotations(
      updateResp: UpdatePartyDetailsResponse
  ): Map[String, String] =
    updateResp.partyDetails.value.localMetadata.value.annotations

  def extractUpdatedAnnotations(
      allocateResp: AllocatePartyResponse
  ): Map[String, String] =
    allocateResp.partyDetails.value.localMetadata.value.annotations

  def withFreshParty[T](
      connectedSynchronizers: Int,
      annotations: Map[String, String] = Map.empty,
  )(
      f: PartyDetails => Future[T]
  )(implicit ledger: ParticipantTestContext, ec: ExecutionContext): Future[T] = {
    val req = AllocatePartyRequest(
      partyIdHint = "",
      localMetadata = Some(
        ObjectMeta(
          resourceVersion = "",
          annotations = annotations,
        )
      ),
      identityProviderId = "",
      synchronizerId = "",
      userId = "",
    )
    for {
      (create, _) <- ledger.allocateParty(req, connectedSynchronizers)
      v <- f(create.partyDetails.value)
    } yield v
  }

  def testWithFreshPartyDetails(
      shortIdentifier: String,
      description: String,
  )(
      annotations: Map[String, String] = Map.empty
  )(
      body: ExecutionContext => ParticipantTestContext => PartyDetails => Future[Unit]
  ): Unit =
    test(
      shortIdentifier = shortIdentifier,
      description = description,
      partyAllocation = allocate(NoParties),
    )(implicit ec => { case p @ Participants(Participant(ledger, Seq())) =>
      withFreshParty(
        connectedSynchronizers = p.minSynchronizers,
        annotations = annotations,
      ) { partyDetails =>
        body(ec)(ledger)(partyDetails)
      }(ledger, ec)
    })

  def testWithoutPartyDetails(
      shortIdentifier: String,
      description: String,
  )(
      body: ExecutionContext => ParticipantTestContext => Participants => Future[Unit]
  ): Unit =
    test(
      shortIdentifier = shortIdentifier,
      description = description,
      partyAllocation = allocate(NoParties),
    )(implicit ec => { case p @ Participants(Participant(ledger, Seq())) =>
      body(ec)(ledger)(p)
    })

  def newPartyDetails(
      party: String,
      annotations: Map[String, String] = Map.empty,
      isLocal: Boolean = true,
  ): PartyDetails = PartyDetails(
    party = party,
    isLocal = isLocal,
    localMetadata = Some(ObjectMeta(resourceVersion = "", annotations = annotations)),
    identityProviderId = "",
  )

  protected def unsetResourceVersion(u: PartyDetails): PartyDetails =
    u.update(_.localMetadata.resourceVersion := "")
  protected def unsetResourceVersion(u: AllocatePartyResponse): AllocatePartyResponse =
    u.update(_.partyDetails.localMetadata.resourceVersion := "")
  protected def unsetResourceVersion(u: UpdatePartyDetailsResponse): UpdatePartyDetailsResponse =
    u.update(_.partyDetails.localMetadata.resourceVersion := "")
}
