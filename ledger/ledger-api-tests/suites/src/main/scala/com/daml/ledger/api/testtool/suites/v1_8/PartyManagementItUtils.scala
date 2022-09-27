// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.api.testtool.suites.v1_8

import com.daml.ledger.api.testtool.infrastructure.Allocation.{
  NoParties,
  Participant,
  Participants,
  allocate,
}
import com.daml.ledger.api.testtool.infrastructure.participant.{Features, ParticipantTestContext}
import com.daml.ledger.api.v1.admin.object_meta.ObjectMeta
import com.daml.ledger.api.v1.admin.party_management_service._
import com.google.protobuf.field_mask.FieldMask

import scala.concurrent.{ExecutionContext, Future}

trait PartyManagementItUtils { self: PartyManagementServiceIT =>
  def unsetResourceVersion[T](t: T): T = {
    val t2: T = t match {
      case u: PartyDetails => u.update(_.localMetadata.resourceVersion := "").asInstanceOf[T]
      case u: AllocatePartyResponse =>
        u.update(_.partyDetails.localMetadata.resourceVersion := "").asInstanceOf[T]
      case u: UpdatePartyDetailsResponse =>
        u.update(_.partyDetails.localMetadata.resourceVersion := "").asInstanceOf[T]
      case other => sys.error(s"could not match $other")
    }
    t2
  }

  def updateRequest(
      party: String,
      displayName: String = "",
      isLocal: Boolean = false,
      annotations: Map[String, String] = Map.empty,
      resourceVersion: String = "",
      updatePaths: Seq[String],
  ): UpdatePartyDetailsRequest =
    UpdatePartyDetailsRequest(
      partyDetails = Some(
        PartyDetails(
          party = party,
          displayName = displayName,
          isLocal = isLocal,
          localMetadata =
            Some(ObjectMeta(resourceVersion = resourceVersion, annotations = annotations)),
        )
      ),
      updateMask = Some(FieldMask(updatePaths)),
    )

  def extractUpdatedAnnotations(
      updateResp: UpdatePartyDetailsResponse
  ): Map[String, String] =
    updateResp.partyDetails.get.localMetadata.get.annotations

  def extractUpdatedAnnotations(
      allocateResp: AllocatePartyResponse
  ): Map[String, String] =
    allocateResp.partyDetails.get.localMetadata.get.annotations

  def withFreshParty[T](
      annotations: Map[String, String] = Map.empty,
      displayName: String = "",
  )(
      f: PartyDetails => Future[T]
  )(implicit ledger: ParticipantTestContext, ec: ExecutionContext): Future[T] = {
    val req = AllocatePartyRequest(
      displayName = displayName,
      localMetadata = Some(
        ObjectMeta(
          resourceVersion = "",
          annotations = annotations,
        )
      ),
    )
    for {
      create <- ledger.allocateParty(req)
      v <- f(create.partyDetails.get)
    } yield v
  }

  def testWithFreshPartyDetails(
      shortIdentifier: String,
      description: String,
  )(
      annotations: Map[String, String] = Map.empty,
      displayName: String = "",
  )(
      body: ExecutionContext => ParticipantTestContext => PartyDetails => Future[Unit]
  ): Unit = {
    test(
      shortIdentifier = shortIdentifier,
      description = description,
      partyAllocation = allocate(NoParties),
      enabled = (features: Features) => features.userAndPartyLocalMetadataExtensions,
    )(implicit ec => { case Participants(Participant(ledger)) =>
      withFreshParty(
        annotations = annotations,
        displayName = displayName,
      ) { partyDetails =>
        body(ec)(ledger)(partyDetails)
      }(ledger, ec)
    })
  }

  def newPartyDetails(
      party: String,
      annotations: Map[String, String] = Map.empty,
      isLocal: Boolean = true,
  ): PartyDetails = PartyDetails(
    party = party,
    isLocal = isLocal,
    localMetadata = Some(ObjectMeta(annotations = annotations)),
  )
}
