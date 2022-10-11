// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.api.testtool.suites.v1_8.objectmeta

import com.daml.error.definitions.LedgerApiErrors
import com.daml.ledger.api.testtool.infrastructure.ExpectedErrorDescription
import com.daml.ledger.api.testtool.infrastructure.participant.ParticipantTestContext
import com.daml.ledger.api.testtool.suites.v1_8.PartyManagementITBase
import com.daml.ledger.api.v1.admin.object_meta.ObjectMeta
import com.daml.ledger.api.v1.admin.party_management_service.{
  AllocatePartyRequest,
  GetPartiesRequest,
  PartyDetails,
}

import scala.concurrent.{ExecutionContext, Future}

class PartyManagementServiceObjectMetaIT extends PartyManagementITBase with ObjectMetaTests {

  type Resource = PartyDetails
  type ResourceId = String

  override private[objectmeta] def getId(resource: Resource): ResourceId = resource.party

  override private[objectmeta] def annotationsUpdateRequestFieldPath: String =
    "party_details.local_metadata.annotations"

  override private[objectmeta] def resourceVersionUpdatePath: String =
    "local_metadata.resource_version"

  override private[objectmeta] def annotationsUpdatePath: String =
    "local_metadata.annotations"

  override private[objectmeta] def annotationsShortUpdatePath = "local_metadata"

  override private[objectmeta] def resourceIdPath = "party"

  override private[objectmeta] def extractAnnotations(resource: Resource): Map[String, String] =
    resource.getLocalMetadata.annotations

  override private[objectmeta] def extractMetadata(resource: Resource): ObjectMeta =
    resource.getLocalMetadata

  override private[objectmeta] def testWithFreshResource(
      shortIdentifier: String,
      description: String,
  )(
      annotations: Map[String, String] = Map.empty
  )(
      body: ExecutionContext => ParticipantTestContext => Resource => Future[Unit]
  ): Unit = {
    testWithFreshPartyDetails(
      shortIdentifier = shortIdentifier,
      description = description,
      requiresUserAndPartyLocalMetadataExtensions = true,
    )(annotations = annotations)(body)
  }

  override private[objectmeta] def testWithoutResource(
      shortIdentifier: String,
      description: String,
  )(
      body: ExecutionContext => ParticipantTestContext => Future[Unit]
  ): Unit = {
    testWithoutPartyDetails(
      shortIdentifier = shortIdentifier,
      description = description,
      requiresUserAndPartyLocalMetadataExtensions = true,
    )(body)
  }

  override private[objectmeta] def createResourceWithAnnotations(
      annotations: Map[String, String]
  )(implicit ec: ExecutionContext, ledger: ParticipantTestContext): Future[Map[String, String]] = {
    val req = AllocatePartyRequest(localMetadata = Some(ObjectMeta(annotations = annotations)))
    ledger
      .allocateParty(req)
      .map(extractUpdatedAnnotations)
  }

  override private[objectmeta] def fetchNewestAnnotations(
      id: ResourceId
  )(implicit ec: ExecutionContext, ledger: ParticipantTestContext): Future[Map[String, String]] = {
    ledger
      .getParties(GetPartiesRequest(parties = Seq(id)))
      .map(_.partyDetails.head.getLocalMetadata.annotations)
  }

  override private[objectmeta] def update(
      id: ResourceId,
      annotations: Map[String, String],
      updatePaths: Seq[String],
      resourceVersion: String = "",
  )(implicit ec: ExecutionContext, ledger: ParticipantTestContext): Future[ObjectMeta] = {
    val req = updateRequest(
      party = id,
      annotations = annotations,
      resourceVersion = resourceVersion,
      updatePaths = updatePaths,
    )
    ledger
      .updatePartyDetails(req)
      .map(_.getPartyDetails.getLocalMetadata)
  }

  override private[objectmeta] def concurrentUserUpdateDetectedErrorDescription(
      id: ResourceId
  ): ExpectedErrorDescription = ExpectedErrorDescription(
    errorCode = LedgerApiErrors.Admin.PartyManagement.ConcurrentPartyDetailsUpdateDetected,
    exceptionMessageSubstring = Some(
      s"ABORTED: CONCURRENT_PARTY_DETAILS_UPDATE_DETECTED(2,0): Update operation for party '${id}' failed due to a concurrent update to the same party"
    ),
  )

  override private[objectmeta] def invalidUpdateRequestErrorDescription(
      id: ResourceId,
      errorMessageSuffix: String,
  ): ExpectedErrorDescription = ExpectedErrorDescription(
    errorCode = LedgerApiErrors.Admin.PartyManagement.InvalidUpdatePartyDetailsRequest,
    exceptionMessageSubstring = Some(
      s"INVALID_ARGUMENT: INVALID_PARTY_DETAILS_UPDATE_REQUEST(8,0): Update operation for party '${id}' failed due to: $errorMessageSuffix"
    ),
  )

}
