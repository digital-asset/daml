// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.api.testtool.suites.v2_1.objectmeta

import com.daml.ledger.api.testtool.infrastructure.Allocation.Participants
import com.daml.ledger.api.testtool.infrastructure.participant.ParticipantTestContext
import com.daml.ledger.api.v2.admin.object_meta.ObjectMeta
import com.digitalasset.base.error.ErrorCode

import scala.concurrent.{ExecutionContext, Future}

trait ObjectMetaTestsBase {

  // A resource containing an ObjectMeta metadata
  private[objectmeta] type Resource
  private[objectmeta] type ResourceId

  private[objectmeta] def getId(resource: Resource): ResourceId

  private[objectmeta] def annotationsUpdateRequestFieldPath: String

  private[objectmeta] def resourceVersionUpdatePath: String
  private[objectmeta] def annotationsUpdatePath: String
  private[objectmeta] def annotationsShortUpdatePath: String
  private[objectmeta] def resourceIdPath: String

  private[objectmeta] def extractAnnotations(resource: Resource): Map[String, String]
  private[objectmeta] def extractMetadata(resource: Resource): ObjectMeta

  private[objectmeta] def update(
      id: ResourceId,
      annotations: Map[String, String],
      updatePaths: Seq[String],
      resourceVersion: String = "",
  )(implicit
      ec: ExecutionContext,
      ledger: ParticipantTestContext,
  ): Future[ObjectMeta]

  private[objectmeta] def fetchNewestAnnotations(
      id: ResourceId
  )(implicit
      ec: ExecutionContext,
      ledger: ParticipantTestContext,
  ): Future[Map[String, String]]

  private[objectmeta] def createResourceWithAnnotations(
      connectedSynchronizers: Int,
      annotations: Map[String, String],
  )(implicit
      ec: ExecutionContext,
      ledger: ParticipantTestContext,
  ): Future[Map[String, String]]

  private[objectmeta] def testWithoutResource(
      shortIdentifier: String,
      description: String,
  )(
      body: ExecutionContext => ParticipantTestContext => Participants => Future[Unit]
  ): Unit

  private[objectmeta] def testWithFreshResource(
      shortIdentifier: String,
      description: String,
  )(
      annotations: Map[String, String] = Map.empty
  )(
      body: ExecutionContext => ParticipantTestContext => Resource => Future[Unit]
  ): Unit

  private[objectmeta] def assertValidResourceVersionString(v: String, sourceMsg: String): Unit =
    assert(v.nonEmpty, s"resource version (from $sourceMsg) must be non empty")

  private[objectmeta] def concurrentUserUpdateDetectedErrorCode: ErrorCode

  private[objectmeta] def invalidUpdateRequestErrorCode: ErrorCode

}
