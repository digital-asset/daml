// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.api.testtool.suites.v1_8.objectmeta

import com.daml.error.ErrorCode
import com.daml.ledger.api.testtool.infrastructure.participant.ParticipantTestContext
import com.daml.ledger.api.v1.admin.object_meta.ObjectMeta

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

  private[objectmeta] def createResourceWithAnnotations(annotations: Map[String, String])(implicit
      ec: ExecutionContext,
      ledger: ParticipantTestContext,
  ): Future[Map[String, String]]

  private[objectmeta] def testWithoutResource(
      shortIdentifier: String,
      description: String,
  )(
      body: ExecutionContext => ParticipantTestContext => Future[Unit]
  ): Unit

  private[objectmeta] def testWithFreshResource(
      shortIdentifier: String,
      description: String,
  )(
      annotations: Map[String, String] = Map.empty
  )(
      body: ExecutionContext => ParticipantTestContext => Resource => Future[Unit]
  ): Unit

  private[objectmeta] def assertValidResourceVersionString(v: String, sourceMsg: String): Unit = {
    assert(v.nonEmpty, s"resource version (from $sourceMsg) must be non empty")
  }

  private[objectmeta] def concurrentUserUpdateDetectedErrorCode: ErrorCode

  private[objectmeta] def invalidUpdateRequestErrorCode: ErrorCode

}
