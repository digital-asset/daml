// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.api.testtool.suites.v1_8.object_meta

import com.daml.ledger.api.testtool.infrastructure.ExpectedErrorDescription
import com.daml.ledger.api.testtool.infrastructure.participant.ParticipantTestContext
import com.daml.ledger.api.v1.admin.object_meta.ObjectMeta

import scala.concurrent.{ExecutionContext, Future}

trait ObjectMetaTestsBase {

  // A resource containing an ObjectMeta metadata
  private[object_meta] type Resource
  private[object_meta] type ResourceId

  private[object_meta] def getId(resource: Resource): ResourceId

  private[object_meta] def annotationsUpdateRequestFieldPath: String

  private[object_meta] def resourceVersionUpdatePath: String
  private[object_meta] def annotationsUpdatePath: String
  private[object_meta] def annotationsShortUpdatePath: String
  private[object_meta] def resourceIdPath: String

  private[object_meta] def extractAnnotations(resource: Resource): Map[String, String]
  private[object_meta] def extractMetadata(resource: Resource): ObjectMeta

  private[object_meta] def update(
      id: ResourceId,
      annotations: Map[String, String],
      updatePaths: Seq[String],
      resourceVersion: String = "",
  )(implicit
      ec: ExecutionContext,
      ledger: ParticipantTestContext,
  ): Future[ObjectMeta]

  private[object_meta] def fetchNewestAnnotations(
      id: ResourceId
  )(implicit
      ec: ExecutionContext,
      ledger: ParticipantTestContext,
  ): Future[Map[String, String]]

  private[object_meta] def createResourceWithAnnotations(annotations: Map[String, String])(implicit
      ec: ExecutionContext,
      ledger: ParticipantTestContext,
  ): Future[Map[String, String]]

  private[object_meta] def testWithoutResource(
      shortIdentifier: String,
      description: String,
  )(
      body: ExecutionContext => ParticipantTestContext => Future[Unit]
  ): Unit

  private[object_meta] def testWithFreshResource(
      shortIdentifier: String,
      description: String,
  )(
      annotations: Map[String, String] = Map.empty
  )(
      body: ExecutionContext => ParticipantTestContext => Resource => Future[Unit]
  ): Unit

  private[object_meta] def assertValidResourceVersionString(v: String, sourceMsg: String): Unit = {
    assert(v.nonEmpty, s"resource version (from $sourceMsg) must be non empty")
  }

  private[object_meta] def concurrentUserUpdateDetectedErrorDescription(
      id: ResourceId
  ): ExpectedErrorDescription

  private[object_meta] def invalidUpdateRequestErrorDescription(
      id: ResourceId,
      errorMessageSuffix: String,
  ): ExpectedErrorDescription

}
