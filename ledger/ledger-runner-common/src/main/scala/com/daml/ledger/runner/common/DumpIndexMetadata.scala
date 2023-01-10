// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.runner.common

import com.daml.ledger.resources.{Resource, ResourceContext, ResourceOwner}
import com.daml.logging.LoggingContext.newLoggingContext
import com.daml.logging.{ContextualizedLogger, LoggingContext}
import com.daml.platform.store.IndexMetadata

import scala.concurrent.{ExecutionContext, Future}

object DumpIndexMetadata {
  val logger = ContextualizedLogger.get(this.getClass)

  def dumpIndexMetadata(
      jdbcUrl: String
  )(implicit
      executionContext: ExecutionContext,
      context: ResourceContext,
  ): Future[Unit] = {
    newLoggingContext { implicit loggingContext: LoggingContext =>
      val metadataFuture = IndexMetadata.read(jdbcUrl).use { metadata =>
        logger.warn(s"ledger_id: ${metadata.ledgerId}")
        logger.warn(s"participant_id: ${metadata.participantId}")
        logger.warn(s"ledger_end: ${metadata.ledgerEnd}")
        logger.warn(s"version: ${metadata.participantIntegrationApiVersion}")
        Future.unit
      }
      metadataFuture.failed.foreach { exception =>
        logger.error("Error while retrieving the index metadata", exception)
      }
      metadataFuture
    }
  }

  def apply(
      jdbcUrls: Seq[String]
  ): ResourceOwner[Unit] = {
    new ResourceOwner[Unit] {
      override def acquire()(implicit context: ResourceContext): Resource[Unit] = {
        Resource.sequenceIgnoringValues(jdbcUrls.map(dumpIndexMetadata).map(Resource.fromFuture))
      }
    }
  }
}
