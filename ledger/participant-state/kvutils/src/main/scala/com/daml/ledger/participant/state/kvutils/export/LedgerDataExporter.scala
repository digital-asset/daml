// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.participant.state.kvutils.export

import java.io.{BufferedOutputStream, DataOutputStream}
import java.nio.file.{Files, Paths}

import com.daml.resources.{Resource, ResourceOwner}
import org.slf4j.LoggerFactory

import scala.concurrent.ExecutionContext

trait LedgerDataExporter {
  def addSubmission(submissionInfo: SubmissionInfo): SubmissionAggregator
}

object LedgerDataExporter {
  val EnvironmentVariableName = "KVUTILS_LEDGER_EXPORT"

  private val logger = LoggerFactory.getLogger(this.getClass)

  object Owner extends ResourceOwner[LedgerDataExporter] {
    override def acquire()(
        implicit executionContext: ExecutionContext
    ): Resource[LedgerDataExporter] =
      sys.env
        .get(EnvironmentVariableName)
        .map(Paths.get(_))
        .map { path =>
          logger.info(s"Enabled writing ledger entries to $path.")
          ResourceOwner
            .forCloseable(() =>
              new DataOutputStream(new BufferedOutputStream(Files.newOutputStream(path))))
            .acquire()
            .map(new SerializationBasedLedgerDataExporter(_))
        }
        .getOrElse(Resource.successful(NoOpLedgerDataExporter))
  }

}
