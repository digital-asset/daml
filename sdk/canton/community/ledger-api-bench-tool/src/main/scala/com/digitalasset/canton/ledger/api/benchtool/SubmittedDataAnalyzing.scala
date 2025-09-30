// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.ledger.api.benchtool

import com.digitalasset.canton.ledger.api.benchtool.config.WorkflowConfig
import com.digitalasset.canton.ledger.api.benchtool.submission.{AllocatedParties, PartyAllocating}
import org.slf4j.{Logger, LoggerFactory}

import scala.concurrent.{ExecutionContext, Future}

/** Contains utilities for retrieving useful facts from data already submitted to a Ledger API
  * server. (The motivating use case are the benchmarks that do not perform a submission step on
  * their own and for that reason cannot statically determine these facts.)
  */
object SubmittedDataAnalyzing {

  private[benchtool] val logger: Logger = LoggerFactory.getLogger(getClass)

  def determineAllocatedParties(
      workflowConfig: WorkflowConfig,
      partyAllocating: PartyAllocating,
  )(implicit ec: ExecutionContext): Future[AllocatedParties] = {
    logger.info("Analyzing existing parties..")
    for {
      existingParties <- {
        logger.info("Analyzing existing parties..")
        partyAllocating.lookupExistingParties()
      }
    } yield {
      AllocatedParties.forExistingParties(
        parties = existingParties.toList,
        partyPrefixesForPartySets =
          workflowConfig.streams.flatMap(_.partySetPrefixes.iterator).distinct,
      )
    }
  }
}
