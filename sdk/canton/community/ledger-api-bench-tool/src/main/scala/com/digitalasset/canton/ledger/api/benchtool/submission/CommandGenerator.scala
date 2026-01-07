// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.ledger.api.benchtool.submission

import com.daml.ledger.api.v2.commands.Command
import com.daml.ledger.javaapi.data.Party
import org.apache.pekko.NotUsed
import org.apache.pekko.stream.scaladsl.Source

import scala.concurrent.{ExecutionContext, Future}
import scala.util.Try

trait CommandGenerator {
  val batchesPerInstance: Int
  def commandBatchSource(
      numContractsToGenerate: Int,
      contractCommandGenerationParallelism: Int,
  )(implicit ec: ExecutionContext): Source[(Int, Seq[Command]), NotUsed]
  def nextUserId(): String
  def nextExtraCommandSubmitters(): List[Party]
}

trait SimpleCommandGenerator extends CommandGenerator {
  def next(): Try[Seq[Command]]

  override val batchesPerInstance: Int = 1
  override def commandBatchSource(numContractsToGenerate: Int, commandGenerationParallelism: Int)(
      implicit ec: ExecutionContext
  ): Source[(Int, Seq[Command]), NotUsed] =
    Source
      .fromIterator(() => (1 to numContractsToGenerate).iterator)
      .mapAsync(commandGenerationParallelism)(index =>
        Future.fromTry(
          next().map(cmd => index -> cmd)
        )
      )
}
