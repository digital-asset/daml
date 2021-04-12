// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.participant.state.kvutils.tools.integritycheck

import java.io.Writer
import java.nio.file.Path

import akka.stream.Materializer
import com.daml.ledger.participant.state.v1.ReadService

import scala.concurrent.{ExecutionContext, Future}

object StateUpdateExporter {

  def write(
      expectedReadService: ReplayingReadService,
      actualReadService: ReplayingReadService,
      outputWriterFactory: Path => Writer,
      config: Config,
  )(implicit executionContext: ExecutionContext, materializer: Materializer): Future[Unit] = {
    for {
      _ <- config.expectedUpdatesPath.fold(Future.unit)(path =>
        StateUpdateExporter.write(
          expectedReadService,
          outputWriterFactory(path),
        )
      )
      _ <- config.actualUpdatesPath.fold(Future.unit)(path =>
        StateUpdateExporter.write(
          actualReadService,
          outputWriterFactory(path),
        )
      )
    } yield ()
  }

  private def write(readService: ReadService, outputWriter: Writer)(implicit
      materializer: Materializer,
      executionContext: ExecutionContext,
  ): Future[Unit] = {
    readService
      .stateUpdates(None)
      .runForeach { case (_, update) =>
        outputWriter.write(s"$update\n")
      }
      .map(_ => ())
      .andThen { case _ => outputWriter.close() }
  }
}
