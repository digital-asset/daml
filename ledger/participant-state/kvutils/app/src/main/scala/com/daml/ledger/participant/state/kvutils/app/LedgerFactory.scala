// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.participant.state.kvutils.app

import akka.stream.Materializer
import com.daml.ledger.participant.state.index.v2.IndexCompletionsService
import com.daml.ledger.participant.state.kvutils.api.{
  KeyValueParticipantStateReader,
  KeyValueParticipantStateWriter,
  LedgerReader,
  LedgerWriter,
  WriteServiceWithDeduplicationSupport,
}
import com.daml.ledger.participant.state.kvutils.deduplication.{
  CompletionBasedDeduplicationPeriodConverter,
  DeduplicationPeriodSupport,
}
import com.daml.ledger.participant.state.v2.{ReadService, WriteService}
import com.daml.ledger.resources.ResourceOwner
import com.daml.lf.engine.Engine
import com.daml.logging.LoggingContext
import com.daml.metrics.Metrics
import com.daml.platform.server.api.validation.{DeduplicationPeriodValidator, ErrorFactories}

import scala.concurrent.ExecutionContext

trait LedgerFactory[ExtraConfig] {
  def ledgerName: String
  def readWriteServiceFactoryOwner(
      config: Config[ExtraConfig],
      participantConfig: ParticipantConfig,
      engine: Engine,
      metrics: Metrics,
  )(implicit
      materializer: Materializer,
      executionContext: ExecutionContext,
      loggingContext: LoggingContext,
  ): ResourceOwner[ReadWriteServiceFactory]
}

trait ReadWriteServiceFactory {
  def readService(): ReadService

  def writeService(): WriteService
}

class KeyValueReadWriteFactory(
    config: Config[_],
    metrics: Metrics,
    ledgerReader: LedgerReader,
    ledgerWriter: LedgerWriter,
) extends ReadWriteServiceFactory {
  override def readService(): ReadService = {
    KeyValueParticipantStateReader(
      ledgerReader,
      metrics,
      config.enableSelfServiceErrorCodes,
    )
  }

  override def writeService(): WriteService = {
    new KeyValueParticipantStateWriter(
      ledgerWriter,
      metrics,
    )
  }
}

class KeyValueDeduplicationSupportFactory(
    delegate: ReadWriteServiceFactory,
    config: Config[_],
    completionsService: IndexCompletionsService,
)(implicit materializer: Materializer, ec: ExecutionContext)
    extends ReadWriteServiceFactory {
  override def readService(): ReadService = delegate.readService()

  override def writeService(): WriteService = {
    val writeServiceDelegate = delegate.writeService()
    val errorFactories = ErrorFactories(config.enableSelfServiceErrorCodes)
    new WriteServiceWithDeduplicationSupport(
      writeServiceDelegate,
      new DeduplicationPeriodSupport(
        new CompletionBasedDeduplicationPeriodConverter(
          completionsService
        ),
        new DeduplicationPeriodValidator(errorFactories),
        errorFactories,
      ),
    )
  }
}
