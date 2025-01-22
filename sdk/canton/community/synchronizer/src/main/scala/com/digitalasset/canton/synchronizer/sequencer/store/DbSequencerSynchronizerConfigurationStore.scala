// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.synchronizer.sequencer.store

import cats.data.EitherT
import cats.syntax.traverse.*
import com.digitalasset.canton.config.CantonRequireTypes.{String1, String255}
import com.digitalasset.canton.config.ProcessingTimeout
import com.digitalasset.canton.lifecycle.FutureUnlessShutdown
import com.digitalasset.canton.logging.NamedLoggerFactory
import com.digitalasset.canton.protocol.StaticSynchronizerParameters
import com.digitalasset.canton.resource.{DbStorage, DbStore}
import com.digitalasset.canton.serialization.ProtoConverter.ParsingResult
import com.digitalasset.canton.topology.SynchronizerId
import com.digitalasset.canton.tracing.TraceContext
import com.google.protobuf.ByteString

import scala.concurrent.ExecutionContext

class DbSequencerSynchronizerConfigurationStore(
    override protected val storage: DbStorage,
    override protected val timeouts: ProcessingTimeout,
    override protected val loggerFactory: NamedLoggerFactory,
)(implicit executionContext: ExecutionContext)
    extends SequencerSynchronizerConfigurationStore
    with DbStore {

  private type SerializedRow = (String255, ByteString)
  import DbStorage.Implicits.*
  import storage.api.*

  // sentinel value used to ensure the table can only have a single row
  // see create table sql for more details
  private val singleRowLockValue: String1 = String1.fromChar('X')

  override def fetchConfiguration(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, SequencerSynchronizerConfigurationStoreError, Option[
    SequencerSynchronizerConfiguration
  ]] =
    for {
      rowO <- EitherT.right(
        storage
          .query(
            sql"""select synchronizer_id, static_synchronizer_parameters from sequencer_synchronizer_configuration #${storage
                .limit(1)}"""
              .as[SerializedRow]
              .headOption,
            "fetch-configuration",
          )
      )
      config <- EitherT
        .fromEither[FutureUnlessShutdown](rowO.traverse(deserialize))
        .leftMap[SequencerSynchronizerConfigurationStoreError](
          SequencerSynchronizerConfigurationStoreError.DeserializationError.apply
        )
    } yield config

  override def saveConfiguration(configuration: SequencerSynchronizerConfiguration)(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, SequencerSynchronizerConfigurationStoreError, Unit] = {
    val (synchronizerId, synchronizerParameters) = serialize(configuration)

    EitherT.right(
      storage
        .update_(
          storage.profile match {
            case _: DbStorage.Profile.H2 =>
              sqlu"""merge into sequencer_synchronizer_configuration
                   (lock, synchronizer_id, static_synchronizer_parameters)
                   values
                   ($singleRowLockValue, $synchronizerId, $synchronizerParameters)"""
            case _: DbStorage.Profile.Postgres =>
              sqlu"""insert into sequencer_synchronizer_configuration (synchronizer_id, static_synchronizer_parameters)
              values ($synchronizerId, $synchronizerParameters)
              on conflict (lock) do update set synchronizer_id = excluded.synchronizer_id,
                static_synchronizer_parameters = excluded.static_synchronizer_parameters"""
          },
          "save-configuration",
        )
    )
  }

  private def serialize(config: SequencerSynchronizerConfiguration): SerializedRow =
    (
      config.synchronizerId.toLengthLimitedString,
      config.synchronizerParameters.toByteString,
    )

  private def deserialize(
      row: SerializedRow
  ): ParsingResult[SequencerSynchronizerConfiguration] = for {
    synchronizerId <- SynchronizerId.fromProtoPrimitive(row._1.unwrap, "synchronizerId")
    synchronizerParameters <- StaticSynchronizerParameters.fromTrustedByteString(
      row._2
    )
  } yield SequencerSynchronizerConfiguration(synchronizerId, synchronizerParameters)
}
