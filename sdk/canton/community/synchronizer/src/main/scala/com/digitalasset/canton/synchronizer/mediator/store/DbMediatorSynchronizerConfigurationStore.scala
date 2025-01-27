// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.synchronizer.mediator.store

import cats.syntax.either.*
import cats.syntax.traverse.*
import com.digitalasset.canton.config.CantonRequireTypes.{String1, String255}
import com.digitalasset.canton.config.ProcessingTimeout
import com.digitalasset.canton.lifecycle.FutureUnlessShutdown
import com.digitalasset.canton.logging.NamedLoggerFactory
import com.digitalasset.canton.protocol.StaticSynchronizerParameters
import com.digitalasset.canton.resource.{DbStorage, DbStore}
import com.digitalasset.canton.sequencing.SequencerConnections
import com.digitalasset.canton.serialization.ProtoConverter.ParsingResult
import com.digitalasset.canton.store.db.DbDeserializationException
import com.digitalasset.canton.topology.SynchronizerId
import com.digitalasset.canton.tracing.TraceContext
import com.google.protobuf.ByteString

import scala.concurrent.ExecutionContext

class DbMediatorSynchronizerConfigurationStore(
    override protected val storage: DbStorage,
    override protected val timeouts: ProcessingTimeout,
    override protected val loggerFactory: NamedLoggerFactory,
)(implicit executionContext: ExecutionContext)
    extends MediatorSynchronizerConfigurationStore
    with DbStore {

  private type SerializedRow = (String255, ByteString, ByteString)
  import DbStorage.Implicits.*
  import storage.api.*

  // sentinel value used to ensure the table can only have a single row
  // see create table sql for more details
  protected val singleRowLockValue: String1 = String1.fromChar('X')

  override def fetchConfiguration(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[Option[MediatorSynchronizerConfiguration]] =
    for {
      rowO <-
        storage
          .query(
            sql"""select synchronizer_id, static_synchronizer_parameters, sequencer_connection
            from mediator_synchronizer_configuration #${storage
                .limit(1)}""".as[SerializedRow].headOption,
            "fetch-configuration",
          )

      config = rowO
        .traverse(deserialize)
        .valueOr(err =>
          throw new DbDeserializationException(
            s"Failed to deserialize mediator synchronizer configuration: $err"
          )
        )
    } yield config

  override def saveConfiguration(configuration: MediatorSynchronizerConfiguration)(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[Unit] = {
    val (synchronizerId, synchronizerParameters, sequencerConnection) = serialize(
      configuration
    )

    storage
      .update_(
        storage.profile match {
          case _: DbStorage.Profile.H2 =>
            sqlu"""merge into mediator_synchronizer_configuration
                   (lock, synchronizer_id, static_synchronizer_parameters, sequencer_connection)
                   values
                   ($singleRowLockValue, $synchronizerId, $synchronizerParameters, $sequencerConnection)"""
          case _: DbStorage.Profile.Postgres =>
            sqlu"""insert into mediator_synchronizer_configuration (synchronizer_id, static_synchronizer_parameters, sequencer_connection)
              values ($synchronizerId, $synchronizerParameters, $sequencerConnection)
              on conflict (lock) do update set
                synchronizer_id = excluded.synchronizer_id,
                static_synchronizer_parameters = excluded.static_synchronizer_parameters,
                sequencer_connection = excluded.sequencer_connection"""
        },
        "save-configuration",
      )
  }

  private def serialize(config: MediatorSynchronizerConfiguration): SerializedRow = {
    val MediatorSynchronizerConfiguration(
      synchronizerId,
      synchronizerParameters,
      sequencerConnections,
    ) = config
    (
      synchronizerId.toLengthLimitedString,
      synchronizerParameters.toByteString,
      sequencerConnections.toByteString(synchronizerParameters.protocolVersion),
    )
  }

  private def deserialize(
      row: SerializedRow
  ): ParsingResult[MediatorSynchronizerConfiguration] =
    for {
      synchronizerId <- SynchronizerId.fromProtoPrimitive(row._1.unwrap, "synchronizerId")
      synchronizerParameters <- StaticSynchronizerParameters.fromTrustedByteString(
        row._2
      )
      sequencerConnections <- SequencerConnections.fromTrustedByteString(row._3)
    } yield MediatorSynchronizerConfiguration(
      synchronizerId,
      synchronizerParameters,
      sequencerConnections,
    )
}
