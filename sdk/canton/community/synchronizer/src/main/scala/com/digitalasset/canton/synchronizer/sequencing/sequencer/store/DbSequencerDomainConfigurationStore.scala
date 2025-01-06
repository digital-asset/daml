// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.synchronizer.sequencing.sequencer.store

import cats.data.EitherT
import cats.syntax.traverse.*
import com.digitalasset.canton.config.CantonRequireTypes.{String1, String255}
import com.digitalasset.canton.config.ProcessingTimeout
import com.digitalasset.canton.logging.NamedLoggerFactory
import com.digitalasset.canton.protocol.StaticDomainParameters
import com.digitalasset.canton.resource.{DbStorage, DbStore}
import com.digitalasset.canton.serialization.ProtoConverter.ParsingResult
import com.digitalasset.canton.topology.SynchronizerId
import com.digitalasset.canton.tracing.TraceContext
import com.google.protobuf.ByteString

import scala.concurrent.{ExecutionContext, Future}

class DbSequencerDomainConfigurationStore(
    override protected val storage: DbStorage,
    override protected val timeouts: ProcessingTimeout,
    override protected val loggerFactory: NamedLoggerFactory,
)(implicit executionContext: ExecutionContext)
    extends SequencerDomainConfigurationStore
    with DbStore {

  private type SerializedRow = (String255, ByteString)
  import DbStorage.Implicits.*
  import storage.api.*

  // sentinel value used to ensure the table can only have a single row
  // see create table sql for more details
  private val singleRowLockValue: String1 = String1.fromChar('X')

  override def fetchConfiguration(implicit
      traceContext: TraceContext
  ): EitherT[Future, SequencerDomainConfigurationStoreError, Option[SequencerDomainConfiguration]] =
    for {
      rowO <- EitherT.right(
        storage
          .query(
            sql"""select synchronizer_id, static_domain_parameters from sequencer_domain_configuration #${storage
                .limit(1)}"""
              .as[SerializedRow]
              .headOption,
            "fetch-configuration",
          )
      )
      config <- EitherT
        .fromEither[Future](rowO.traverse(deserialize))
        .leftMap[SequencerDomainConfigurationStoreError](
          SequencerDomainConfigurationStoreError.DeserializationError.apply
        )
    } yield config

  override def saveConfiguration(configuration: SequencerDomainConfiguration)(implicit
      traceContext: TraceContext
  ): EitherT[Future, SequencerDomainConfigurationStoreError, Unit] = {
    val (synchronizerId, domainParameters) = serialize(configuration)

    EitherT.right(
      storage
        .update_(
          storage.profile match {
            case _: DbStorage.Profile.H2 =>
              sqlu"""merge into sequencer_domain_configuration
                   (lock, synchronizer_id, static_domain_parameters)
                   values
                   ($singleRowLockValue, $synchronizerId, $domainParameters)"""
            case _: DbStorage.Profile.Postgres =>
              sqlu"""insert into sequencer_domain_configuration (synchronizer_id, static_domain_parameters)
              values ($synchronizerId, $domainParameters)
              on conflict (lock) do update set synchronizer_id = excluded.synchronizer_id,
                static_domain_parameters = excluded.static_domain_parameters"""
          },
          "save-configuration",
        )
    )
  }

  private def serialize(config: SequencerDomainConfiguration): SerializedRow =
    (
      config.synchronizerId.toLengthLimitedString,
      config.domainParameters.toByteString,
    )

  private def deserialize(
      row: SerializedRow
  ): ParsingResult[SequencerDomainConfiguration] = for {
    synchronizerId <- SynchronizerId.fromProtoPrimitive(row._1.unwrap, "synchronizerId")
    domainParameters <- StaticDomainParameters.fromTrustedByteString(
      row._2
    )
  } yield SequencerDomainConfiguration(synchronizerId, domainParameters)
}
