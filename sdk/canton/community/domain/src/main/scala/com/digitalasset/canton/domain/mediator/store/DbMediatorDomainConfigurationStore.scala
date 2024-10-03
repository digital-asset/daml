// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.domain.mediator.store

import cats.syntax.either.*
import cats.syntax.traverse.*
import com.digitalasset.canton.config.CantonRequireTypes.{String1, String255}
import com.digitalasset.canton.config.ProcessingTimeout
import com.digitalasset.canton.lifecycle.FutureUnlessShutdown
import com.digitalasset.canton.logging.NamedLoggerFactory
import com.digitalasset.canton.protocol.StaticDomainParameters
import com.digitalasset.canton.resource.{DbStorage, DbStore}
import com.digitalasset.canton.sequencing.SequencerConnections
import com.digitalasset.canton.serialization.ProtoConverter.ParsingResult
import com.digitalasset.canton.store.db.DbDeserializationException
import com.digitalasset.canton.topology.DomainId
import com.digitalasset.canton.tracing.TraceContext
import com.google.protobuf.ByteString

import scala.concurrent.ExecutionContext

class DbMediatorDomainConfigurationStore(
    override protected val storage: DbStorage,
    override protected val timeouts: ProcessingTimeout,
    override protected val loggerFactory: NamedLoggerFactory,
)(implicit executionContext: ExecutionContext)
    extends MediatorDomainConfigurationStore
    with DbStore {

  private type SerializedRow = (String255, ByteString, ByteString)
  import DbStorage.Implicits.*
  import storage.api.*

  // sentinel value used to ensure the table can only have a single row
  // see create table sql for more details
  protected val singleRowLockValue: String1 = String1.fromChar('X')

  override def fetchConfiguration(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[Option[MediatorDomainConfiguration]] =
    for {
      rowO <-
        storage
          .queryUnlessShutdown(
            sql"""select domain_id, static_domain_parameters, sequencer_connection
            from mediator_domain_configuration #${storage.limit(1)}""".as[SerializedRow].headOption,
            "fetch-configuration",
          )

      config = rowO
        .traverse(deserialize)
        .valueOr(err =>
          throw new DbDeserializationException(
            s"Failed to deserialize mediator domain configuration: $err"
          )
        )
    } yield config

  override def saveConfiguration(configuration: MediatorDomainConfiguration)(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[Unit] = {
    val (domainId, domainParameters, sequencerConnection) = serialize(
      configuration
    )

    storage
      .updateUnlessShutdown_(
        storage.profile match {
          case _: DbStorage.Profile.H2 =>
            sqlu"""merge into mediator_domain_configuration
                   (lock, domain_id, static_domain_parameters, sequencer_connection)
                   values
                   ($singleRowLockValue, $domainId, $domainParameters, $sequencerConnection)"""
          case _: DbStorage.Profile.Postgres =>
            sqlu"""insert into mediator_domain_configuration (domain_id, static_domain_parameters, sequencer_connection)
              values ($domainId, $domainParameters, $sequencerConnection)
              on conflict (lock) do update set
                domain_id = excluded.domain_id,
                static_domain_parameters = excluded.static_domain_parameters,
                sequencer_connection = excluded.sequencer_connection"""
        },
        "save-configuration",
      )
  }

  private def serialize(config: MediatorDomainConfiguration): SerializedRow = {
    val MediatorDomainConfiguration(
      domainId,
      domainParameters,
      sequencerConnections,
    ) = config
    (
      domainId.toLengthLimitedString,
      domainParameters.toByteString,
      sequencerConnections.toByteString(domainParameters.protocolVersion),
    )
  }

  private def deserialize(
      row: SerializedRow
  ): ParsingResult[MediatorDomainConfiguration] =
    for {
      domainId <- DomainId.fromProtoPrimitive(row._1.unwrap, "domainId")
      domainParameters <- StaticDomainParameters.fromTrustedByteString(
        row._2
      )
      sequencerConnections <- SequencerConnections.fromTrustedByteString(row._3)
    } yield MediatorDomainConfiguration(
      domainId,
      domainParameters,
      sequencerConnections,
    )
}
