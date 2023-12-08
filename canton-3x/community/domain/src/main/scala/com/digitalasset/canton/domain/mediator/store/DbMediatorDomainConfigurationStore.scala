// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.domain.mediator.store

import cats.data.EitherT
import cats.syntax.traverse.*
import com.digitalasset.canton.config.CantonRequireTypes.{String1, String255, String68}
import com.digitalasset.canton.config.ProcessingTimeout
import com.digitalasset.canton.crypto.Fingerprint
import com.digitalasset.canton.logging.NamedLoggerFactory
import com.digitalasset.canton.protocol.StaticDomainParameters
import com.digitalasset.canton.resource.{DbStorage, DbStore}
import com.digitalasset.canton.sequencing.SequencerConnections
import com.digitalasset.canton.serialization.ProtoConverter.ParsingResult
import com.digitalasset.canton.topology.DomainId
import com.digitalasset.canton.tracing.TraceContext
import com.google.protobuf.ByteString

import scala.concurrent.{ExecutionContext, Future}

class DbMediatorDomainConfigurationStore(
    override protected val storage: DbStorage,
    override protected val timeouts: ProcessingTimeout,
    override protected val loggerFactory: NamedLoggerFactory,
)(implicit executionContext: ExecutionContext)
    extends MediatorDomainConfigurationStore
    with DbStore {

  private type SerializedRow = (String68, String255, ByteString, ByteString)
  import DbStorage.Implicits.*
  import storage.api.*

  // sentinel value used to ensure the table can only have a single row
  // see create table sql for more details
  protected val singleRowLockValue: String1 = String1.fromChar('X')

  override def fetchConfiguration(implicit
      traceContext: TraceContext
  ): EitherT[Future, MediatorDomainConfigurationStoreError, Option[MediatorDomainConfiguration]] =
    for {
      rowO <- EitherT.right(
        storage
          .query(
            sql"""select initial_key_context, domain_id, static_domain_parameters, sequencer_connection
            from mediator_domain_configuration #${storage.limit(1)}""".as[SerializedRow].headOption,
            "fetch-configuration",
          )
      )
      config <- EitherT
        .fromEither[Future](rowO.traverse(deserialize))
        .leftMap[MediatorDomainConfigurationStoreError](
          MediatorDomainConfigurationStoreError.DeserializationError
        )
    } yield config

  override def saveConfiguration(configuration: MediatorDomainConfiguration)(implicit
      traceContext: TraceContext
  ): EitherT[Future, MediatorDomainConfigurationStoreError, Unit] = {
    val (initialKeyContext, domainId, domainParameters, sequencerConnection) = serialize(
      configuration
    )

    EitherT.right(
      storage
        .update_(
          storage.profile match {
            case _: DbStorage.Profile.H2 =>
              sqlu"""merge into mediator_domain_configuration
                   (lock, initial_key_context, domain_id, static_domain_parameters, sequencer_connection)
                   values
                   ($singleRowLockValue, $initialKeyContext, $domainId, $domainParameters, $sequencerConnection)"""
            case _: DbStorage.Profile.Postgres =>
              sqlu"""insert into mediator_domain_configuration (initial_key_context, domain_id, static_domain_parameters, sequencer_connection)
              values ($initialKeyContext, $domainId, $domainParameters, $sequencerConnection)
              on conflict (lock) do update set initial_key_context = excluded.initial_key_context,
                domain_id = excluded.domain_id,
                static_domain_parameters = excluded.static_domain_parameters,
                sequencer_connection = excluded.sequencer_connection"""
            case _: DbStorage.Profile.Oracle =>
              sqlu"""merge into mediator_domain_configuration mdc
                      using (
                        select
                          $initialKeyContext initial_key_context,
                          $domainId domain_id,
                          $domainParameters static_domain_parameters,
                          $sequencerConnection sequencer_connection
                          from dual
                          ) excluded
                      on (mdc."LOCK" = 'X')
                       when matched then
                        update set mdc.initial_key_context = excluded.initial_key_context,
                          mdc.domain_id = excluded.domain_id,
                          mdc.static_domain_parameters = excluded.static_domain_parameters,
                          mdc.sequencer_connection = excluded.sequencer_connection
                       when not matched then
                        insert (initial_key_context, domain_id, static_domain_parameters, sequencer_connection)
                        values (excluded.initial_key_context, excluded.domain_id, excluded.static_domain_parameters, excluded.sequencer_connection)
                     """

          },
          "save-configuration",
        )
    )
  }

  private def serialize(config: MediatorDomainConfiguration): SerializedRow = {
    val MediatorDomainConfiguration(
      initialKeyFingerprint,
      domainId,
      domainParameters,
      sequencerConnections,
    ) = config
    (
      initialKeyFingerprint.toLengthLimitedString,
      domainId.toLengthLimitedString,
      domainParameters.toByteString,
      sequencerConnections.toByteString(domainParameters.protocolVersion),
    )
  }

  private def deserialize(
      row: SerializedRow
  ): ParsingResult[MediatorDomainConfiguration] = {
    for {
      initialKeyFingerprint <- Fingerprint.fromProtoPrimitive(row._1.unwrap)
      domainId <- DomainId.fromProtoPrimitive(row._2.unwrap, "domainId")
      domainParameters <- StaticDomainParameters.fromByteString(row._3)
      sequencerConnections <- SequencerConnections.fromByteString(row._4)
    } yield MediatorDomainConfiguration(
      initialKeyFingerprint,
      domainId,
      domainParameters,
      sequencerConnections,
    )
  }
}
