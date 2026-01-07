// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.ledger.api.client

import com.daml.grpc.adapter.ExecutionSequencerFactory
import com.daml.ledger.api.v2.transaction_filter.CumulativeFilter.IdentifierFilter
import com.daml.ledger.api.v2.transaction_filter.{
  CumulativeFilter,
  EventFormat,
  Filters,
  TemplateFilter,
}
import com.daml.ledger.api.v2.value.Identifier
import com.daml.ledger.javaapi
import com.digitalasset.canton.auth.CantonAdminTokenDispenser
import com.digitalasset.canton.config.ClientConfig
import com.digitalasset.canton.ledger.api.refinements.ApiTypes.UserId
import com.digitalasset.canton.ledger.client.LedgerClient
import com.digitalasset.canton.ledger.client.configuration.{
  CommandClientConfiguration,
  LedgerClientChannelConfiguration,
  LedgerClientConfiguration,
}
import com.digitalasset.canton.logging.NamedLoggerFactory
import com.digitalasset.canton.networking.grpc.ClientChannelBuilder
import com.digitalasset.canton.topology.PartyId
import com.digitalasset.canton.tracing.{TraceContextGrpc, TracerProvider}
import io.opentelemetry.instrumentation.grpc.v1_6.GrpcTelemetry

import scala.concurrent.ExecutionContextExecutor

object LedgerConnection {
  def createLedgerClient(
      userId: UserId,
      config: ClientConfig,
      commandClientConfiguration: CommandClientConfiguration,
      tracerProvider: TracerProvider,
      loggerFactory: NamedLoggerFactory,
      tokenDispenser: Option[CantonAdminTokenDispenser] = None,
  )(implicit
      ec: ExecutionContextExecutor,
      executionSequencerFactory: ExecutionSequencerFactory,
  ): LedgerClient = {
    val clientConfig = LedgerClientConfiguration(
      userId = UserId.unwrap(userId),
      commandClient = commandClientConfiguration,
      token = () => tokenDispenser.map(_.getCurrentToken.secret),
    )
    val clientChannelConfig = LedgerClientChannelConfiguration(
      sslContext = config.tlsConfig.map(x => ClientChannelBuilder.sslContext(x)),
      // Hard-coding the maximum value (= 2GB).
      // If a limit is needed, because an application can't handle transactions at that size,
      // the participants should agree on a lower limit and enforce that through synchronizer parameters.
      maxInboundMessageSize = Int.MaxValue,
    )

    val builder = clientChannelConfig
      .builderFor(config.address, config.port.unwrap)
      .executor(ec)
      .intercept(
        TraceContextGrpc.clientInterceptor(
          Some(GrpcTelemetry.builder(tracerProvider.openTelemetry).build().newClientInterceptor())
        )
      )
    LedgerClient.withoutToken(builder.build(), clientConfig, loggerFactory)
  }

  def eventFormatByParty(filter: Map[PartyId, Seq[Identifier]]): EventFormat =
    EventFormat(
      filtersByParty = filter.map {
        case (p, Nil) => p.toProtoPrimitive -> Filters.defaultInstance
        case (p, ts) =>
          p.toProtoPrimitive -> Filters(
            ts.map(tf =>
              CumulativeFilter(IdentifierFilter.TemplateFilter(TemplateFilter(Some(tf), false)))
            )
          )
      },
      filtersForAnyParty = None,
      verbose = false,
    )

  def mapTemplateIds(id: javaapi.data.Identifier): Identifier =
    Identifier(
      packageId = id.getPackageId,
      moduleName = id.getModuleName,
      entityName = id.getEntityName,
    )

}
