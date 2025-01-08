// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.ledger.api.client

import com.daml.grpc.adapter.ExecutionSequencerFactory
import com.daml.ledger.api.v2.transaction_filter.CumulativeFilter.IdentifierFilter
import com.daml.ledger.api.v2.transaction_filter.{
  CumulativeFilter,
  Filters,
  TemplateFilter,
  TransactionFilter,
}
import com.daml.ledger.api.v2.value.Identifier
import com.daml.ledger.javaapi
import com.digitalasset.canton.config.ClientConfig
import com.digitalasset.canton.ledger.api.refinements.ApiTypes.ApplicationId
import com.digitalasset.canton.ledger.client.LedgerClient
import com.digitalasset.canton.ledger.client.configuration.{
  CommandClientConfiguration,
  LedgerClientChannelConfiguration,
  LedgerClientConfiguration,
}
import com.digitalasset.canton.logging.NamedLoggerFactory
import com.digitalasset.canton.networking.grpc.ClientChannelBuilder
import com.digitalasset.canton.topology.PartyId
import com.digitalasset.canton.tracing.TracerProvider
import io.opentelemetry.instrumentation.grpc.v1_6.GrpcTelemetry

import scala.concurrent.ExecutionContextExecutor

object LedgerConnection {
  def createLedgerClient(
      applicationId: ApplicationId,
      config: ClientConfig,
      commandClientConfiguration: CommandClientConfiguration,
      tracerProvider: TracerProvider,
      loggerFactory: NamedLoggerFactory,
      token: Option[String] = None,
  )(implicit
      ec: ExecutionContextExecutor,
      executionSequencerFactory: ExecutionSequencerFactory,
  ): LedgerClient = {
    val clientConfig = LedgerClientConfiguration(
      applicationId = ApplicationId.unwrap(applicationId),
      commandClient = commandClientConfiguration,
      token = token,
    )
    val clientChannelConfig = LedgerClientChannelConfiguration(
      sslContext = config.tls.map(x => ClientChannelBuilder.sslContext(x)),
      // Hard-coding the maximum value (= 2GB).
      // If a limit is needed, because an application can't handle transactions at that size,
      // the participants should agree on a lower limit and enforce that through synchronizer parameters.
      maxInboundMessageSize = Int.MaxValue,
    )

    val builder = clientChannelConfig
      .builderFor(config.address, config.port.unwrap)
      .executor(ec)
      .intercept(
        GrpcTelemetry.builder(tracerProvider.openTelemetry).build().newClientInterceptor()
      )
    LedgerClient.withoutToken(builder.build(), clientConfig, loggerFactory)
  }

  def transactionFilterByParty(filter: Map[PartyId, Seq[Identifier]]): TransactionFilter =
    TransactionFilter(filter.map {
      case (p, Nil) => p.toProtoPrimitive -> Filters.defaultInstance
      case (p, ts) =>
        p.toProtoPrimitive -> Filters(
          ts.map(tf =>
            CumulativeFilter(IdentifierFilter.TemplateFilter(TemplateFilter(Some(tf), false)))
          )
        )
    })

  def mapTemplateIds(id: javaapi.data.Identifier): Identifier =
    Identifier(
      packageId = id.getPackageId,
      moduleName = id.getModuleName,
      entityName = id.getEntityName,
    )

}
