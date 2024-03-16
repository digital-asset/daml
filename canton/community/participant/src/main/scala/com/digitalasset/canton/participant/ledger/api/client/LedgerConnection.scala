// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.ledger.api.client

import com.daml.grpc.adapter.ExecutionSequencerFactory
import com.daml.ledger.api.v2.transaction_filter.{
  Filters,
  InclusiveFilters,
  TemplateFilter,
  TransactionFilter as TransactionFilterV2,
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
import io.opentelemetry.instrumentation.grpc.v1_6.GrpcTracing

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
      // the participants should agree on a lower limit and enforce that through domain parameters.
      maxInboundMessageSize = Int.MaxValue,
    )

    val builder = clientChannelConfig
      .builderFor(config.address, config.port.unwrap)
      .executor(ec)
      .intercept(
        GrpcTracing.builder(tracerProvider.openTelemetry).build().newClientInterceptor()
      )
    LedgerClient.withoutToken(builder.build(), clientConfig, loggerFactory)
  }

  def transactionFilterByPartyV2(filter: Map[PartyId, Seq[Identifier]]): TransactionFilterV2 =
    TransactionFilterV2(filter.map {
      case (p, Nil) => p.toParty.getValue -> Filters.defaultInstance
      case (p, ts) =>
        p.toParty.getValue -> Filters(
          Some(
            InclusiveFilters(
              templateFilters = ts.map(tf => TemplateFilter(Some(tf), false))
            )
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
