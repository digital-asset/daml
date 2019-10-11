// Copyright (c) 2019 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.http

import akka.NotUsed
import akka.stream.scaladsl.Source
import com.digitalasset.grpc.adapter.ExecutionSequencerFactory
import com.digitalasset.http.util.FutureUtil.toFuture
import com.digitalasset.jwt.domain.Jwt
import com.digitalasset.ledger.api.v1.active_contracts_service.GetActiveContractsResponse
import com.digitalasset.ledger.api.v1.command_service.{
  SubmitAndWaitForTransactionResponse,
  SubmitAndWaitRequest
}
import com.digitalasset.ledger.api.v1.transaction_filter.TransactionFilter
import com.digitalasset.ledger.client.LedgerClient
import com.digitalasset.ledger.client.configuration.LedgerClientConfiguration
import io.grpc.netty.{NegotiationType, NettyChannelBuilder}
import io.grpc.stub.MetadataUtils
import io.grpc.{Channel, ClientInterceptors, Metadata}
import scalaz.\/

import scala.concurrent.{ExecutionContext, Future}

object LedgerClientJwt {

  type SubmitAndWaitForTransaction =
    (Jwt, SubmitAndWaitRequest) => Future[SubmitAndWaitForTransactionResponse]

  type GetActiveContracts =
    (Jwt, TransactionFilter, Boolean) => Source[GetActiveContractsResponse, NotUsed]

  def singleHostChannel(
      hostIp: String,
      port: Int,
      configuration: LedgerClientConfiguration,
      maxInboundMessageSize: Int)(
      implicit ec: ExecutionContext,
      esf: ExecutionSequencerFactory): Throwable \/ Channel = \/.fromTryCatchNonFatal {

    val builder: NettyChannelBuilder = NettyChannelBuilder
      .forAddress(hostIp, port)
      .maxInboundMessageSize(maxInboundMessageSize)

    configuration.sslContext
      .fold {
        builder.usePlaintext()
      } { sslContext =>
        builder.sslContext(sslContext).negotiationType(NegotiationType.TLS)
      }

    val channel = builder.build()

    val _ = sys.addShutdownHook { val _ = channel.shutdownNow() }

    channel
  }

  def forChannel(jwt: Jwt, configuration: LedgerClientConfiguration, channel: Channel)(
      implicit ec: ExecutionContext,
      esf: ExecutionSequencerFactory
  ): Future[LedgerClient] =
    for {
      channelWithJwt <- toFuture(decorateChannel(channel, jwt))
      clientWithJwt <- LedgerClient.forChannel(configuration, channelWithJwt)
    } yield clientWithJwt

  // TODO(Leo): all examples show it as `authorization` but not `Authorization`, why???
  private val authorizationKey = Metadata.Key.of("authorization", Metadata.ASCII_STRING_MARSHALLER)

  private def decorateChannel(channel: Channel, jwt: Jwt): Throwable \/ Channel =
    \/.fromTryCatchNonFatal {
      val extraHeaders = new Metadata()
      //  `Authorization: Bearer json-web-token`.
      extraHeaders.put(authorizationKey, s"Bearer ${jwt.value: String}")
      val interceptor = MetadataUtils.newAttachHeadersInterceptor(extraHeaders)
      ClientInterceptors.intercept(channel, interceptor)
    }

  def submitAndWaitForTransaction(config: LedgerClientConfiguration, channel: io.grpc.Channel)(
      implicit ec: ExecutionContext,
      esf: ExecutionSequencerFactory): SubmitAndWaitForTransaction =
    (jwt, req) =>
      forChannel(jwt, config, channel)
        .flatMap(_.commandServiceClient.submitAndWaitForTransaction(req))
  @SuppressWarnings(Array("org.wartremover.warts.Any"))
  def getActiveContracts(config: LedgerClientConfiguration, channel: io.grpc.Channel)(
      implicit ec: ExecutionContext,
      esf: ExecutionSequencerFactory): GetActiveContracts =
    (jwt, filter, flag) =>
      Source
        .fromFuture(forChannel(jwt, config, channel))
        .flatMapConcat(client => client.activeContractSetClient.getActiveContracts(filter, flag))
}
