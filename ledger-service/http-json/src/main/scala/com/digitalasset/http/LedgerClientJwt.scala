// Copyright (c) 2019 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.http

import com.digitalasset.grpc.adapter.ExecutionSequencerFactory
import com.digitalasset.http.util.FutureUtil.toFuture
import com.digitalasset.jwt.domain.Jwt
import com.digitalasset.ledger.client.LedgerClient
import com.digitalasset.ledger.client.configuration.LedgerClientConfiguration
import io.grpc.netty.{NegotiationType, NettyChannelBuilder}
import io.grpc.stub.MetadataUtils
import io.grpc.{Channel, ClientInterceptors, Metadata}
import scalaz.\/

import scala.concurrent.{ExecutionContext, Future}

object LedgerClientJwt {

  def singleHostChannel(hostIp: String, port: Int, configuration: LedgerClientConfiguration)(
      implicit ec: ExecutionContext,
      esf: ExecutionSequencerFactory): Throwable \/ Channel = \/.fromTryCatchNonFatal {

    val builder: NettyChannelBuilder = NettyChannelBuilder
      .forAddress(hostIp, port)

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

  def forChannel(configuration: LedgerClientConfiguration, channel: Channel)(jwt: Jwt)(
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
}
