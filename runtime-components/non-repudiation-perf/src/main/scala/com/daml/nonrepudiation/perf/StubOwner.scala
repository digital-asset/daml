// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.nonrepudiation.perf

import java.net.{InetAddress, InetSocketAddress, SocketAddress}
import java.security.PrivateKey
import java.security.cert.X509Certificate
import java.time.Clock

import com.daml.ledger.api.v1.command_submission_service.CommandSubmissionServiceGrpc
import com.daml.ledger.api.v1.command_submission_service.CommandSubmissionServiceGrpc.CommandSubmissionServiceBlockingStub
import com.daml.nonrepudiation.client.SigningInterceptor
import com.daml.nonrepudiation.{
  CertificateRepository,
  CommandIdString,
  NonRepudiationProxy,
  SignedPayloadRepository,
}
import com.daml.ports.FreePort
import com.daml.resources.grpc.{GrpcResourceOwnerFactories => Resources}
import com.daml.resources.{AbstractResourceOwner, Resource}
import io.grpc.inprocess.{InProcessChannelBuilder, InProcessServerBuilder}
import io.grpc.netty.{NettyChannelBuilder, NettyServerBuilder}
import io.grpc.protobuf.services.ProtoReflectionService
import io.grpc.{ManagedChannelBuilder, ServerBuilder}

import scala.concurrent.ExecutionContext
import scala.concurrent.duration.DurationInt

final class StubOwner private (
    key: PrivateKey,
    certificate: X509Certificate,
    certificates: CertificateRepository,
    signedPayloads: SignedPayloadRepository[CommandIdString],
    builders: StubOwner.Builders,
    serviceExecutionContext: ExecutionContext,
) extends AbstractResourceOwner[ExecutionContext, CommandSubmissionServiceBlockingStub] {

  override def acquire()(implicit
      context: ExecutionContext
  ): Resource[ExecutionContext, CommandSubmissionServiceBlockingStub] = {

    builders.participantServer
      .addService(DummyCommandSubmissionService.bind(serviceExecutionContext))
      .addService(ProtoReflectionService.newInstance())

    val stubOwner =
      for {
        _ <- Resources.forServer(builders.participantServer, 5.seconds)
        participant <- Resources.forChannel(builders.participantChannel, 5.seconds)
        _ <- NonRepudiationProxy.owner(
          participant = participant,
          serverBuilder = builders.proxyServer,
          certificateRepository = certificates,
          signedPayloadRepository = signedPayloads,
          timestampProvider = Clock.systemUTC(),
          serviceName = CommandSubmissionServiceGrpc.SERVICE.getName,
        )
      } yield CommandSubmissionServiceGrpc
        .blockingStub(builders.proxyChannel.build())
        .withInterceptors(new SigningInterceptor(key, certificate))

    stubOwner.acquire()(context)

  }

}

object StubOwner {

  final class Builders(
      val participantServer: ServerBuilder[_ <: ServerBuilder[_]],
      val participantChannel: ManagedChannelBuilder[_ <: ManagedChannelBuilder[_]],
      val proxyServer: ServerBuilder[_ <: ServerBuilder[_]],
      val proxyChannel: ManagedChannelBuilder[_ <: ManagedChannelBuilder[_]],
  )

  object Builders {

    def apply(useNetworkStack: Boolean): StubOwner.Builders =
      if (useNetworkStack) {
        val participantPort: Int = FreePort.find().value
        val participantAddress: SocketAddress =
          new InetSocketAddress(InetAddress.getLoopbackAddress, participantPort)
        val proxyPort: Int = FreePort.find().value
        val proxyAddress: SocketAddress =
          new InetSocketAddress(InetAddress.getLoopbackAddress, proxyPort)
        new Builders(
          NettyServerBuilder.forPort(participantPort),
          NettyChannelBuilder.forAddress(participantAddress).usePlaintext(),
          NettyServerBuilder.forPort(proxyPort),
          NettyChannelBuilder.forAddress(proxyAddress).usePlaintext(),
        )
      } else {
        val participantName: String = InProcessServerBuilder.generateName()
        val proxyName: String = InProcessServerBuilder.generateName()
        new Builders(
          InProcessServerBuilder.forName(participantName),
          InProcessChannelBuilder.forName(participantName),
          InProcessServerBuilder.forName(proxyName),
          InProcessChannelBuilder.forName(proxyName),
        )
      }

  }

  def apply(
      useNetworkStack: Boolean,
      key: PrivateKey,
      certificate: X509Certificate,
      certificates: CertificateRepository,
      signedPayloads: SignedPayloadRepository[CommandIdString],
      serviceExecutionContext: ExecutionContext,
  ): AbstractResourceOwner[ExecutionContext, CommandSubmissionServiceBlockingStub] =
    new StubOwner(
      key,
      certificate,
      certificates,
      signedPayloads,
      Builders(useNetworkStack),
      serviceExecutionContext,
    )

}
