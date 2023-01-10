// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.nonrepudiation.perf

import java.security.PrivateKey
import java.security.cert.X509Certificate
import java.time.Clock

import com.daml.ledger.api.v1.command_submission_service.CommandSubmissionServiceGrpc
import com.daml.ledger.api.v1.command_submission_service.CommandSubmissionServiceGrpc.CommandSubmissionServiceBlockingStub
import com.daml.nonrepudiation.client.SigningInterceptor
import com.daml.nonrepudiation.testing.DummyTestSetup
import com.daml.nonrepudiation.{
  CertificateRepository,
  CommandIdString,
  NonRepudiationProxy,
  SignedPayloadRepository,
}
import com.daml.resources.grpc.{GrpcResourceOwnerFactories => Resources}
import com.daml.resources.{AbstractResourceOwner, Resource}

import scala.concurrent.ExecutionContext
import scala.concurrent.duration.DurationInt

final class StubOwner private (
    key: PrivateKey,
    certificate: X509Certificate,
    certificates: CertificateRepository,
    signedPayloads: SignedPayloadRepository[CommandIdString],
    builders: DummyTestSetup.Builders,
) extends AbstractResourceOwner[ExecutionContext, CommandSubmissionServiceBlockingStub] {

  override def acquire()(implicit
      context: ExecutionContext
  ): Resource[ExecutionContext, CommandSubmissionServiceBlockingStub] = {

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
        .withInterceptors(SigningInterceptor.signCommands(key, certificate))

    stubOwner.acquire()(context)

  }

}

object StubOwner {

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
      DummyTestSetup.Builders(useNetworkStack, serviceExecutionContext),
    )

}
