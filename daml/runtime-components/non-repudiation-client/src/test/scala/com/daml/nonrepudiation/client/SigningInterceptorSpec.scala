// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.nonrepudiation.client

import java.time.{Clock, Instant, ZoneId}
import java.util.Collections

import com.daml.ledger.api.v1.CommandServiceOuterClass.SubmitAndWaitRequest
import com.daml.ledger.javaapi.data.Command
import com.daml.ledger.api.v1.CommandsOuterClass.{Command => ProtoCommand}
import com.daml.ledger.api.v1.command_service.CommandServiceGrpc.CommandService
import com.daml.ledger.api.v1.command_submission_service.CommandSubmissionServiceGrpc.CommandSubmissionService
import com.daml.ledger.rxjava.DamlLedgerClient
import com.daml.nonrepudiation.testing._
import com.daml.nonrepudiation.{
  AlgorithmString,
  CommandIdString,
  NonRepudiationProxy,
  SignatureBytes,
  SignedPayload,
}
import com.daml.resources.grpc.{GrpcResourceOwnerFactories => Resources}
import io.grpc.netty.NettyChannelBuilder
import org.scalatest.Inside
import org.scalatest.flatspec.AsyncFlatSpec
import org.scalatest.matchers.should.Matchers

import scala.concurrent.ExecutionContext
import scala.concurrent.duration.DurationInt

final class SigningInterceptorSpec extends AsyncFlatSpec with Matchers with Inside {

  behavior of "SigningInterceptor"

  it should "be possible to use it with the Java bindings" in {
    val (key, certificate) = generateKeyAndCertificate()
    val builders =
      DummyTestSetup.Builders(
        useNetworkStack = true,
        serviceExecutionContext = ExecutionContext.global,
      )

    val certificates = new Certificates
    val signedPayloads = new SignedPayloads[CommandIdString]

    val expectedAlgorithm = AlgorithmString.SHA256withRSA
    val expectedFingerprint = certificates.put(certificate)
    val expectedTimestamp = Instant.now()

    val proxy =
      for {
        _ <- Resources.forServer(builders.participantServer, 5.seconds)
        participant <- Resources.forChannel(builders.participantChannel, 5.seconds)
        proxy <- NonRepudiationProxy.owner(
          participant,
          builders.proxyServer,
          certificates,
          signedPayloads,
          Clock.fixed(expectedTimestamp, ZoneId.systemDefault()),
          CommandService.scalaDescriptor.fullName,
          CommandSubmissionService.scalaDescriptor.fullName,
        )
      } yield proxy

    proxy.use { _ =>
      val clientChannelBuilder =
        builders.proxyChannel
          .asInstanceOf[NettyChannelBuilder]
          .intercept(SigningInterceptor.signCommands(key, certificate))
      val client = DamlLedgerClient.newBuilder(clientChannelBuilder).build()
      client.connect()
      val command = ProtoCommand.parseFrom(generateCommand().getCommands.commands.head.toByteArray)
      val expectedWorkflowId = "workflow-id"
      val expectedApplicationId = "application-id"
      val expectedCommandId = "command-id"
      val expectedParty = "party-1"
      client.getCommandClient
        .submitAndWait(
          expectedWorkflowId,
          expectedApplicationId,
          expectedCommandId,
          expectedParty,
          Collections.singletonList(Command.fromProtoCommand(command)),
        )
        .blockingGet()

      withClue("Only the command should be signed, not the call to the ledger identity service") {
        signedPayloads.size shouldBe 1
      }

      inside(signedPayloads.get(CommandIdString.wrap("command-id"))) {
        case Seq(SignedPayload(algorithm, fingerprint, payload, signature, timestamp)) =>
          val commands = SubmitAndWaitRequest.parseFrom(payload.unsafeArray).getCommands
          algorithm shouldBe expectedAlgorithm
          fingerprint shouldBe expectedFingerprint
          commands.getWorkflowId shouldBe expectedWorkflowId
          commands.getApplicationId shouldBe expectedApplicationId
          commands.getCommandId shouldBe expectedCommandId
          commands.getParty shouldBe expectedParty
          commands.getCommandsCount shouldBe 1
          signature shouldBe SignatureBytes.sign(expectedAlgorithm, key, payload)
          timestamp shouldBe expectedTimestamp
      }
    }
  }

}
