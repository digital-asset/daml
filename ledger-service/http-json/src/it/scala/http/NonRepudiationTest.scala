// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.http

import java.nio.file.{Files, Path}
import java.security.cert.X509Certificate
import java.time.Clock
import java.util.UUID

import akka.http.scaladsl.model.{StatusCodes, Uri}
import com.daml.doobie.logging.Slf4jLogHandler
import com.daml.http.AbstractHttpServiceIntegrationTestFuns.{dar1, dar2}
import com.daml.http.json.{DomainJsonDecoder, DomainJsonEncoder}
import com.daml.ledger.api.v1.command_service.CommandServiceGrpc
import com.daml.ledger.api.v1.command_submission_service.{
  CommandSubmissionServiceGrpc,
  SubmitRequest,
}
import com.daml.ledger.api.v1.value.Value.Sum
import com.daml.ledger.api.v1.value.{RecordField, Value}
import com.daml.ledger.client.LedgerClient
import com.daml.nonrepudiation.postgresql.{Tables, createTransactor}
import com.daml.nonrepudiation.testing.generateKeyAndCertificate
import com.daml.nonrepudiation.{CommandIdString, NonRepudiationProxy}
import com.daml.ports.{FreePort, Port}
import com.daml.resources.grpc.GrpcResourceOwnerFactories
import com.daml.testing.postgresql.PostgresAroundEach
import io.grpc.Server
import io.grpc.netty.{NettyChannelBuilder, NettyServerBuilder}
import org.scalatest.freespec.AsyncFreeSpec
import org.scalatest.matchers.should.Matchers
import org.scalatest.{Assertion, BeforeAndAfterEach, Inside}

import scala.concurrent.Future
import scala.concurrent.duration.DurationInt

@SuppressWarnings(Array("org.wartremover.warts.NonUnitStatements"))
final class NonRepudiationTest
    extends AsyncFreeSpec
    with Matchers
    with Inside
    with BeforeAndAfterEach
    with AbstractHttpServiceIntegrationTestFuns
    with PostgresAroundEach {

  import HttpServiceTestFixture._

  private var certificatePath: Path = _
  private var privateKeyPath: Path = _
  private var privateKeyAlgorithm: String = _
  private var certificate: X509Certificate = _

  override protected def beforeEach(): Unit = {
    super.beforeEach()
    val (key, cert) = generateKeyAndCertificate()
    certificate = cert
    certificatePath = Files.createTempFile("non-repudiation-test", "certificate")
    privateKeyPath = Files.createTempFile("non-repudiation-test", "key")
    Files.write(certificatePath, cert.getEncoded)
    Files.write(privateKeyPath, key.getEncoded)
    privateKeyAlgorithm = key.getAlgorithm
  }

  override protected def afterEach(): Unit = {
    super.afterEach()
    Files.delete(certificatePath)
    Files.delete(privateKeyPath)
  }

  override val jdbcConfig: Option[JdbcConfig] = None

  override val staticContentConfig: Option[StaticContentConfig] = None

  override val useTls: UseTls = UseTls.NoTls

  override val wsConfig: Option[WebsocketConfig] = None

  "correctly sign a command" in withSetup { (db, uri, encoder) =>
    val expectedParty = "Alice"
    val expectedNumber = "abc123"
    val expectedCommandId = UUID.randomUUID.toString
    val meta = Some(domain.CommandMeta(commandId = Some(domain.CommandId(expectedCommandId))))
    val domainParty = domain.Party(expectedParty)
    val command = accountCreateCommand(domainParty, expectedNumber).copy(meta = meta)
    postCreateCommand(command, encoder, uri)
      .flatMap { case (status, _) =>
        status shouldBe StatusCodes.OK
        val payloads = db.signedPayloads.get(CommandIdString.wrap(expectedCommandId))
        payloads should have size 1
        val signedCommand = SubmitRequest.parseFrom(payloads.head.payload.unsafeArray)
        val commands = signedCommand.getCommands.commands
        commands should have size 1
        val actualFields = commands.head.getCreate.getCreateArguments.fields.map(stripIdentifiers)
        val expectedFields = command.payload.fields.map(stripIdentifiers)
        actualFields should contain theSameElementsAs expectedFields
      }
  }

  private def stripIdentifiers(field: RecordField): RecordField =
    field.copy(value = Some(stripIdentifiers(field.getValue)))

  // Doesn't aim at being complete, neither in stripping identifiers recursively
  // Only covers variant because it's the only case interesting for the test cases here
  private def stripIdentifiers(value: Value): Value =
    value.sum match {
      case _: Sum.Variant =>
        value.copy(sum = Value.Sum.Variant(value.sum.variant.get.copy(variantId = None)))
      case _ => value
    }

  private def withParticipant[A] =
    HttpServiceTestFixture.withLedger[A](List(dar1, dar2), testId, None, useTls) _

  private def withJsonApi[A](participantPort: Port) =
    HttpServiceTestFixture.withHttpService[A](
      testId,
      participantPort,
      jdbcConfig,
      staticContentConfig,
      LeakPasswords.No,
      useTls,
      wsConfig,
      Some((certificatePath, privateKeyPath, privateKeyAlgorithm)),
    ) _

  private def withSetup[A](test: (Tables, Uri, DomainJsonEncoder) => Future[Assertion]) =
    withParticipant { case (participantPort, _: LedgerClient) =>
      val participantChannelBuilder =
        NettyChannelBuilder
          .forAddress("localhost", participantPort.value)
          .usePlaintext()

      val proxyPort = FreePort.find()

      val proxyBuilder = NettyServerBuilder.forPort(proxyPort.value)

      val setup =
        for {
          participantChannel <- GrpcResourceOwnerFactories.forChannel(
            participantChannelBuilder,
            shutdownTimeout = 5.seconds,
          )
          transactor <- createTransactor(
            postgresDatabase.url,
            postgresDatabase.userName,
            postgresDatabase.password,
            maxPoolSize = 10,
            GrpcResourceOwnerFactories,
          )
          db = Tables.initialize(transactor)(Slf4jLogHandler(getClass))
          _ = db.certificates.put(certificate)
          proxy <- NonRepudiationProxy.owner(
            participantChannel,
            proxyBuilder,
            db.certificates,
            db.signedPayloads,
            Clock.systemUTC(),
            CommandServiceGrpc.CommandService.scalaDescriptor.fullName,
            CommandSubmissionServiceGrpc.CommandSubmissionService.scalaDescriptor.fullName,
          )
        } yield (proxy, db)

      setup.use { case (_: Server, db: Tables) =>
        withJsonApi(proxyPort) { (uri, encoder, _: DomainJsonDecoder, _: LedgerClient) =>
          test(db, uri, encoder)
        }
      }

    }

}
