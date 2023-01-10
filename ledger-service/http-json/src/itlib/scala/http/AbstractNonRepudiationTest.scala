// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.http

import java.nio.file.Files
import java.security.cert.X509Certificate
import java.time.Clock

import akka.http.scaladsl.model.Uri
import com.daml.doobie.logging.Slf4jLogHandler
import com.daml.http.dbbackend.JdbcConfig
import com.daml.http.json.{DomainJsonDecoder, DomainJsonEncoder}
import com.daml.ledger.api.v1.command_service.CommandServiceGrpc
import com.daml.ledger.api.v1.command_submission_service.CommandSubmissionServiceGrpc
import com.daml.ledger.api.v1.value.Value.Sum
import com.daml.ledger.api.v1.value.{RecordField, Value, Variant}
import com.daml.ledger.client.withoutledgerid.{LedgerClient => DamlLedgerClient}
import com.daml.nonrepudiation.NonRepudiationProxy
import com.daml.nonrepudiation.postgresql.{Tables, createTransactor}
import com.daml.nonrepudiation.testing.generateKeyAndCertificate
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
abstract class AbstractNonRepudiationTest
    extends AsyncFreeSpec
    with Matchers
    with Inside
    with BeforeAndAfterEach
    with AbstractHttpServiceIntegrationTestFuns
    with PostgresAroundEach {

  import AbstractNonRepudiationTest._
  import HttpServiceTestFixture._

  private var nonRepudiation: nonrepudiation.Configuration.Cli = _
  private var certificate: X509Certificate = _

  override protected def beforeEach(): Unit = {
    super.beforeEach()
    val (key, cert) = generateKeyAndCertificate()
    certificate = cert
    val certificatePath = Files.createTempFile("non-repudiation-test", "certificate")
    val privateKeyPath = Files.createTempFile("non-repudiation-test", "key")
    Files.write(certificatePath, cert.getEncoded)
    Files.write(privateKeyPath, key.getEncoded)
    nonRepudiation =
      nonrepudiation.Configuration.Cli(certificatePath, privateKeyPath, key.getAlgorithm)
  }

  override protected def afterEach(): Unit = {
    super.afterEach()
    nonRepudiation.certificateFile.foreach(Files.delete)
    nonRepudiation.privateKeyFile.foreach(Files.delete)
  }

  override val jdbcConfig: Option[JdbcConfig] = None

  override val staticContentConfig: Option[StaticContentConfig] = None

  override val useTls: UseTls = UseTls.NoTls

  override val wsConfig: Option[WebsocketConfig] = None

  protected def stripIdentifiers(field: RecordField): RecordField =
    field.copy(value = Some(stripIdentifiers(field.getValue)))

  // Doesn't aim at being complete, neither in stripping identifiers recursively
  // Only covers variant because it's the only case interesting for the test cases here
  private def stripIdentifiers(value: Value): Value =
    value match {
      case Value(Sum.Variant(Variant(Some(_), constructor, value))) =>
        Value(Sum.Variant(Variant(None, constructor, value)))
      case _ => value
    }

  private def withParticipant[A] =
    usingLedger[A](testId) _

  private def withJsonApi[A](participantPort: Port) =
    HttpServiceTestFixture.withHttpService[A](
      testName = testId,
      ledgerPort = participantPort,
      jdbcConfig = jdbcConfig,
      staticContentConfig = staticContentConfig,
      leakPasswords = LeakPasswords.No,
      useTls = useTls,
      wsConfig = wsConfig,
      nonRepudiation = nonRepudiation,
    ) _

  protected def withSetup[A](test: SetupFixture => Future[Assertion]) =
    withParticipant { case (participantPort, _: DamlLedgerClient, _) =>
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
        withJsonApi(proxyPort) { (uri, encoder, _: DomainJsonDecoder, _: DamlLedgerClient) =>
          test(SetupFixture(db, uri, encoder))
        }
      }

    }

}

object AbstractNonRepudiationTest {
  import AbstractHttpServiceIntegrationTestFuns.{UriFixture, EncoderFixture}
  final case class SetupFixture(db: Tables, uri: Uri, encoder: DomainJsonEncoder)
      extends UriFixture
      with EncoderFixture
}
