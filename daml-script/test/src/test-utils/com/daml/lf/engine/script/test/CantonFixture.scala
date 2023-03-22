// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf.engine.script.test

import java.io.File
import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Path, Paths}

import com.daml.bazeltools.BazelRunfiles._
import com.daml.ledger.api.testing.utils.{AkkaBeforeAndAfterAll, OwnedResource, SuiteResource}
import com.daml.ledger.api.tls.TlsConfiguration
import com.daml.ledger.client.{configuration => clientConfig, LedgerClient}
import com.daml.ledger.resources.{Resource, ResourceContext, ResourceOwner}
import com.daml.lf.data.Ref
import com.daml.lf.engine.script._
import com.daml.lf.engine.script.ledgerinteraction.{GrpcLedgerClient, ScriptTimeMode}
import com.daml.ports.{LockedFreePort, Port}
import com.daml.scalautil.Statement.discard
import com.daml.timer.RetryStrategy
import com.google.protobuf.ByteString
import org.scalatest.Suite
import spray.json.JsString

import scala.concurrent.duration.DurationInt
import scala.concurrent.{ExecutionContext, Future}
import scala.sys.process.Process

@scala.annotation.nowarn("msg=match may not be exhaustive")
trait CantonFixture
    extends AbstractScriptTest
    with SuiteResource[Vector[Port]]
    with AkkaBeforeAndAfterAll {
  self: Suite =>

  protected def darFiles: List[File]
  protected def nParticipants: Int
  protected def devMode: Boolean
  protected def timeMode: ScriptTimeMode
  protected def tlsEnable: Boolean

  private val tmpDir = Files.createTempDirectory("testMultiParticipantFixture")
  private val cantonConfigPath = tmpDir.resolve("participant.config")
  private val portFile = tmpDir.resolve("portfile")

  override protected def afterAll(): Unit = {
    Files.delete(cantonConfigPath)
    discard(Files.deleteIfExists(portFile))
    Files.delete(tmpDir)
    super.afterAll()
  }

  lazy val List(serverCrt, serverPem, caCrt, clientCrt, clientPem) =
    List("server.crt", "server.pem", "ca.crt", "client.crt", "client.pem").map { src =>
      Paths.get(rlocation("test-common/test-certificates/" + src))
    }

  lazy val tlsConfig =
    TlsConfiguration(
      enabled = tlsEnable,
      certChainFile = Some(clientCrt.toFile),
      privateKeyFile = Some(clientPem.toFile),
      trustCollectionFile = Some(caCrt.toFile),
    )

  private def toJson(path: Path): String = JsString(path.toString).toString()

  private def canton(): ResourceOwner[Vector[Port]] =
    new ResourceOwner[Vector[Port]] {
      override def acquire()(implicit context: ResourceContext): Resource[Vector[Port]] = {
        def start(): Future[(Vector[Port], Process)] = {
          val ports =
            Vector.fill(nParticipants)(LockedFreePort.find() -> LockedFreePort.find())
          val domainPublicApi = LockedFreePort.find()
          val domainAdminApi = LockedFreePort.find()

          val cantonPath = rlocation(
            "external/canton/lib/canton-open-source-2.7.0-SNAPSHOT.jar"
          )
          val exe = if (sys.props("os.name").toLowerCase.contains("windows")) ".exe" else ""
          val java = s"${System.getenv("JAVA_HOME")}/bin/java${exe}"
          val (timeType, clockType) = timeMode match {
            case ScriptTimeMode.Static => (Some("monotonic-time"), Some("sim-clock"))
            case ScriptTimeMode.WallClock => (None, None)
          }
          val tslConfig =
            if (tlsEnable)
              s"""tls {
               |          cert-chain-file = ${toJson(serverCrt)}
               |          private-key-file = ${toJson(serverPem)}
               |          trust-collection-file = ${toJson(caCrt)}
               |        }""".stripMargin
            else
              ""
          def participantConfig(i: Int) = {
            val (adminPort, ledgerApiPort) = ports(i)
            s"""participant${i} {
               |      admin-api.port = ${adminPort.port}
               |      ledger-api{
               |        port = ${ledgerApiPort.port}
               |        ${tslConfig}
               |      }
               |      storage.type = memory
               |      parameters.dev-version-support = ${devMode}
               |      ${timeType.fold("")(x => "testing-time.type = " + x)}
               |    }""".stripMargin
          }
          val participantsConfig =
            (0 until nParticipants).map(participantConfig(_)).mkString("", "\n", "")
          val cantonConfig =
            s"""canton {
               |  parameters.non-standard-config = yes
               |  
               |  parameters {
               |    ports-file = ${toJson(portFile)}
               |    ${clockType.fold("")(x => "clock.type = " + x)}
               |  }
               |  
               |  domains {
               |    local {
               |      storage.type = memory
               |      public-api.port = ${domainPublicApi.port}
               |      admin-api.port = ${domainAdminApi.port}
               |      init.domain-parameters.protocol-version = ${if (devMode) Int.MaxValue else 4}
               |    }
               |  }
               |  participants {
               |    ${participantsConfig}      
               |  }
               |}
          """.stripMargin
          discard(Files.write(cantonConfigPath, cantonConfig.getBytes(StandardCharsets.UTF_8)))
          for {
            proc <- Future(
              Process(
                Seq(
                  java,
                  "-jar",
                  cantonPath,
                  "daemon",
                  "--auto-connect-local",
                  "-c",
                  cantonConfigPath.toString,
                )
              ).run()
            )
            _ <- RetryStrategy.constant(attempts = 240, waitTime = 1.seconds)((_, _) =>
              Future(Files.size(portFile))
            )
          } yield (ports.map(_._2.port), proc)
        }
        def stop(r: (Vector[Port], Process)): Future[Unit] = {
          r._2.destroy()
          discard(r._2.exitValue())
          Future.unit
        }
        Resource(start())(stop).map({ case (ports, _) => ports })
      }
    }

  override protected lazy val suiteResource: OwnedResource[ResourceContext, Vector[Port]] = {
    implicit val resourceContext: ResourceContext = ResourceContext(system.dispatcher)
    import ResourceContext.executionContext
    new OwnedResource[ResourceContext, Vector[Port]](
      for {
        ports <- canton()
        _ <- ResourceOwner.forFuture { () =>
          Future.traverse(ports) { port =>
            for {
              client <- LedgerClient.singleHost(
                hostIp = "localhost",
                port = port.value,
                configuration = clientConfig.LedgerClientConfiguration(
                  applicationId = "daml-script",
                  ledgerIdRequirement = clientConfig.LedgerIdRequirement.none,
                  commandClient = clientConfig.CommandClientConfiguration.default,
                ),
                channelConfig =
                  clientConfig.LedgerClientChannelConfiguration(sslContext = tlsConfig.client()),
              )
              _ <- Future.traverse(darFiles)(file =>
                client.packageManagementClient.uploadDarFile(
                  ByteString.copyFrom(Files.readAllBytes(file.toPath))
                )
              )
            } yield ()
          }
        }
      } yield ports,
      acquisitionTimeout = 2.minute,
      releaseTimeout = 2.minute,
    )
  }

  def participantClients(
      authenticatedParties: Option[List[Ref.Party]] = None,
      maxInboundMessageSize: Int = ScriptConfig.DefaultMaxInboundMessageSize,
  ): Future[Participants[GrpcLedgerClient]] = {
    implicit val ec: ExecutionContext = system.dispatcher
    val participants = suiteResource.value.zipWithIndex.map { case (port, i) =>
      Participant(s"participant$i") -> ApiParameters(
        "localhost",
        port.value,
        authenticatedParties.map(AbstractScriptTest.getToken(_, false)),
        application_id = Some(AbstractScriptTest.appId),
      )
    }
    val params = Participants(
      participants.headOption.map(_._2),
      participants.toMap,
      Map.empty,
    )
    Runner.connect(
      params,
      tlsConfig = tlsConfig,
      maxInboundMessageSize = maxInboundMessageSize,
    )
  }

}
