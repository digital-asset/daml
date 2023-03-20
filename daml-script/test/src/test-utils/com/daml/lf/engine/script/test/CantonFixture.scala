// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf.engine.script.test

import java.io.File
import java.net.InetAddress
import java.nio.charset.StandardCharsets
import java.nio.file.Files
import com.daml.bazeltools.BazelRunfiles._
import com.daml.ledger.api.testing.utils.{AkkaBeforeAndAfterAll, OwnedResource, SuiteResource}
import com.daml.ledger.api.tls.TlsConfiguration
import com.daml.ledger.api.v1.admin.package_management_service.{
  PackageManagementServiceGrpc,
  UploadDarFileRequest,
}
import com.daml.ledger.resources.{Resource, ResourceContext, ResourceOwner}
import com.daml.lf.engine.script._
import com.daml.lf.engine.script.ledgerinteraction.{GrpcLedgerClient, ScriptTimeMode}
import com.daml.ports.{LockedFreePort, Port}
import com.daml.scalautil.Statement.discard
import com.daml.timer.RetryStrategy
import com.google.protobuf.ByteString
import io.grpc.ManagedChannelBuilder
import org.scalatest.Suite
import spray.json.JsString

import scala.concurrent.duration.DurationInt
import scala.concurrent.{ExecutionContext, Future}
import scala.language.existentials
import scala.sys.process.Process

trait CantonFixture
    extends AbstractScriptTest
    with SuiteResource[Vector[Port]]
    with AkkaBeforeAndAfterAll {
  self: Suite =>

  protected def darFiles: List[File]
  protected def nParticipants: Int
  protected def devMode: Boolean
  protected def timeMode: ScriptTimeMode

  private val tmpDir = Files.createTempDirectory("testMultiParticipantFixture")
  private val cantonConfigPath = tmpDir.resolve("participant.config")
  private val portFile = tmpDir.resolve("portfile")

  override protected def afterAll(): Unit = {
    Files.delete(cantonConfigPath)
    discard(Files.deleteIfExists(portFile))
    Files.delete(tmpDir)
    super.afterAll()
  }

  private def canton(): ResourceOwner[Vector[Port]] =
    new ResourceOwner[Vector[Port]] {
      override def acquire()(implicit context: ResourceContext): Resource[Vector[Port]] = {
        def start(): Future[(Vector[Port], Process)] = {
          val ports =
            Vector.fill(nParticipants)(
              LockedFreePort.find() -> LockedFreePort.find()
            )
          val domainPublicApi = LockedFreePort.find()
          val domainAdminApi = LockedFreePort.find()
          val cantonPath = rlocation(
            "external/canton/lib/canton-open-source-2.7.0-SNAPSHOT.jar"
          )
          val exe = if (sys.props("os.name").toLowerCase.contains("windows")) ".exe" else ""
          val java = s"${System.getenv("JAVA_HOME")}/bin/java${exe}"
          val (setTimeType, clockType) = timeMode match {
            case ScriptTimeMode.Static => ("testing-time.type = monotonic-time", "sim-clock")
            case ScriptTimeMode.WallClock => ("", "wall-clock")
          }
          def participantConfig(i: Int) = {
            val (adminPort, ledgerApiPort) = ports(i)
            s"""
               |     participant${i} {
               |       admin-api.port = ${adminPort.port}
               |       ledger-api.port = ${ledgerApiPort.port}
               |       storage.type = memory
               |       parameters.dev-version-support = ${devMode}
               |       ${setTimeType}
               |     }
               |""".stripMargin
          }
          val participantsConfig =
            (0 until nParticipants).map(participantConfig(_)).mkString("\n")
          val cantonConfig =
            s"""
               | 
               | canton {
               | 
               |   parameters.non-standard-config = yes
               |   
               |   parameters {
               |     ports-file = ${JsString(portFile.toString).toString()}
               |     clock.type = ${clockType}
               |   }
               |   domains {
               |     local {
               |       storage.type = memory
               |       public-api.port = ${domainPublicApi.port}
               |       admin-api.port = ${domainAdminApi.port}
               |       init.domain-parameters.protocol-version = ${if (devMode) Int.MaxValue else 4}
               |     }
               |   }
               |   participants {
               |     ${participantsConfig}      
               |   }
               | }
          """.stripMargin
          for {
            _ <- Future(
              Files.write(cantonConfigPath, cantonConfig.getBytes(StandardCharsets.UTF_8))
            )
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
            val builder = ManagedChannelBuilder
              .forAddress(InetAddress.getLoopbackAddress.getHostName, port.value)
            discard(builder.usePlaintext())
            ResourceOwner.forChannel(builder, shutdownTimeout = 1.second).use { channel =>
              val packageManagement = PackageManagementServiceGrpc.stub(channel)
              Future.traverse(darFiles)(file =>
                packageManagement.uploadDarFile(
                  UploadDarFileRequest.of(
                    darFile = ByteString.copyFrom(Files.readAllBytes(file.toPath)),
                    submissionId = s"${getClass.getSimpleName}-upload",
                  )
                )
              )
            }
          }
        }
      } yield ports,
      acquisitionTimeout = 2.minute,
      releaseTimeout = 2.minute,
    )
  }

  def participantClients(
      maxInboundMessageSize: Int = ScriptConfig.DefaultMaxInboundMessageSize,
      tlsConfiguration: TlsConfiguration = TlsConfiguration.Empty.copy(enabled = false),
  ): Future[Participants[GrpcLedgerClient]] = {
    implicit val ec: ExecutionContext = system.dispatcher
    val participants = suiteResource.value.zipWithIndex.map { case (port, i) =>
      Participant(s"participant$i") -> ApiParameters("localhost", port.value, None, None)
    }
    val params = Participants(
      participants.headOption.map(_._2),
      participants.toMap,
      Map.empty,
    )
    Runner.connect(
      params,
      tlsConfig = tlsConfiguration,
      maxInboundMessageSize = maxInboundMessageSize,
    )
  }

}
