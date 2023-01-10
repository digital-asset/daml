// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf.engine.script.test

import java.net.InetAddress
import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Paths}
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

import scala.concurrent.duration.DurationInt
import scala.concurrent.{ExecutionContext, Future}
import scala.language.existentials
import scala.sys.process.Process

trait MultiParticipantFixture
    extends AbstractScriptTest
    with SuiteResource[(Port, Port)]
    with AkkaBeforeAndAfterAll {
  self: Suite =>
  private def darFile = Paths.get(rlocation("daml-script/test/script-test.dar"))

  private val tmpDir = Files.createTempDirectory("testMultiParticipantFixture")
  private val cantonConfigPath = tmpDir.resolve("participant.config")

  override protected def afterAll(): Unit = {
    Files.delete(cantonConfigPath)
    super.afterAll()

  }

  private def canton(): ResourceOwner[(Port, Port)] =
    new ResourceOwner[(Port, Port)] {
      override def acquire()(implicit context: ResourceContext): Resource[(Port, Port)] = {
        def start(): Future[(Port, Port, Process)] = {
          val p1LedgerApi = LockedFreePort.find()
          val p2LedgerApi = LockedFreePort.find()
          val p1AdminApi = LockedFreePort.find()
          val p2AdminApi = LockedFreePort.find()
          val domainPublicApi = LockedFreePort.find()
          val domainAdminApi = LockedFreePort.find()
          val cantonPath = rlocation(
            "external/canton/lib/canton-open-source-2.6.0-SNAPSHOT.jar"
          )
          val exe = if (sys.props("os.name").toLowerCase.contains("windows")) ".exe" else ""
          val java = s"${System.getenv("JAVA_HOME")}/bin/java${exe}"
          val cantonConfig = s"""
          | canton {
          |   domains {
          |     local {
          |       storage.type = memory
          |       public-api.port = ${domainPublicApi.port}
          |       admin-api.port = ${domainAdminApi.port}
          |     }
          |   }
          |   participants {
          |     p1 {
          |       admin-api.port = ${p1AdminApi.port}
          |       ledger-api.port = ${p1LedgerApi.port}
          |       storage.type = memory
          |     }
          |     p2 {
          |       admin-api.port = ${p2AdminApi.port}
          |       ledger-api.port = ${p2LedgerApi.port}
          |       storage.type = memory
          |     }
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
            _ <- Future.traverse(
              Seq(p1LedgerApi, p2LedgerApi, p1AdminApi, p2AdminApi, domainPublicApi, domainAdminApi)
            )(p =>
              RetryStrategy.constant(attempts = 120, waitTime = 1.seconds)((_, _) =>
                Future(p.testAndUnlock(InetAddress.getLoopbackAddress))
              )
            )
          } yield (p1LedgerApi.port, p2LedgerApi.port, proc)
        }
        def stop(r: (Port, Port, Process)): Future[Unit] = {
          r._3.destroy()
          discard(r._3.exitValue())
          Future.unit
        }
        Resource(start())(stop).map({ case (p1, p2, _) => (p1, p2) })
      }
    }

  override protected lazy val suiteResource: OwnedResource[ResourceContext, (Port, Port)] = {
    implicit val resourceContext: ResourceContext = ResourceContext(system.dispatcher)
    import ResourceContext.executionContext
    new OwnedResource[ResourceContext, (Port, Port)](
      for {
        (p1, p2) <- canton()
        _ <- ResourceOwner.forFuture { () =>
          Future.traverse(Seq(p1, p2)) { port =>
            val builder = ManagedChannelBuilder
              .forAddress(InetAddress.getLoopbackAddress.getHostName, port.value)
            discard(builder.usePlaintext())
            ResourceOwner.forChannel(builder, shutdownTimeout = 1.second).use { channel =>
              val packageManagement = PackageManagementServiceGrpc.stub(channel)
              packageManagement.uploadDarFile(
                UploadDarFileRequest.of(
                  darFile = ByteString.copyFrom(Files.readAllBytes(darFile)),
                  submissionId = s"${getClass.getSimpleName}-upload",
                )
              )
            }
          }
        }
      } yield (p1, p2),
      acquisitionTimeout = 2.minute,
      releaseTimeout = 2.minute,
    )
  }

  def participantClients(): Future[Participants[GrpcLedgerClient]] = {
    implicit val ec: ExecutionContext = system.dispatcher
    val params = Participants(
      None,
      Seq(
        (Participant("one"), ApiParameters("localhost", suiteResource.value._1.value, None, None)),
        (Participant("two"), ApiParameters("localhost", suiteResource.value._2.value, None, None)),
      ).toMap,
      Map.empty,
    )
    Runner.connect(
      params,
      tlsConfig = TlsConfiguration(
        enabled = false,
        certChainFile = None,
        privateKeyFile = None,
        trustCollectionFile = None,
      ),
      maxInboundMessageSize = ScriptConfig.DefaultMaxInboundMessageSize,
    )
  }

  override def timeMode: ScriptTimeMode = ScriptTimeMode.WallClock
}
