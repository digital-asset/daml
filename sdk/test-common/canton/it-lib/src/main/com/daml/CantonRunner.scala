// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml
package integrationtest

import com.daml.bazeltools.BazelRunfiles._
import com.daml.jwt.JwtSigner
import com.daml.jwt.domain.DecodedJwt
import com.daml.lf.data.Ref
import com.daml.ledger.api.auth
import com.daml.ledger.resources.{Resource, ResourceContext, ResourceOwner}
import com.daml.platform.services.time.TimeProviderType
import com.daml.ports.{LockedFreePort, PortLock}
import com.daml.scalautil.Statement.discard
import com.daml.timer.RetryStrategy
import spray.json.JsString

import scala.concurrent.duration.DurationInt
import scala.concurrent.Future
import scala.sys.process.{Process, ProcessLogger}
import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Path, Paths}

import scala.concurrent.ExecutionContext

object CantonRunner {

  private[integrationtest] def toJson(s: String): String = JsString(s).toString()
  private[integrationtest] def toJson(path: Path): String = toJson(path.toString)

  lazy val cantonPath = Paths.get(rlocation("canton/community_app_deploy.jar"))

  case class CantonFiles(
      bootstrapFile: Path,
      configFile: Path,
      cantonLogFile: Path,
      portsFile: Path,
      completionFile: Path,
  )

  object CantonFiles {
    def apply(dir: Path): CantonFiles = CantonFiles(
      bootstrapFile = dir.resolve("participant.bootstrap"),
      configFile = dir.resolve("participant.config"),
      cantonLogFile = dir.resolve("canton.log"),
      portsFile = dir.resolve("portsfile"),
      completionFile = dir.resolve("completion"),
    )
  }

  def start(
      config: CantonConfig,
      logger: org.slf4j.Logger,
      darFiles: Seq[Path],
      files: CantonFiles,
  )(implicit ec: ExecutionContext): Future[
    ((PortLock.Locked, PortLock.Locked), Vector[(PortLock.Locked, PortLock.Locked)], Process)
  ] = {
    def info(s: String) = if (config.debug) logger.info(s)

    val ports =
      Vector.fill(config.nParticipants)(LockedFreePort.find() -> LockedFreePort.find())
    val domainPublicApi = LockedFreePort.find()
    val domainAdminApi = LockedFreePort.find()
    val exe = if (sys.props("os.name").toLowerCase.contains("windows")) ".exe" else ""
    val java = s"${System.getenv("JAVA_HOME")}/bin/java${exe}"
    val (timeType, clockType) = config.timeProviderType match {
      case TimeProviderType.Static => (Some("monotonic-time"), Some("sim-clock"))
      case TimeProviderType.WallClock => (None, None)
    }
    val authConfig = config.authSecret.fold("")(secret => s"""auth-services = [{
        |          type = unsafe-jwt-hmac-256
        |          secret = "${toJson(secret)}"
        |        }]
        |""".stripMargin)
    val tls = config.tlsConfig.fold("")(config => s"""tls {
        |          cert-chain-file = ${toJson(config.serverCrt)}
        |          private-key-file = ${toJson(config.serverPem)}
        |          trust-collection-file = ${toJson(config.caCrt)}
        |        }""".stripMargin)
    val partiesPageSize =
      config.maxPartiesPageSize.fold("")(pageSize => s"""party-management-service{
         |          max-parties-page-size = $pageSize
         |        }
         |""".stripMargin)

    def participantConfig(i: Int) = {
      val (adminPort, ledgerApiPort) = ports(i)
      val participantId = config.participantIds(i)
      s"""${participantId} {
         |      admin-api.port = ${adminPort.port}
         |      ledger-api{
         |        max-deduplication-duration = 0s
         |        port = ${ledgerApiPort.port}
         |        ${authConfig}
         |        ${tls}
         |        ${partiesPageSize}
         |      }
         |      storage.type = memory
         |      parameters = {
         |        enable-engine-stack-traces = true
         |        dev-version-support = ${config.devMode}
         |        disable-upgrade-validation = ${config.disableUpgradeValidation}
         |      }
         |      ${timeType.fold("")(x => "testing-time.type = " + x)}
         |    }""".stripMargin
    }
    val participantsConfig =
      (0 until config.nParticipants).map(participantConfig).mkString("\n")
    val cantonConfig =
      s"""canton {
         |  parameters{
         |    non-standard-config = yes
         |    dev-version-support = yes
         |    ports-file = ${toJson(files.portsFile)}
         |    ${clockType.fold("")(x => "clock.type = " + x)}
         |  }
         |
         |  domains {
         |    local {
         |      storage.type = memory
         |      public-api.port = ${domainPublicApi.port}
         |      admin-api.port = ${domainAdminApi.port}
         |      init.domain-parameters.protocol-version = ${if (config.devMode) "dev"
        else "5"}
         |    }
         |  }
         |  participants {
         |    ${participantsConfig}
         |  }
         |}
          """.stripMargin
    discard(Files.write(files.configFile, cantonConfig.getBytes(StandardCharsets.UTF_8)))

    val bootstrapUploadDar = darFiles
      .map(darFile =>
        s"participants.all.dars.upload(\"${darFile.toString.replace("\\", "\\\\")}\", true, true)"
      )
      .mkString("\n")
    // Run the given clients bootstrap, upload dars via the console (which internally calls the admin api), then write a non-empty file for us to wait on
    // TODO(DACH-NY/canton#3149): Consolidate dars.upload and ledger_api.package.upload_dar
    // If the above is fixed, we can revert back to using ledgerClient.uploadDar here.
    val completionFile = files.completionFile.toString.replace("\\", "\\\\")
    val bootstrapContent =
      s"""import java.nio.file.{Files, Paths}
         |import java.nio.charset.StandardCharsets
         |${config.bootstrapScript.getOrElse("")}
         |$bootstrapUploadDar
         |Files.write(Paths.get("$completionFile"), "Completed".getBytes(StandardCharsets.UTF_8))
         |""".stripMargin

    discard { Files.write(files.bootstrapFile, bootstrapContent.getBytes(StandardCharsets.UTF_8)) }

    val debugOptions =
      if (config.debug) List("--log-file-name", files.cantonLogFile.toString, "--verbose")
      else List.empty
    info(
      s"""Starting canton with parameters:
         |  authSecret = ${config.authSecret}
         |  devMode = ${config.devMode}
         |  nParticipants = ${config.nParticipants}
         |  timeProviderType = ${config.timeProviderType}
         |  tlsEnable = ${config.tlsConfig.isDefined}
         |""".stripMargin
    )
    var outputBuffer = ""
    val cmd = java ::
      "-jar" ::
      config.jarPath.toString ::
      "daemon" ::
      "--auto-connect-local" ::
      "-c" ::
      files.configFile.toString ::
      "--bootstrap" ::
      files.bootstrapFile.toString ::
      debugOptions
    info(cmd.mkString("\\\n    "))
    for {
      proc <- Future(
        Process(
          cmd,
          None,
          // env-vars here
        ).run(ProcessLogger { str =>
          if (config.debug) println(str)
          outputBuffer += str
        })
      )
      _ = info("waiting for Canton to start")
      startedCanton <- waitForFile(proc, files.portsFile)
      _ <-
        if (startedCanton)
          Future.successful(info("Canton started"))
        else
          Future.failed(new Error("Canton failed expectedly with logs:\n" + outputBuffer))

      _ = info("waiting for bootstrap/upload to run")
      ranBootstrap <- waitForFile(proc, files.completionFile)
      _ <-
        if (ranBootstrap)
          Future.successful(info(s"${darFiles.size} packages loaded to ${ports.size} participants"))
        else
          Future.failed(new Error("Canton failed expectedly with logs:\n" + outputBuffer))
    } yield ((domainAdminApi, domainPublicApi), ports, proc)
  }

  private def waitForFile(proc: Process, path: Path)(implicit
      ec: ExecutionContext
  ): Future[Boolean] =
    RetryStrategy
      .constant(attempts = 240, waitTime = 1.seconds) { (_, _) =>
        if (proc.isAlive())
          Future(Files.size(path))
        else
          Future.successful(-1L)
      }
      .map(_ > 0)

  def stop(
      r: (
          (PortLock.Locked, PortLock.Locked),
          Vector[(PortLock.Locked, PortLock.Locked)],
          Process,
      )
  ): Future[Unit] = {
    val ((domainAdminApi, domainPublicApi), ports, process) = r
    process.destroy()
    discard(process.exitValue())
    domainAdminApi.unlock()
    domainPublicApi.unlock()
    ports.foreach { case (p1, p2) =>
      p1.unlock()
      p2.unlock()
    }
    Future.unit
  }

  def run(
      config: CantonConfig,
      tmpDir: Path,
      logger: org.slf4j.Logger,
      darFiles: Seq[Path],
  ): ResourceOwner[Vector[CantonFixture.LedgerPorts]] =
    new ResourceOwner[Vector[CantonFixture.LedgerPorts]] {
      override def acquire()(implicit
          context: ResourceContext
      ): Resource[Vector[CantonFixture.LedgerPorts]] = {
        val files = CantonFiles(tmpDir)
        Resource(start(config, logger, darFiles, files))(stop).map({ case (_, ports, _) =>
          ports.map { case (adminPort, ledgerPort) =>
            CantonFixture.LedgerPorts(ledgerPort.port, adminPort.port)
          }
        })
      }
    }

  def getToken(
      userId: String,
      authSecret: Option[String] = None,
      targetScope: Option[String] = None,
  ): Option[String] = authSecret.map { secret =>
    val payload = auth.StandardJWTPayload(
      issuer = None,
      userId = userId,
      participantId = None,
      exp = None,
      format = auth.StandardJWTTokenFormat.Scope,
      audiences = List.empty,
      scope = Some(targetScope.getOrElse("daml_ledger_api")),
    )
    val header = """{"alg": "HS256", "typ": "JWT"}"""
    val jwt = DecodedJwt[String](header, auth.AuthServiceJWTCodec.writeToString(payload))
    JwtSigner.HMAC256.sign(jwt, secret).toEither match {
      case Right(a) => a.value
      case Left(e) => throw new IllegalStateException(e.toString)
    }
  }

  val adminUserId = Ref.UserId.assertFromString("participant_admin")
}
