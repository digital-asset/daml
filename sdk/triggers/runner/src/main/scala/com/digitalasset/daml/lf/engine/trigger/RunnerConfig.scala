// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf.engine.trigger

import ch.qos.logback.classic.Level

import java.nio.file.{Path, Paths}
import java.time.Duration
import com.daml.lf.data.Ref
import com.daml.ledger.api.domain
import com.daml.ledger.api.refinements.ApiTypes.{ApplicationId, Party}
import com.daml.ledger.api.tls.TlsConfiguration
import com.daml.ledger.api.tls.TlsConfigurationCli
import com.daml.ledger.client.LedgerClient
import com.daml.lf.engine.trigger.TriggerRunnerConfig.DefaultTriggerRunnerConfig
import com.daml.lf.language.LanguageMajorVersion
import com.daml.platform.services.time.TimeProviderType
import com.daml.lf.speedy.Compiler

import scala.concurrent.{ExecutionContext, Future}

sealed trait LogEncoder
object LogEncoder {
  object Plain extends LogEncoder
  object Json extends LogEncoder
}

sealed trait CompilerConfigBuilder extends Product with Serializable {
  def build(majorLanguageVersion: LanguageMajorVersion): Compiler.Config
}
object CompilerConfigBuilder {
  final case object Default extends CompilerConfigBuilder {
    override def build(majorLanguageVersion: LanguageMajorVersion): Compiler.Config =
      Compiler.Config.Default(majorLanguageVersion)
  }
  final case object Dev extends CompilerConfigBuilder {
    override def build(majorLanguageVersion: LanguageMajorVersion): Compiler.Config =
      Compiler.Config.Dev(majorLanguageVersion)
  }
}

case class RunnerConfig(
    darPath: Path,
    // If defined, we will only list the triggers in the DAR and exit.
    listTriggers: Option[Boolean],
    triggerIdentifier: String,
    ledgerHost: String,
    ledgerPort: Int,
    ledgerClaims: ClaimsSpecification,
    maxInboundMessageSize: Int,
    // optional so we can detect if both --static-time and --wall-clock-time are passed.
    timeProviderType: Option[TimeProviderType],
    commandTtl: Duration,
    accessTokenFile: Option[Path],
    applicationId: ApplicationId,
    tlsConfig: TlsConfiguration,
    compilerConfigBuilder: CompilerConfigBuilder,
    majorLanguageVersion: LanguageMajorVersion,
    triggerConfig: TriggerRunnerConfig,
    rootLoggingLevel: Option[Level],
    logEncoder: LogEncoder,
) {
  private def updatePartySpec(f: TriggerParties => TriggerParties): RunnerConfig =
    if (ledgerClaims == null) {
      copy(ledgerClaims = PartySpecification(f(TriggerParties(Party(""), Set.empty))))
    } else
      ledgerClaims match {
        case PartySpecification(claims) =>
          copy(ledgerClaims = PartySpecification(f(claims)))
        case _: UserSpecification =>
          throw new IllegalArgumentException(
            s"Must specify either --ledger-party and --ledger-readas or --ledger-userid but not both"
          )
      }
  private def updateActAs(party: Party): RunnerConfig =
    updatePartySpec(spec => spec.copy(actAs = party))

  private def updateReadAs(parties: Seq[Party]): RunnerConfig =
    updatePartySpec(spec => spec.copy(readAs = spec.readAs ++ parties))

  private def updateUser(userId: Ref.UserId): RunnerConfig =
    if (ledgerClaims == null) {
      copy(ledgerClaims = UserSpecification(userId))
    } else
      ledgerClaims match {
        case UserSpecification(_) => copy(ledgerClaims = UserSpecification(userId))
        case _: PartySpecification =>
          throw new IllegalArgumentException(
            s"Must specify either --ledger-party and --ledger-readas or --ledger-userid but not both"
          )
      }
}

sealed abstract class ClaimsSpecification {
  def resolveClaims(client: LedgerClient)(implicit ec: ExecutionContext): Future[TriggerParties]
}

final case class PartySpecification(claims: TriggerParties) extends ClaimsSpecification {
  override def resolveClaims(client: LedgerClient)(implicit
      ec: ExecutionContext
  ): Future[TriggerParties] =
    Future.successful(claims)
}

final case class UserSpecification(userId: Ref.UserId) extends ClaimsSpecification {
  override def resolveClaims(
      client: LedgerClient
  )(implicit ec: ExecutionContext): Future[TriggerParties] = for {
    user <- client.userManagementClient.getUser(userId)
    primaryParty <- user.primaryParty.fold[Future[Ref.Party]](
      Future.failed(
        new IllegalArgumentException(
          s"User $user has no primary party. Specify a party explicitly via --ledger-party"
        )
      )
    )(Future.successful)
    rights <- client.userManagementClient.listUserRights(userId)
    readAs = rights.collect { case domain.UserRight.CanReadAs(party) =>
      party
    }.toSet
    actAs = rights.collect { case domain.UserRight.CanActAs(party) =>
      party
    }.toSet
    _ <-
      if (actAs.contains(primaryParty)) {
        Future.unit
      } else {
        Future.failed(
          new IllegalArgumentException(
            s"User $user has primary party $primaryParty but no actAs claims for that party. Either change the user rights or specify a different party via --ledger-party"
          )
        )
      }
    readers = (readAs ++ actAs) - primaryParty
  } yield TriggerParties(Party(primaryParty), readers.map(Party(_)))
}

final case class TriggerParties(
    actAs: Party,
    readAs: Set[Party],
) {
  lazy val readers: Set[Party] = readAs + actAs
}

object RunnerConfig {

  implicit val userRead: scopt.Read[Ref.UserId] = scopt.Read.reads { s =>
    Ref.UserId.fromString(s).fold(e => throw new IllegalArgumentException(e), identity)
  }

  private[trigger] val DefaultMaxInboundMessageSize: Int = 4194304
  private[trigger] val DefaultTimeProviderType: TimeProviderType = TimeProviderType.WallClock
  private[trigger] val DefaultApplicationId: ApplicationId =
    ApplicationId("daml-trigger")
  private[trigger] val DefaultCompilerConfigBuilder: CompilerConfigBuilder =
    CompilerConfigBuilder.Default
  private[trigger] val DefaultMajorLanguageVersion: LanguageMajorVersion = LanguageMajorVersion.V1

  @SuppressWarnings(Array("org.wartremover.warts.NonUnitStatements")) // scopt builders
  private val parser = new scopt.OptionParser[RunnerConfig]("trigger-runner") {
    head("trigger-runner")

    opt[String]("dar")
      .required()
      .action((f, c) => c.copy(darPath = Paths.get(f)))
      .text("Path to the dar file containing the trigger")

    opt[String]("trigger-name")
      .action((t, c) => c.copy(triggerIdentifier = t))
      .text("Identifier of the trigger that should be run in the format Module.Name:Entity.Name")

    opt[String]("ledger-host")
      .action((t, c) => c.copy(ledgerHost = t))
      .text("Ledger hostname")

    opt[Int]("ledger-port")
      .action((t, c) => c.copy(ledgerPort = t))
      .text("Ledger port")

    opt[String]("ledger-party")
      .action((t, c) => c.updateActAs(Party(t)))
      .text("""The party the trigger can act as.
              |Mutually exclusive with --ledger-user.""".stripMargin)

    opt[Seq[String]]("ledger-readas")
      .action((t, c) => c.updateReadAs(t.map(Party(_))))
      .unbounded()
      .text(
        """A comma-separated list of parties the trigger can read as.
          |Can be specified multiple-times.
          |Mutually exclusive with --ledger-user.""".stripMargin
      )

    opt[Ref.UserId]("ledger-user")
      .action((u, c) => c.updateUser(u))
      .unbounded()
      .text(
        """The id of the user the trigger should run as.
          |This is equivalent to specifying the primary party
          |of the user as --ledger-party and
          |all actAs and readAs claims of the user other than the
          |primary party as --ledger-readas.
          |The user must have a primary party.
          |Mutually exclusive with --ledger-party and --ledger-readas.""".stripMargin
      )

    opt[Int]("max-inbound-message-size")
      .action((x, c) => c.copy(maxInboundMessageSize = x))
      .optional()
      .text(
        s"Optional max inbound message size in bytes. Defaults to ${DefaultMaxInboundMessageSize}"
      )

    opt[Unit]('w', "wall-clock-time")
      .action { (_, c) =>
        setTimeProviderType(c, TimeProviderType.WallClock)
      }
      .text("Use wall clock time (UTC).")

    opt[Unit]('s', "static-time")
      .action { (_, c) =>
        setTimeProviderType(c, TimeProviderType.Static)
      }
      .text("Use static time.")

    opt[Long]("ttl")
      .action { (t, c) =>
        c.copy(commandTtl = Duration.ofSeconds(t))
      }
      .text("TTL in seconds used for commands emitted by the trigger. Defaults to 30s.")

    opt[String]("access-token-file")
      .action { (f, c) =>
        c.copy(accessTokenFile = Some(Paths.get(f)))
      }
      .text(
        "File from which the access token will be read, required to interact with an authenticated ledger"
      )

    opt[String]("application-id")
      .action { (appId, c) =>
        c.copy(applicationId = ApplicationId(appId))
      }
      .text(s"Application ID used to submit commands. Defaults to ${DefaultApplicationId}")

    opt[Unit]('v', "verbose")
      .text("Root logging level -> DEBUG")
      .action((_, cli) => cli.copy(rootLoggingLevel = Some(Level.DEBUG)))

    opt[Unit]("debug")
      .text("Root logging level -> DEBUG")
      .action((_, cli) => cli.copy(rootLoggingLevel = Some(Level.DEBUG)))

    implicit val levelRead: scopt.Read[Level] = scopt.Read.reads(Level.valueOf)
    opt[Level]("log-level-root")
      .text("Log-level of the root logger")
      .valueName("<LEVEL>")
      .action((level, cli) => cli.copy(rootLoggingLevel = Some(level)))

    opt[String]("log-encoder")
      .text("Log encoder: plain|json")
      .action {
        case ("json", cli) => cli.copy(logEncoder = LogEncoder.Json)
        case ("plain", cli) => cli.copy(logEncoder = LogEncoder.Plain)
        case (other, _) =>
          throw new IllegalArgumentException(s"Unsupported logging encoder $other")
      }

    opt[Long]("max-batch-size")
      .optional()
      .text(
        s"maximum number of messages processed between two high-level rule triggers. Defaults to ${DefaultTriggerRunnerConfig.maximumBatchSize}"
      )
      .action((size, cli) =>
        if (size > 0) cli.copy(triggerConfig = cli.triggerConfig.copy(maximumBatchSize = size))
        else throw new IllegalArgumentException(s"batch size must be strictly positive")
      )

    opt[Unit]("dev-mode-unsafe")
      .action((_, c) => c.copy(compilerConfigBuilder = CompilerConfigBuilder.Dev))
      .optional()
      .text(
        "Turns on development mode. Development mode allows development versions of Daml-LF language."
      )
      .hidden()

    implicit val majorLanguageVersionRead: scopt.Read[LanguageMajorVersion] =
      scopt.Read.reads(s =>
        LanguageMajorVersion.fromString(s) match {
          case Some(v) => v
          case None => throw new IllegalArgumentException(s"$s is not a valid major LF version")
        }
      )
    opt[LanguageMajorVersion]("lf-major-version")
      .action((v, c) => c.copy(majorLanguageVersion = v))
      .optional()
      .text(
        "The major version of LF to use."
      )
      // TODO(#17366): unhide once LF v2 has a stable version
      .hidden()

    TlsConfigurationCli.parse(this, colSpacer = "        ")((f, c) =>
      c.copy(tlsConfig = f(c.tlsConfig))
    )

    help("help").text("Print this usage text")

    cmd("list")
      .action((_, c) => c.copy(listTriggers = Some(false)))
      .text("List the triggers in the DAR.")

    cmd("verbose-list")
      .hidden()
      .action((_, c) => c.copy(listTriggers = Some(true)))

    checkConfig(c =>
      c.listTriggers match {
        case Some(_) =>
          // I do not want to break the trigger CLI and require a
          // "run" command so I can’t make these options required
          // in general. Therefore, we do this check in checkConfig.
          success
        case None =>
          if (c.triggerIdentifier == null) {
            failure("Missing option --trigger-name")
          } else if (c.ledgerHost == null) {
            failure("Missing option --ledger-host")
          } else if (c.ledgerPort == 0) {
            failure("Missing option --ledger-port")
          } else if (c.ledgerClaims == null) {
            failure("Missing option --ledger-party or --ledger-user")
          } else {
            c.ledgerClaims match {
              case PartySpecification(TriggerParties(actAs, _)) if actAs == Party("") =>
                failure("Missing option --ledger-party")
              case _ => success
            }
          }
      }
    )
  }

  private def setTimeProviderType(
      config: RunnerConfig,
      timeProviderType: TimeProviderType,
  ): RunnerConfig = {
    if (config.timeProviderType.exists(_ != timeProviderType)) {
      throw new IllegalStateException(
        "Static time mode (`-s`/`--static-time`) and wall-clock time mode (`-w`/`--wall-clock-time`) are mutually exclusive. The time mode must be unambiguous."
      )
    }
    config.copy(timeProviderType = Some(timeProviderType))
  }

  def parse(args: Array[String]): Option[RunnerConfig] =
    parser.parse(
      args,
      Empty,
    )

  val Empty: RunnerConfig = RunnerConfig(
    darPath = null,
    listTriggers = None,
    triggerIdentifier = null,
    ledgerHost = null,
    ledgerPort = 0,
    ledgerClaims = null,
    maxInboundMessageSize = DefaultMaxInboundMessageSize,
    timeProviderType = None,
    commandTtl = Duration.ofSeconds(30L),
    accessTokenFile = None,
    tlsConfig = TlsConfiguration(enabled = false, None, None, None),
    applicationId = DefaultApplicationId,
    compilerConfigBuilder = DefaultCompilerConfigBuilder,
    majorLanguageVersion = DefaultMajorLanguageVersion,
    triggerConfig = DefaultTriggerRunnerConfig,
    rootLoggingLevel = None,
    logEncoder = LogEncoder.Plain,
  )
}
