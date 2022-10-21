// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf.engine.trigger

import java.nio.file.{Path, Paths}
import java.time.Duration

import com.daml.lf.data.Ref
import com.daml.ledger.api.domain
import com.daml.ledger.api.refinements.ApiTypes.{ApplicationId, Party}
import com.daml.ledger.api.tls.TlsConfiguration
import com.daml.ledger.api.tls.TlsConfigurationCli
import com.daml.ledger.client.LedgerClient
import com.daml.platform.services.time.TimeProviderType
import com.daml.lf.speedy.Compiler

import scala.concurrent.{ExecutionContext, Future}

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
    compilerConfig: Compiler.Config,
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
  override def resolveClaims(client: LedgerClient)(implicit ec: ExecutionContext) =
    Future.successful(claims)
}

final case class UserSpecification(userId: Ref.UserId) extends ClaimsSpecification {
  override def resolveClaims(client: LedgerClient)(implicit ec: ExecutionContext) = for {
    user <- client.userManagementClient.getUser(userId)
    primaryParty <- user.primaryParty.fold[Future[Ref.Party]](
      Future.failed(
        new IllegalArgumentException(
          s"User $user has no primary party. Specify a party explicitly via --ledger-party"
        )
      )
    )(Future.successful(_))
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
  private[trigger] val DefaultCompilerConfig: Compiler.Config = Compiler.Config.Default

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

    opt[Unit]("dev-mode-unsafe")
      .action((_, c) => c.copy(compilerConfig = Compiler.Config.Dev))
      .optional()
      .text(
        "Turns on development mode. Development mode allows development versions of Daml-LF language."
      )
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
    tlsConfig = TlsConfiguration(false, None, None, None),
    applicationId = DefaultApplicationId,
    compilerConfig = DefaultCompilerConfig,
  )
}
