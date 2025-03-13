// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration

import com.digitalasset.canton.LedgerParticipantId
import com.digitalasset.canton.config.StorageConfig
import com.digitalasset.canton.console.FeatureFlag
import com.digitalasset.canton.integration.plugins.UseExternalProcess
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.metrics.LedgerApiServerMetrics
import com.digitalasset.canton.participant.config.LedgerApiServerConfig
import com.digitalasset.canton.participant.ledger.api.LedgerApiStore
import com.digitalasset.canton.platform.store.backend.postgresql.PostgresDataSourceConfig
import com.digitalasset.canton.tracing.NoTracing

import scala.concurrent.duration.Duration
import scala.concurrent.{Await, Future}
import scala.util.{Failure, Success, Try, Using}

class LedgerApiStoreIntegrityChecker(
    override protected val loggerFactory: NamedLoggerFactory
) extends NamedLogging
    with NoTracing {

  def verifyParticipantLapiIntegrity(
      env: TestConsoleEnvironment,
      plugins: Seq[EnvironmentSetupPlugin],
  ): Unit = {
    import env.*

    def justWaitForIt[T](f: Future[T]) =
      Await.result(f, Duration(20, "seconds"))

    def verifyOverStore(
        participantName: String,
        participantLoggingName: String,
        storageConfig: StorageConfig,
    ): Unit =
      storageConfig match {
        case _ if environment.testingConfig.participantsWithoutLapiVerification(participantName) =>
          logger.info(
            s"Checking participant Ledger API Store integrity, for $participantLoggingName bypassed as it is specified in testingConfig.participantsWithoutLapiVerification."
          )

        case _: StorageConfig.Memory =>
          logger.error(
            s"Checking participant Ledger API Store integrity, for $participantLoggingName is FAILED, as it is backed by an InMemory store. For safety please consider: 1) keep local in-memory participant running at the end of the test, 2) switch to persistent storage 3) or as a last resort exclude in-memory, not running participants explicitly via testingConfig.participantsWithoutLapiVerification."
          )

        case nonInMemoryStorageConfig =>
          logger.info(
            s"Checking participant Ledger API Store integrity, for $participantLoggingName..."
          )

          try {
            Using.resource(
              justWaitForIt(
                LedgerApiStore
                  .initialize(
                    storageConfig = nonInMemoryStorageConfig,
                    ledgerParticipantId = LedgerParticipantId.assertFromString("fakeid"),
                    legderApiDatabaseConnectionTimeout =
                      LedgerApiServerConfig().databaseConnectionTimeout,
                    ledgerApiPostgresDataSourceConfig = PostgresDataSourceConfig(),
                    timeouts = environmentTimeouts,
                    loggerFactory = loggerFactory,
                    metrics = LedgerApiServerMetrics.ForTesting,
                    onlyForTesting_DoNotInitializeInMemoryState =
                      true, // otherwise we emmit error logs and keep a DBDispatcher open for empty DBs
                  )
                  .failOnShutdownToAbortException("ledger api store initialization")
              )
            ) { ledgerApiStore =>
              Try(
                justWaitForIt(
                  ledgerApiStore
                    .onlyForTestingVerifyIntegrity(
                      failForEmptyDB =
                        false // sometimes configured participants are not even started once
                    )
                    .failOnShutdownToAbortException("ledger api store initialization")
                )
              ) match {
                case Success(_) =>
                  logger.info(
                    s"Checking participant Ledger API Store integrity, for $participantLoggingName...OK"
                  )

                case Failure(t) =>
                  logger.error(
                    s"Checking participant Ledger API Store integrity, for $participantLoggingName...FAILED",
                    t,
                  )
              }
            }
          } catch {
            case t: Throwable =>
              logger.error(
                s"Checking participant Ledger API Store integrity, for $participantLoggingName...FAILED TO ACCESS DATABASE",
                t,
              )
          }
      }

    logger.info("Checking participant Ledger API Store integrity after tests...")
    val (runningAndActiveParticipants, notRunningParticipants) =
      participants.local.partition(participant =>
        featureSet.contains(FeatureFlag.Testing) &&
          participant.is_running &&
          participant.runningNode.exists(_.getNode.isDefined) &&
          participant.health.is_running() &&
          participant.health.status.isActive
            .getOrElse(false) // for passive replicas we need to go over store as well
      )

    runningAndActiveParticipants
      .foreach { runningParticipant =>
        runningParticipant.health.status
        logger.info(
          s"Checking participant Ledger API Store integrity, for local running participant ${runningParticipant.name}..."
        )
        Try(runningParticipant.testing.state_inspection.verifyLapiStoreIntegrity()) match {
          case Success(_) =>
            logger.info(
              s"Checking participant Ledger API Store integrity, for local running participant ${runningParticipant.name}...OK"
            )

          case Failure(t) =>
            logger.error(
              s"Checking participant Ledger API Store integrity, for local running participant ${runningParticipant.name}...FAILED",
              t,
            )
        }
      }

    notRunningParticipants
      .foreach { notRunningParticipant =>
        verifyOverStore(
          participantName = notRunningParticipant.name,
          participantLoggingName = s"not running local participant ${notRunningParticipant.name}",
          storageConfig = actualConfig.participants
            .getOrElse(
              notRunningParticipant.name,
              throw new IllegalStateException(
                s"No configuration found for a not running participant $notRunningParticipant."
              ),
            )
            .storage,
        )
      }

    plugins.collectFirst { case external: UseExternalProcess => external } match {
      case Some(externalPlugin) =>
        externalPlugin.externalParticipants.foreach { remoteParticipant =>
          verifyOverStore(
            participantName = remoteParticipant,
            participantLoggingName = s"for remote participant $remoteParticipant",
            storageConfig = externalPlugin.storageConfig(remoteParticipant),
          )
        }

      case None =>
        val remoteNonExcludedParticipants =
          participants.remote
            .map(_.name)
            .filterNot(environment.testingConfig.participantsWithoutLapiVerification)
        if (remoteNonExcludedParticipants.nonEmpty) {
          logger.error(
            s"Checking participant Ledger API Store integrity: external plugin not found, but there are remote participants [$remoteNonExcludedParticipants]. For safety please exclude remote, non-external participants explicitly via testingConfig.participantsWithoutLapiVerification"
          )
        }
    }
    logger.info(s"Checking participant Ledger API Store integrity...DONE")
  }

}
