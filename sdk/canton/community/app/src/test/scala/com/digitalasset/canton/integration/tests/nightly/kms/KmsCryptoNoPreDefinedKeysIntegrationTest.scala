// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.tests.nightly.kms

import cats.data.EitherT
import cats.syntax.parallel.*
import com.digitalasset.canton.concurrent.{ExecutionContextIdlenessExecutorService, Threading}
import com.digitalasset.canton.config.DefaultProcessingTimeouts.shutdownProcessing
import com.digitalasset.canton.config.{DbConfig, KmsConfig}
import com.digitalasset.canton.crypto.kms.{Kms, KmsError}
import com.digitalasset.canton.crypto.store.{CryptoPrivateStore, KmsCryptoPrivateStore}
import com.digitalasset.canton.integration.plugins.{UseKms, UseReferenceBlockSequencer}
import com.digitalasset.canton.integration.tests.security.kms.KmsCryptoIntegrationTestBase
import com.digitalasset.canton.integration.{
  CommunityIntegrationTest,
  EnvironmentSetup,
  EnvironmentSetupPlugin,
}
import com.digitalasset.canton.lifecycle.FutureUnlessShutdown
import com.digitalasset.canton.util.FutureInstances.*

import scala.concurrent.Future

/** Runs a crypto integration test with one participant running a KMS provider without pre-generated
  * keys (i.e. keys are generated on-the-fly using a KMS and nodes are automatically initialized).
  */
trait KmsCryptoNoPreDefinedKeysIntegrationTest extends KmsCryptoIntegrationTestBase {
  self: CommunityIntegrationTest with EnvironmentSetup =>

  protected def kmsConfig: KmsConfig

  private lazy implicit val kmsInitKeysDeletionExecutionContext
      : ExecutionContextIdlenessExecutorService =
    Threading.newExecutionContext(
      loggerFactory.threadName + "-kms-init-keys-deletion-execution-context",
      noTracingLogger,
    )

  private def deleteKeys(
      store: CryptoPrivateStore,
      kmsClient: Kms,
  ): EitherT[FutureUnlessShutdown, KmsError, Unit] =
    store match {
      case kmsStore: KmsCryptoPrivateStore =>
        for {
          listKeys <- EitherT.right(kmsStore.listAllKmsKeys)
          _ <- listKeys.toList.parTraverse(kmsKeyId => kmsClient.deleteKey(kmsKeyId))
        } yield ()
      case _ => EitherT.rightT[FutureUnlessShutdown, KmsError](())
    }

  // all generated keys must be deleted after the test is ran
  protected def deleteAllGenerateKeys(): Unit =
    shutdownProcessing.await_("delete all canton-created keys") {
      val deleteResult = UseKms.withKmsClient(kmsConfig, timeouts, loggerFactory) { kmsClient =>
        provideEnvironment.nodes.local.parTraverse_ { node =>
          if (protectedNodes.contains(node.name))
            deleteKeys(node.crypto.cryptoPrivateStore, kmsClient).failOnShutdown
          else EitherT.rightT[Future, KmsError](())
        }
      }
      deleteResult.valueOr(err => logger.error("error deleting keys: " + err.show))

    }

  setupPlugins(
    withAutoInit = true,
    storagePlugin = Option.empty[EnvironmentSetupPlugin],
    sequencerPlugin = new UseReferenceBlockSequencer[DbConfig.Postgres](loggerFactory),
  )
}
