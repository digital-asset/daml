// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.plugins

import cats.data.EitherT
import cats.syntax.either.*
import cats.syntax.parallel.*
import com.digitalasset.canton.concurrent.{
  ExecutionContextIdlenessExecutorService,
  ExecutorServiceExtensions,
  Threading,
}
import com.digitalasset.canton.config.*
import com.digitalasset.canton.config.DefaultProcessingTimeouts.shutdownProcessing
import com.digitalasset.canton.crypto.kms.{Kms, KmsError, KmsKeyId}
import com.digitalasset.canton.crypto.store.{CryptoPrivateStore, EncryptedCryptoPrivateStore}
import com.digitalasset.canton.integration.{
  ConfigTransforms,
  EnvironmentSetupPlugin,
  TestConsoleEnvironment,
}
import com.digitalasset.canton.lifecycle.{FutureUnlessShutdown, LifeCycle}
import com.digitalasset.canton.logging.NamedLoggerFactory
import com.digitalasset.canton.tracing.NoTracing
import com.digitalasset.canton.util.FutureInstances.*
import com.digitalasset.canton.util.ResourceUtil
import monocle.macros.syntax.lens.*

import scala.concurrent.{ExecutionContext, Future}

abstract class UseKms extends EnvironmentSetupPlugin with AutoCloseable with NoTracing {

  protected def keyId: Option[KmsKeyId]
  protected def nodes: Set[String]
  protected def nodesWithSessionSigningKeysDisabled: Set[String]
  protected def enableEncryptedPrivateStore: EncryptedPrivateStoreStatus
  protected def kmsConfig: KmsConfig

  protected val timeouts: ProcessingTimeout
  protected val loggerFactory: NamedLoggerFactory

  protected def createKms()(implicit ec: ExecutionContext): Either[KmsError, Kms]

  // ensure that all nodes with session signing keys `disabled` are part of the full protected node set
  require(
    nodesWithSessionSigningKeysDisabled.subsetOf(nodes),
    s"`nodesWithSessionSigningKeysDisabled` must be a subset of `nodes`, but found: " +
      s"${nodesWithSessionSigningKeysDisabled.diff(nodes).mkString(", ")}",
  )

  protected def withKmsClient[V](
      f: Kms => EitherT[Future, KmsError, V]
  )(implicit ec: ExecutionContext): EitherT[Future, KmsError, V] =
    for {
      kmsClient <- createKms().toEitherT[Future]
      res <- ResourceUtil.withResourceM(kmsClient)(f)
    } yield res

  private lazy val kmsKeyDeletionExecutionContext: ExecutionContextIdlenessExecutorService =
    Threading.newExecutionContext(
      loggerFactory.threadName + "-kms-key-deletion-execution-context",
      noTracingLogger,
    )

  private def encryptedPrivateStoreConfig(reverted: Boolean) =
    EncryptedPrivateStoreConfig.Kms(wrapperKeyId = keyId, reverted)

  private def enableKms(
      name: String,
      cryptoConfig: CryptoConfig,
  ): CryptoConfig =
    if (nodes.contains(name))
      changeCryptoConfig(cryptoConfig, disableSessionSigningKeysForNode(name))
    else cryptoConfig

  private def changeCryptoConfig(conf: CryptoConfig, disableSessionKeys: Boolean): CryptoConfig =
    enableEncryptedPrivateStore match {
      case EncryptedPrivateStoreStatus.Enable =>
        enableEncryptedPrivateStore(addKmsConfig(conf, disableSessionKeys = true))
      case EncryptedPrivateStoreStatus.Revert =>
        revertEncryptedPrivateStore(addKmsConfig(conf, disableSessionKeys = true))
      // session signing keys can only be used if we are directly storing all our private keys in an external KMS
      case EncryptedPrivateStoreStatus.Disable =>
        disableEncryptedPrivateStore(addKmsConfig(conf, disableSessionKeys))
    }

  private def setSessionKeysInKmsConfig(kmsConfig: KmsConfig, enabled: Boolean): KmsConfig =
    kmsConfig match {
      case driverConfig: KmsConfig.Driver =>
        driverConfig.focus(_.sessionSigningKeys.enabled).replace(enabled)
      case awsConfig: KmsConfig.Aws =>
        awsConfig.focus(_.sessionSigningKeys.enabled).replace(enabled)
      case gcpConfig: KmsConfig.Gcp =>
        gcpConfig.focus(_.sessionSigningKeys.enabled).replace(enabled)
    }

  private def addKmsConfig(conf: CryptoConfig, disableSessionKeys: Boolean): CryptoConfig =
    conf
      .focus(_.kms)
      .replace(
        Some(setSessionKeysInKmsConfig(kmsConfig, enabled = !disableSessionKeys))
      )

  private def enableEncryptedPrivateStore(conf: CryptoConfig): CryptoConfig =
    conf
      .focus(_.privateKeyStore.encryption)
      .replace(Some(encryptedPrivateStoreConfig(reverted = false)))

  private def revertEncryptedPrivateStore(conf: CryptoConfig): CryptoConfig =
    conf
      .focus(_.privateKeyStore.encryption)
      .replace(Some(encryptedPrivateStoreConfig(reverted = true)))

  private def disableEncryptedPrivateStore(conf: CryptoConfig): CryptoConfig =
    conf
      .focus(_.privateKeyStore.encryption)
      .replace(None)

  private def disableSessionSigningKeysForNode(name: String): Boolean =
    nodesWithSessionSigningKeysDisabled.contains(name)

  private def transformConfig(config: CantonConfig): CantonConfig = {
    // change the overall configs
    val updateParticipantConfigs = ConfigTransforms.updateAllParticipantConfigs {
      case (name, config) =>
        config
          .focus(_.crypto)
          .replace(enableKms(name, config.crypto))
    }
    val updateSequencersConfigs = ConfigTransforms.updateAllSequencerConfigs {
      case (name, config) =>
        config
          .focus(_.crypto)
          .replace(enableKms(name, config.crypto))
    }
    val updateMediatorsConfigs = ConfigTransforms.updateAllMediatorConfigs { case (name, config) =>
      config
        .focus(_.crypto)
        .replace(enableKms(name, config.crypto))
    }
    updateSequencersConfigs
      .compose(updateMediatorsConfigs)
      .compose(updateParticipantConfigs)(config)
  }

  override def beforeEnvironmentCreated(config: CantonConfig): CantonConfig =
    transformConfig(config)

  private def deleteKey(
      store: CryptoPrivateStore,
      kmsClient: Kms,
  ): EitherT[FutureUnlessShutdown, KmsError, Unit] = {
    implicit val ec: ExecutionContext = kmsKeyDeletionExecutionContext
    store match {
      case store: EncryptedCryptoPrivateStore =>
        kmsClient.deleteKey(store.wrapperKeyId)
      case _ => EitherT.rightT(())
    }
  }

  override def beforeEnvironmentDestroyed(
      environment: TestConsoleEnvironment
  ): Unit = {
    implicit val ec: ExecutionContext = kmsKeyDeletionExecutionContext

    // delete all keys created by Canton (if PRE-DEFINED keyId is not set)
    keyId match {
      case Some(_) => ()
      case None =>
        shutdownProcessing.await_("delete all canton-created wrapper keys") {
          val deleteResult = withKmsClient { kmsClient =>
            environment.nodes.local.parTraverse_ { node =>
              deleteKey(node.crypto.cryptoPrivateStore, kmsClient)
                .onShutdown(throw new RuntimeException("Aborted due to shutdown."))
            }
          }
          deleteResult.valueOr(err => logger.error("error deleting keys: " + err.show))
        }
    }
  }

  override def close(): Unit =
    LifeCycle.close(
      ExecutorServiceExtensions(kmsKeyDeletionExecutionContext)(
        logger,
        DefaultProcessingTimeouts.testing,
      )
    )(logger)

  override def afterTests(): Unit = close()
}

sealed trait EncryptedPrivateStoreStatus
object EncryptedPrivateStoreStatus {
  final case object Enable extends EncryptedPrivateStoreStatus
  final case object Disable extends EncryptedPrivateStoreStatus
  final case object Revert extends EncryptedPrivateStoreStatus
}
