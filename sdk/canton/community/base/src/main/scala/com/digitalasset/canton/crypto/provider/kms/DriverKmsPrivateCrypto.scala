// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.crypto.provider.kms

import com.digitalasset.canton.config.ProcessingTimeout
import com.digitalasset.canton.crypto.CryptoFactory.CryptoStoresAndSchemes
import com.digitalasset.canton.crypto.kms.driver.v1.DriverKms
import com.digitalasset.canton.crypto.store.{CryptoPublicStore, KmsCryptoPrivateStore}
import com.digitalasset.canton.crypto.{EncryptionKeySpec, SigningAlgorithmSpec, SigningKeySpec}
import com.digitalasset.canton.health.{
  ComponentHealthState,
  CompositeHealthElement,
  HealthQuasiComponent,
}
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}

import scala.concurrent.ExecutionContext

class DriverKmsPrivateCrypto private (
    override protected val kms: DriverKms,
    override val defaultSigningAlgorithmSpec: SigningAlgorithmSpec,
    override val defaultSigningKeySpec: SigningKeySpec,
    override val defaultEncryptionKeySpec: EncryptionKeySpec,
    override protected val privateStore: KmsCryptoPrivateStore,
    override protected val publicStore: CryptoPublicStore,
    override protected val timeouts: ProcessingTimeout,
    override protected val loggerFactory: NamedLoggerFactory,
)(override implicit val ec: ExecutionContext)
    extends KmsPrivateCrypto
    with NamedLogging
    with CompositeHealthElement[String, HealthQuasiComponent] {

  override type KmsType = DriverKms

  override def name: String = "driver-kms-private-crypto"

  setDependency("driver-kms", kms)

  override protected def combineDependentStates: ComponentHealthState = kms.getState

  override protected def initialHealthState: ComponentHealthState =
    ComponentHealthState.NotInitializedState

}

object DriverKmsPrivateCrypto {

  def create(
      driverKms: DriverKms,
      storesAndSchemes: CryptoStoresAndSchemes,
      kmsCryptoPrivateStore: KmsCryptoPrivateStore,
      timeouts: ProcessingTimeout,
      loggerFactory: NamedLoggerFactory,
  )(implicit executionContext: ExecutionContext): Either[String, DriverKmsPrivateCrypto] =
    for {
      _ <- Either.cond(
        driverKms.supportedSigningKeySpecs
          .contains(storesAndSchemes.signingKeySpec),
        (),
        s"Selected signing key spec ${storesAndSchemes.signingKeySpec} not supported by driver: ${driverKms.supportedSigningKeySpecs}",
      )
      _ <- Either.cond(
        driverKms.supportedSigningAlgoSpecs
          .contains(storesAndSchemes.signingAlgorithmSpec),
        (),
        s"Selected signing algorithm spec ${storesAndSchemes.signingAlgorithmSpec} not supported by driver: ${driverKms.supportedSigningAlgoSpecs}",
      )
      _ <- Either.cond(
        driverKms.supportedEncryptionKeySpecs
          .contains(storesAndSchemes.encryptionKeySpec),
        (),
        s"Selected encryption key spec ${storesAndSchemes.encryptionKeySpec} not supported by driver: ${driverKms.supportedEncryptionKeySpecs}",
      )
      _ <- Either.cond(
        driverKms.supportedEncryptionAlgoSpecs
          .contains(storesAndSchemes.encryptionAlgorithmSpec),
        (),
        s"Selected encryption algorithm spec ${storesAndSchemes.encryptionAlgorithmSpec} not supported by driver: ${driverKms.supportedEncryptionAlgoSpecs}",
      )
    } yield new DriverKmsPrivateCrypto(
      driverKms,
      storesAndSchemes.signingAlgorithmSpec,
      storesAndSchemes.signingKeySpec,
      storesAndSchemes.encryptionKeySpec,
      kmsCryptoPrivateStore,
      storesAndSchemes.cryptoPublicStore,
      timeouts,
      loggerFactory,
    )
}
