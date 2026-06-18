// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.tests.security.kms

import com.daml.test.evidence.tag.Security.SecurityTest.Property.Privacy
import com.daml.test.evidence.tag.Security.{SecurityTest, SecurityTestSuite}
import com.digitalasset.canton.BaseTest
import com.digitalasset.canton.crypto.kms.{Kms, KmsError, KmsKeyId}
import com.digitalasset.canton.crypto.provider.symbolic.SymbolicCrypto
import com.digitalasset.canton.crypto.store.db.{DbCryptoPrivateStore, StoredPrivateKey}
import com.digitalasset.canton.crypto.store.{
  CryptoPrivateStoreExtended,
  EncryptedCryptoPrivateStore,
}
import com.digitalasset.canton.integration.TestConsoleEnvironment
import com.digitalasset.canton.logging.SuppressingLogger.LogEntryOptionality
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.util.ByteString6144
import com.google.protobuf.ByteString

import scala.concurrent.ExecutionContext

trait EncryptedCryptoPrivateStoreTestHelpers extends SecurityTestSuite {
  this: BaseTest =>

  lazy protected val securityAsset: SecurityTest =
    SecurityTest(property = Privacy, asset = "Canton node")

  def getEncryptedCryptoStore(
      nodeName: String
  )(implicit env: TestConsoleEnvironment): EncryptedCryptoPrivateStore =
    env.n(nodeName).crypto.cryptoPrivateStore match {
      // check that node's private store is encrypted
      case encStore: EncryptedCryptoPrivateStore => encStore
      case _ =>
        fail(
          "node " + nodeName + " selected for protection does not have an encrypted crypto private store"
        )
    }

  /** Returns all the private keys in the store by calling listPrivateKeys with all combination of
    * the filters (purpose and encrypted)
    */
  def listAllStoredKeys(store: CryptoPrivateStoreExtended)(implicit
      traceContext: TraceContext,
      ec: ExecutionContext,
  ): Set[StoredPrivateKey] =
    retryET()(store.listPrivateKeys()).valueOrFailShutdown("list keys").futureValue

  def storeClearKey(nodeName: String)(implicit
      ec: ExecutionContext,
      env: TestConsoleEnvironment,
  ): Unit = {
    val encStore = getEncryptedCryptoStore(nodeName)
    val crypto = SymbolicCrypto.create(testedReleaseProtocolVersion, timeouts, loggerFactory)
    encStore.store
      .storePrivateKey(
        crypto.newSymbolicEncryptionKeyPair().privateKey,
        None,
      )
      .valueOrFailShutdown("write clear key in encrypted store")
      .futureValue
  }

  def checkAndDecryptKeys(nodeName: String)(implicit
      env: TestConsoleEnvironment
  ): Set[StoredPrivateKey] = {
    import env.*

    val encStore = getEncryptedCryptoStore(nodeName)
    // lists all the keys as they are stored in the db ('stored'). A db admin has access to this type of keys.
    val allStoredKeys = listAllStoredKeys(encStore.store)
    // lists all the keys AFTER decryption ('in-memory'). This uses the underlying encrypted store to decrypt the keys.
    val allDecryptedStoredKeys = listAllStoredKeys(encStore)

    allStoredKeys.size shouldEqual allDecryptedStoredKeys.size

    forAll(allStoredKeys) { storedKey =>
      val decryptedStoredKey =
        allDecryptedStoredKeys.find(_.id == storedKey.id).valueOrFail("could not find key")
      // verify that each stored private key is different from the in-memory version of the same key
      decryptedStoredKey.data should not be storedKey.data
      // verify the listed keys from the encrypted store match the decryption of the encrypted keys
      decryptedStoredKey.data shouldBe decryptKey(storedKey, encStore.kms)
        .valueOrFail(
          "failed to decrypt keys"
        )
    }
    allDecryptedStoredKeys
  }

  private def decryptKey(
      encryptedKey: StoredPrivateKey,
      kms: Kms,
  )(implicit ec: ExecutionContext): Either[KmsError, ByteString] = {
    /* ByteString6144 defines the higher bound on the data size we can decrypt. This is the
     * maximum accepted input size for all the external KMSs that we support.
     */
    val encryptedKeyData = ByteString6144
      .create(encryptedKey.data) match {
      case Left(err) => fail(err)
      case Right(encData) => encData
    }
    kms
      .decryptSymmetric(
        KmsKeyId(
          encryptedKey.wrapperKeyId
            .valueOrFail(
              s"key ${encryptedKey.id} does not have a wrapper key associated so it cannot be decrypted"
            )
        ),
        encryptedKeyData,
      )
      .map(_.unwrap)
      .value
      .failOnShutdown
      .futureValue
  }

  def checkAndReturnClearKeys(nodeName: String)(implicit
      ec: ExecutionContext,
      env: TestConsoleEnvironment,
  ): Set[StoredPrivateKey] =
    env.n(nodeName).crypto.cryptoPrivateStore match {
      // check that node's private store is in clear
      case store: DbCryptoPrivateStore =>
        val allKeys = listAllStoredKeys(store)
        allKeys should not be empty
        // checks that we can parse all keys (none of them is encrypted)
        forAll(allKeys)(storedKey =>
          store
            .exportPrivateKey(storedKey.id)
            .valueOrFailShutdown("export clear key")
            .futureValue
        )
        allKeys
      case _ =>
        fail(
          "node " + nodeName + " does not have a clear crypto private store"
        )
    }

  def stopAllNodesIgnoringSequencerClientWarnings(stop: => Unit): Unit =
    loggerFactory.assertLogsUnorderedOptional(
      stop,
      // As we stop the synchronizer node before (or at the same time as) the participants, allow the participants'
      // sequencer clients to warn about the sequencer being down until all nodes are stopped.
      (
        LogEntryOptionality.OptionalMany,
        logEntry => {
          logEntry.loggerName should include("GrpcSequencerClientTransport")
          logEntry.warningMessage should include(
            "Request failed for sequencer. Is the server running?"
          )
        },
      ),
    )
}
