// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.crypto.store

import com.daml.nameof.NameOf.functionFullName
import com.digitalasset.canton.config.CachingConfigs
import com.digitalasset.canton.config.CantonRequireTypes.String300
import com.digitalasset.canton.crypto.kms.KmsKeyId
import com.digitalasset.canton.crypto.store.KmsMetadataStore.KmsMetadata
import com.digitalasset.canton.crypto.store.db.DbKmsMetadataStore
import com.digitalasset.canton.crypto.store.memory.InMemoryKmsMetadataStore
import com.digitalasset.canton.crypto.{Fingerprint, KeyPurpose, SigningKeyUsage}
import com.digitalasset.canton.lifecycle.FutureUnlessShutdown
import com.digitalasset.canton.logging.LogEntry
import com.digitalasset.canton.resource.DbStorage
import com.digitalasset.canton.store.db.{DbTest, H2Test, PostgresTest}
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.{BaseTest, HasExecutionContext, ProtocolVersionChecksAsyncWordSpec}
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AsyncWordSpec
import org.scalatest.{BeforeAndAfterAll, EitherValues, OptionValues}

trait KmsMetadataStoreTest
    extends BeforeAndAfterAll
    with EitherValues
    with BaseTest
    with ProtocolVersionChecksAsyncWordSpec
    with HasExecutionContext {
  this: AsyncWordSpec & Matchers & OptionValues =>

  private val metadataA = {
    val fingerprint = Fingerprint.tryFromString(Iterator.continually('a').take(68).mkString)
    val keyId = KmsKeyId(String300.tryCreate(Iterator.continually('a').take(300).mkString))
    KmsMetadata(fingerprint, keyId, KeyPurpose.Signing, Some(SigningKeyUsage.ProtocolOnly))
  }

  private val metadataB = {
    val fingerprint =
      Fingerprint.tryFromString(Iterator.continually('b').take(68).mkString)
    val keyId = KmsKeyId(String300.tryCreate(Iterator.continually('b').take(300).mkString))
    KmsMetadata(fingerprint, keyId, KeyPurpose.Encryption)
  }

  private val fingerprintC =
    Fingerprint.tryFromString(Iterator.continually('c').take(68).mkString)

  def kmsMetadataStore(mk: () => KmsMetadataStore): Unit = {
    "kms metadata store" should {
      "store and get metadata" in {
        val store = mk()

        for {
          _ <- store.store(metadataA)
          _ <- store.store(metadataB)
          getMA <- store.get(metadataA.fingerprint)
          getMB <- store.get(metadataB.fingerprint)
          getMC <- store.get(fingerprintC)
        } yield {
          getMA shouldBe Some(metadataA)
          getMB shouldBe Some(metadataB)
          getMC shouldBe None
        }
      }.failOnShutdown

      "be idempotent if inserting the same metadata twice" in {
        val store = mk()
        for {
          _ <- store.store(metadataA)
          _ <- store.store(metadataA)
          getMA <- store.get(metadataA.fingerprint)
        } yield {
          getMA shouldBe Some(metadataA)
        }
      }.failOnShutdown

      "fail to insert 2 metadata with the same fingerprint" in {
        val store = mk()

        val res = loggerFactory.assertLoggedWarningsAndErrorsSeq(
          for {
            _ <- store.store(metadataA)
            _ <- store.store(metadataA.copy(kmsKeyId = metadataB.kmsKeyId))
          } yield (),
          LogEntry.assertLogSeq(
            Seq(
              (
                _.errorMessage should include("has existing"),
                "expected logged DB failure",
              )
            )
          ),
        )
        recoverToSucceededIf[IllegalStateException](
          res.onShutdown(throw new RuntimeException("aborted due to shutdown."))
        )
      }

      "delete metadata" in {
        val store = mk()

        for {
          _ <- store.store(metadataA)
          _ <- store.store(metadataB)
          afterInsert <- store.get(metadataA.fingerprint)
          _ <- store.delete(metadataA.fingerprint)
          afterDelete <- store.get(metadataA.fingerprint)
          getMB <- store.get(metadataB.fingerprint)
        } yield {
          afterInsert shouldBe Some(metadataA)
          afterDelete shouldBe None
          getMB shouldBe Some(metadataB) // B should still be there
        }
      }.failOnShutdown

      "list metadata" in {
        val store = mk()

        for {
          _ <- store.store(metadataA)
          _ <- store.store(metadataB)
          metadata <- store.list()
        } yield {
          metadata should contain theSameElementsAs List(metadataA, metadataB)
        }
      }.failOnShutdown

      "invalidate internal cache when storing a new value" in {
        val store = mk()

        for {
          firstGet <- store.get(metadataA.fingerprint)
          _ <- store.store(metadataA)
          secondGet <- store.get(metadataA.fingerprint)
        } yield {
          firstGet shouldBe None
          secondGet shouldBe Some(metadataA)
        }
      }.failOnShutdown

      "invalidate internal cache when deleting a value" in {
        val store = mk()

        for {
          _ <- store.store(metadataA)
          firstGet <- store.get(metadataA.fingerprint)
          _ <- store.delete(metadataA.fingerprint)
          secondGet <- store.get(metadataA.fingerprint)
        } yield {
          firstGet shouldBe Some(metadataA)
          secondGet shouldBe None
        }
      }.failOnShutdown
    }
  }
}

trait DbKmsMetadataStoreTest extends AsyncWordSpec with BaseTest with KmsMetadataStoreTest {
  this: DbTest =>
  override def cleanDb(
      storage: DbStorage
  )(implicit traceContext: TraceContext): FutureUnlessShutdown[Unit] = {
    import storage.api.*
    storage.update(DBIO.seq(sqlu"truncate table common_kms_metadata_store"), functionFullName)
  }

  "KmsMetadataStore" should {
    behave like kmsMetadataStore(() =>
      new DbKmsMetadataStore(
        storage,
        CachingConfigs.testing.kmsMetadataCache,
        timeouts,
        loggerFactory,
      )
    )
  }
}

class KmsMetadataStoreTestPostgres extends DbKmsMetadataStoreTest with PostgresTest

class KmsMetadataStoreTestH2 extends DbKmsMetadataStoreTest with H2Test

class KmsMetadataStoreTestInMemory extends AsyncWordSpec with BaseTest with KmsMetadataStoreTest {
  "InMemoryKmsMetadataStore" should {
    behave like kmsMetadataStore(() => new InMemoryKmsMetadataStore(loggerFactory))
  }
}
