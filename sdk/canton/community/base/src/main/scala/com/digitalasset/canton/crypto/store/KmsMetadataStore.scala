// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.crypto.store

import com.daml.nonempty.NonEmpty
import com.digitalasset.canton.config.{CacheConfig, ProcessingTimeout}
import com.digitalasset.canton.crypto.kms.KmsKeyId
import com.digitalasset.canton.crypto.store.KmsMetadataStore.KmsMetadata
import com.digitalasset.canton.crypto.store.db.DbKmsMetadataStore
import com.digitalasset.canton.crypto.store.memory.InMemoryKmsMetadataStore
import com.digitalasset.canton.crypto.{Fingerprint, KeyPurpose, SigningKeyUsage}
import com.digitalasset.canton.lifecycle.FutureUnlessShutdown
import com.digitalasset.canton.logging.NamedLoggerFactory
import com.digitalasset.canton.resource.{DbStorage, MemoryStorage, Storage}
import com.digitalasset.canton.tracing.TraceContext

import scala.concurrent.ExecutionContext

trait KmsMetadataStore {

  /** Get KMS key metadata from the store using the key's fingerprint
    */
  def get(fingerprint: Fingerprint)(implicit
      tc: TraceContext
  ): FutureUnlessShutdown[Option[KmsMetadata]]

  /** Store KMS metadata in the store
    */
  def store(
      metadata: KmsMetadata
  )(implicit ec: ExecutionContext, tc: TraceContext): FutureUnlessShutdown[Unit]

  /** Delete KMS key metadata from the store using its fingerprint
    */
  def delete(
      fingerprint: Fingerprint
  )(implicit ec: ExecutionContext, tc: TraceContext): FutureUnlessShutdown[Unit]

  /** List all KMS keys' metadata in the store
    */
  def list()(implicit
      ec: ExecutionContext,
      tc: TraceContext,
  ): FutureUnlessShutdown[List[KmsMetadata]]
}

object KmsMetadataStore {

  /** The data to be stored inside the KMS metadata store.
    *
    * @param usage If the key is for signing,
    *              this is the set of signing key usages that the key supports.
    *              Otherwise, it must be empty.
    */
  final case class KmsMetadata(
      fingerprint: Fingerprint,
      kmsKeyId: KmsKeyId,
      purpose: KeyPurpose,
      usage: Option[NonEmpty[Set[SigningKeyUsage]]] = None,
  ) {
    require(
      purpose == KeyPurpose.Encryption && usage.isEmpty ||
        purpose == KeyPurpose.Signing && usage.isDefined
    )
  }

  def create(
      storage: Storage,
      kmsCacheConfig: CacheConfig,
      timeouts: ProcessingTimeout,
      loggerFactory: NamedLoggerFactory,
  )(implicit ec: ExecutionContext): KmsMetadataStore =
    storage match {
      case _: MemoryStorage =>
        new InMemoryKmsMetadataStore(loggerFactory)
      case dbStorage: DbStorage =>
        new DbKmsMetadataStore(dbStorage, kmsCacheConfig, timeouts, loggerFactory)
    }
}
