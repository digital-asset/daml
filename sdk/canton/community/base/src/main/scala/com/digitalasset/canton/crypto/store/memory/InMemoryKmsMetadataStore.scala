// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.crypto.store.memory

import cats.syntax.either.*
import com.digitalasset.canton.crypto.Fingerprint
import com.digitalasset.canton.crypto.store.KmsMetadataStore
import com.digitalasset.canton.crypto.store.KmsMetadataStore.KmsMetadata
import com.digitalasset.canton.discard.Implicits.DiscardOps
import com.digitalasset.canton.lifecycle.FutureUnlessShutdown
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.util.TrieMapUtil

import scala.collection.concurrent.TrieMap
import scala.concurrent.ExecutionContext

class InMemoryKmsMetadataStore(override val loggerFactory: NamedLoggerFactory)
    extends KmsMetadataStore
    with NamedLogging {

  private val metadataStore: TrieMap[Fingerprint, KmsMetadata] = TrieMap.empty

  /** Get a key from the store using its fingerprint
    */
  override def get(fingerprint: Fingerprint)(implicit
      tc: TraceContext
  ): FutureUnlessShutdown[Option[KmsMetadataStore.KmsMetadata]] = FutureUnlessShutdown.pure {
    metadataStore.get(fingerprint)
  }

  private def storeDuplicateError(
      key: Fingerprint,
      oldValue: KmsMetadata,
      newValue: KmsMetadata,
  ) =
    new IllegalStateException(
      s"Tried to insert $newValue but $oldValue already exists with the same fingerprint: $key"
    )

  /** Store kms metadata in the store
    */
  override def store(
      metadata: KmsMetadataStore.KmsMetadata
  )(implicit ec: ExecutionContext, tc: TraceContext): FutureUnlessShutdown[Unit] =
    TrieMapUtil
      .insertIfAbsent[Fingerprint, KmsMetadata, Throwable](
        metadataStore,
        metadata.fingerprint,
        metadata,
        storeDuplicateError _,
      )
      .liftTo[FutureUnlessShutdown]

  /** Delete a key from the store using its fingerprint
    */
  override def delete(
      fingerprint: Fingerprint
  )(implicit ec: ExecutionContext, tc: TraceContext): FutureUnlessShutdown[Unit] =
    FutureUnlessShutdown.pure {
      metadataStore.remove(fingerprint).discard
    }

  /** List all keys in the store
    */
  override def list()(implicit
      ec: ExecutionContext,
      tc: TraceContext,
  ): FutureUnlessShutdown[List[KmsMetadataStore.KmsMetadata]] = FutureUnlessShutdown.pure {
    metadataStore.values.toList
  }
}
