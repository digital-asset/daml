// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.crypto.topology

import com.digitalasset.canton.checked
import com.digitalasset.canton.crypto.HashAlgorithm.Sha256
import com.digitalasset.canton.crypto.topology.TopologyStateHash.Builder
import com.digitalasset.canton.crypto.{Hash, HashPurpose}
import com.digitalasset.canton.topology.transaction.TopologyTransactionLike
import com.google.protobuf.ByteString

import java.nio.ByteBuffer
import java.security.MessageDigest

/** A utility class to compute a hash over the state of the topology store. Order of transactions
  * matters. Not thread-safe!
  */
class TopologyStateHash private (prefix: MessageDigest) {
  @SuppressWarnings(Array("org.wartremover.warts.AsInstanceOf"))
  lazy val hash: Hash = {
    val hash = ByteString.copyFrom(prefix.clone().asInstanceOf[MessageDigest].digest)
    checked(Hash.tryCreate(hash, Sha256))
  }

  /** Creates a new builder initialized with the current state of this hash.
    */
  @SuppressWarnings(Array("org.wartremover.warts.AsInstanceOf"))
  def extend(): Builder =
    new Builder(prefix.clone().asInstanceOf[MessageDigest])
}

object TopologyStateHash {
  def build(): Builder = {
    // Below we do the same as `HashBuilder` would do for the initial purpose
    val md: MessageDigest = MessageDigest.getInstance(Sha256.name)
    val purposeByteArray: Array[Byte] =
      ByteBuffer
        .allocate(java.lang.Integer.BYTES)
        .putInt(HashPurpose.InitialTopologyStateConsistency.id)
        .array()
    md.update(purposeByteArray)
    new Builder(md)
  }

  /** We do not use [[com.digitalasset.canton.crypto.HashBuilder]] since it is single-use only, but
    * we cache and reuse computation in this class in
    * [[com.digitalasset.canton.topology.store.db.DbTopologyStore]].
    */
  class Builder private[TopologyStateHash] (prefix: MessageDigest) {
    def add(tx: TopologyTransactionLike[?, ?]): Builder = add(tx.hash.hash)

    def add(txHash: Hash): Builder = {
      addByteString(txHash.getCryptographicEvidence)
      this
    }

    private def addByteString(b: ByteString): Unit =
      prefix.update(b.toByteArray)

    /** Returns the current resulting hash. Safe to be called multiple times.
      */
    @SuppressWarnings(Array("org.wartremover.warts.AsInstanceOf"))
    def finish(): TopologyStateHash =
      new TopologyStateHash(prefix.clone().asInstanceOf[MessageDigest])
  }
}
