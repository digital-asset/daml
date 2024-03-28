// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.topology.store

import cats.syntax.functorFilter.*
import cats.syntax.traverse.*
import com.daml.nonempty.NonEmptyReturningOps.*
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.logging.pretty.{Pretty, PrettyPrinting}
import com.digitalasset.canton.serialization.ProtoConverter
import com.digitalasset.canton.serialization.ProtoConverter.ParsingResult
import com.digitalasset.canton.topology.admin.v30
import com.digitalasset.canton.topology.processing.{EffectiveTime, SequencedTime}
import com.digitalasset.canton.topology.store.StoredTopologyTransactionX.GenericStoredTopologyTransactionX
import com.digitalasset.canton.topology.transaction.SignedTopologyTransactionX.GenericSignedTopologyTransactionX
import com.digitalasset.canton.topology.transaction.*
import com.digitalasset.canton.version.*

import scala.reflect.ClassTag

final case class StoredTopologyTransactionsX[+Op <: TopologyChangeOpX, +M <: TopologyMappingX](
    result: Seq[StoredTopologyTransactionX[Op, M]]
) extends HasVersionedWrapper[StoredTopologyTransactionsX[TopologyChangeOpX, TopologyMappingX]]
    with PrettyPrinting {

  override protected def companionObj = StoredTopologyTransactionsX

  override def pretty: Pretty[StoredTopologyTransactionsX.this.type] = prettyOfParam(
    _.result
  )

  def toTopologyState: List[M] =
    result.map(_.mapping).toList

  def toProtoV30: v30.TopologyTransactions = v30.TopologyTransactions(
    items = result.map { item =>
      v30.TopologyTransactions.Item(
        sequenced = Some(item.sequenced.toProtoPrimitive),
        validFrom = Some(item.validFrom.toProtoPrimitive),
        validUntil = item.validUntil.map(_.toProtoPrimitive),
        // these transactions are serialized as versioned topology transactions
        transaction = item.transaction.toByteString,
      )
    }
  )

  def collectOfType[T <: TopologyChangeOpX: ClassTag]: StoredTopologyTransactionsX[T, M] =
    StoredTopologyTransactionsX(
      result.mapFilter(_.selectOp[T])
    )

  def collectOfMapping[T <: TopologyMappingX: ClassTag]: StoredTopologyTransactionsX[Op, T] =
    StoredTopologyTransactionsX(
      result.mapFilter(_.selectMapping[T])
    )

  def collectOfMapping(
      codes: TopologyMappingX.Code*
  ): StoredTopologyTransactionsX[TopologyChangeOpX, TopologyMappingX] = {
    val codeSet = codes.toSet
    StoredTopologyTransactionsX(
      result.filter(tx => codeSet(tx.mapping.code))
    )
  }

  def filter(
      pred: StoredTopologyTransactionX[Op, M] => Boolean
  ): StoredTopologyTransactionsX[Op, M] =
    StoredTopologyTransactionsX(result.filter(stored => pred(stored)))

  def collectLatestByUniqueKey: StoredTopologyTransactionsX[Op, M] = {
    val toRetain = result
      .groupBy1(_.mapping.uniqueKey)
      .view
      .mapValues(_.last1.hash)
      .values
      .toSet

    // filtering like this (instead of returning the values after groupBy1 directly)
    // retains the original order of the topology transactions
    StoredTopologyTransactionsX(
      result.filter(tx => toRetain(tx.hash))
    )
  }

  def signedTransactions: SignedTopologyTransactionsX[Op, M] = SignedTopologyTransactionsX(
    result.map(_.transaction)
  )

  /** The timestamp of the last topology transaction (if there is at least one) */
  def lastChangeTimestamp: Option[CantonTimestamp] = result
    .map(_.sequenced.value)
    .maxOption

  def asSnapshotAtMaxEffectiveTime: StoredTopologyTransactionsX[Op, M] = {
    result
      .map(_.validFrom.value)
      .maxOption
      .map { maxEffective =>
        // all transactions with a validUntil > the maxEffective should set validUntil to None, to reflect
        // the state of the transactions as of maxEffective
        StoredTopologyTransactionsX(result.map { storedTx =>
          if (storedTx.validUntil.exists(_.value > maxEffective)) {
            storedTx.copy(validUntil = None)
          } else storedTx
        })
      }
      .getOrElse(this) // this case is triggered by `result` being empty
  }

  def retainAuthorizedHistoryAndEffectiveProposals: StoredTopologyTransactionsX[Op, M] = {
    // only retain transactions that are:
    filter(tx =>
      // * fully authorized
      !tx.transaction.isProposal ||
        // * proposals that are still effective
        tx.validUntil.isEmpty
    )
  }
}

object StoredTopologyTransactionsX
    extends HasVersionedMessageCompanion[
      StoredTopologyTransactionsX[TopologyChangeOpX, TopologyMappingX],
    ] {

  type GenericStoredTopologyTransactionsX =
    StoredTopologyTransactionsX[TopologyChangeOpX, TopologyMappingX]
  type PositiveStoredTopologyTransactionsX =
    StoredTopologyTransactionsX[TopologyChangeOpX.Replace, TopologyMappingX]

  val supportedProtoVersions: SupportedProtoVersions = SupportedProtoVersions(
    ProtoVersion(30) -> ProtoCodec(
      ProtocolVersion.v30,
      supportedProtoVersion(v30.TopologyTransactions)(fromProtoV30),
      _.toProtoV30.toByteString,
    )
  )

  def fromProtoV30(
      value: v30.TopologyTransactions
  ): ParsingResult[GenericStoredTopologyTransactionsX] = {
    def parseItem(
        item: v30.TopologyTransactions.Item
    ): ParsingResult[GenericStoredTopologyTransactionX] = {
      for {
        sequenced <- ProtoConverter.parseRequired(
          SequencedTime.fromProtoPrimitive,
          "sequenced",
          item.sequenced,
        )
        validFrom <- ProtoConverter.parseRequired(
          EffectiveTime.fromProtoPrimitive,
          "valid_from",
          item.validFrom,
        )
        validUntil <- item.validUntil.traverse(EffectiveTime.fromProtoPrimitive)
        transaction <- SignedTopologyTransactionX.fromByteStringUnsafe(item.transaction)
      } yield StoredTopologyTransactionX(
        sequenced,
        validFrom,
        validUntil,
        transaction,
      )
    }
    value.items
      .traverse(parseItem)
      .map(StoredTopologyTransactionsX(_))
  }

  def empty: GenericStoredTopologyTransactionsX =
    StoredTopologyTransactionsX[TopologyChangeOpX, TopologyMappingX](Seq())

  override def name: String = "topology transactions"
}

final case class SignedTopologyTransactionsX[+Op <: TopologyChangeOpX, +M <: TopologyMappingX](
    result: Seq[SignedTopologyTransactionX[Op, M]]
) extends PrettyPrinting {

  override def pretty: Pretty[SignedTopologyTransactionsX.this.type] = prettyOfParam(
    _.result
  )

  def collectOfType[T <: TopologyChangeOpX: ClassTag]: SignedTopologyTransactionsX[T, M] =
    SignedTopologyTransactionsX(
      result.mapFilter(_.selectOp[T])
    )

  def collectOfMapping[T <: TopologyMappingX: ClassTag]: SignedTopologyTransactionsX[Op, T] =
    SignedTopologyTransactionsX(
      result.mapFilter(_.selectMapping[T])
    )
}

object SignedTopologyTransactionsX {
  type PositiveSignedTopologyTransactionsX =
    SignedTopologyTransactionsX[TopologyChangeOpX.Replace, TopologyMappingX]

  /** Merges the signatures of transactions with the same transaction hash,
    * while maintaining the order of the first occurrence of each hash.
    *
    * For example:
    * {{{
    * val original = Seq(hash_A, hash_B, hash_A, hash_C, hash_B)
    * compact(original) == Seq(hash_A, hash_B, hash_C)
    * }}}
    */
  def compact(
      txs: Seq[GenericSignedTopologyTransactionX]
  ): Seq[GenericSignedTopologyTransactionX] = {
    val byHash = txs
      .groupBy(_.hash)
      .view
      .mapValues(_.reduceLeftOption((tx1, tx2) => tx1.addSignatures(tx2.signatures.toSeq)))
      .collect { case (k, Some(v)) => k -> v }
      .toMap

    val (compacted, _) = {
      txs.foldLeft((Vector.empty[GenericSignedTopologyTransactionX], byHash)) {
        case ((result, byHash), tx) =>
          val newResult = byHash.get(tx.hash).map(result :+ _).getOrElse(result)
          val txHashRemoved = byHash.removed(tx.hash)
          (newResult, txHashRemoved)
      }
    }
    compacted
  }
}
