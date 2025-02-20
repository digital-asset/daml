// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.topology.store

import cats.syntax.functorFilter.*
import cats.syntax.traverse.*
import com.daml.nonempty.NonEmptyReturningOps.*
import com.digitalasset.canton.config.CantonRequireTypes.String300
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.logging.pretty.{Pretty, PrettyPrinting}
import com.digitalasset.canton.serialization.ProtoConverter
import com.digitalasset.canton.serialization.ProtoConverter.ParsingResult
import com.digitalasset.canton.topology.admin.v30 as adminV30
import com.digitalasset.canton.topology.processing.{EffectiveTime, SequencedTime}
import com.digitalasset.canton.topology.store.StoredTopologyTransaction.GenericStoredTopologyTransaction
import com.digitalasset.canton.topology.store.TopologyStore.EffectiveStateChange
import com.digitalasset.canton.topology.transaction.*
import com.digitalasset.canton.version.*

import scala.reflect.ClassTag

final case class StoredTopologyTransactions[+Op <: TopologyChangeOp, +M <: TopologyMapping](
    result: Seq[StoredTopologyTransaction[Op, M]]
) extends HasVersionedWrapper[StoredTopologyTransactions[TopologyChangeOp, TopologyMapping]]
    with PrettyPrinting {

  override protected def companionObj: StoredTopologyTransactions.type = StoredTopologyTransactions

  override protected def pretty: Pretty[StoredTopologyTransactions.this.type] = prettyOfParam(
    _.result
  )

  def toTopologyState: List[M] =
    result.map(_.mapping).toList

  def toProtoV30: adminV30.TopologyTransactions = adminV30.TopologyTransactions(
    items = result.map { item =>
      adminV30.TopologyTransactions.Item(
        sequenced = Some(item.sequenced.toProtoPrimitive),
        validFrom = Some(item.validFrom.toProtoPrimitive),
        validUntil = item.validUntil.map(_.toProtoPrimitive),
        // these transactions are serialized as versioned topology transactions
        transaction = item.transaction.toByteString,
        rejectionReason = item.rejectionReason.map(_.unwrap),
      )
    }
  )

  def collectOfType[T <: TopologyChangeOp: ClassTag]: StoredTopologyTransactions[T, M] =
    StoredTopologyTransactions(
      result.mapFilter(_.selectOp[T])
    )

  def collectOfMapping[T <: TopologyMapping: ClassTag]: StoredTopologyTransactions[Op, T] =
    StoredTopologyTransactions(
      result.mapFilter(_.selectMapping[T])
    )

  def filter(
      pred: StoredTopologyTransaction[Op, M] => Boolean
  ): StoredTopologyTransactions[Op, M] =
    StoredTopologyTransactions(result.filter(stored => pred(stored)))

  def collectLatestByUniqueKey: StoredTopologyTransactions[Op, M] =
    StoredTopologyTransactions(TopologyTransactions.collectLatestByUniqueKey(result))

  def signedTransactions: Seq[SignedTopologyTransaction[Op, M]] = result.map(_.transaction)

  /** The timestamp of the last topology transaction (if there is at least one) */
  def lastChangeTimestamp: Option[CantonTimestamp] = result
    .map(_.sequenced.value)
    .maxOption

  def asSnapshotAtMaxEffectiveTime: StoredTopologyTransactions[Op, M] =
    result
      .map(_.validFrom.value)
      .maxOption
      .map { maxEffective =>
        // all transactions with a validUntil > the maxEffective should set validUntil to None, to reflect
        // the state of the transactions as of maxEffective
        StoredTopologyTransactions(result.map { storedTx =>
          if (storedTx.validUntil.exists(_.value > maxEffective)) {
            storedTx.copy(validUntil = None)
          } else storedTx
        })
      }
      .getOrElse(this) // this case is triggered by `result` being empty

  def toEffectiveStateChanges(
      fromEffectiveInclusive: CantonTimestamp,
      onlyAtEffective: Boolean,
  ): Seq[EffectiveStateChange] = {
    val validFromMap = result.groupBy(_.validFrom)
    val validUntilMap = result.groupBy(_.validUntil)
    // can contain effective times which are not meeting the effective criteria
    // (because the other valid field was meeting the criteria for the underlying tx)
    val allEffectiveTimes = validFromMap.keysIterator
      .++(validUntilMap.keysIterator.flatten)
      .toSet
      .toSeq
    for {
      effectiveTime <- allEffectiveTimes
      // only care about effective times, which meet the criteria
      if effectiveTime.value == fromEffectiveInclusive ||
        (!onlyAtEffective && effectiveTime.value > fromEffectiveInclusive)
      before = validUntilMap.getOrElse(Some(effectiveTime), Seq.empty)
      after = validFromMap.getOrElse(effectiveTime, Seq.empty)
      // for one sequenced time there is only one effective time
      // invariant: valid_until effective times always paired with a replace/remove with valid_from = the same effective time
      sequencedTime <- after.headOption.map(_.sequenced)
      positiveBefore = StoredTopologyTransactions(before).collectOfType[TopologyChangeOp.Replace]
      positiveAfter = StoredTopologyTransactions(after).collectOfType[TopologyChangeOp.Replace]
      // not caring about transactions resulting in no state change
      if positiveBefore.result.nonEmpty || positiveAfter.result.nonEmpty
    } yield EffectiveStateChange(
      effectiveTime = effectiveTime,
      sequencedTime = sequencedTime,
      before = positiveBefore,
      after = positiveAfter,
    )
  }

}

object StoredTopologyTransactions
    extends HasVersionedMessageCompanion[
      StoredTopologyTransactions[TopologyChangeOp, TopologyMapping],
    ] {

  type GenericStoredTopologyTransactions =
    StoredTopologyTransactions[TopologyChangeOp, TopologyMapping]
  type PositiveStoredTopologyTransactions =
    StoredTopologyTransactions[TopologyChangeOp.Replace, TopologyMapping]

  val supportedProtoVersions: SupportedProtoVersions = SupportedProtoVersions(
    ProtoVersion(30) -> ProtoCodec(
      ProtocolVersion.v33,
      supportedProtoVersion(adminV30.TopologyTransactions)(fromProtoV30),
      _.toProtoV30,
    )
  )

  def fromProtoV30(
      value: adminV30.TopologyTransactions
  ): ParsingResult[GenericStoredTopologyTransactions] = {
    def parseItem(
        item: adminV30.TopologyTransactions.Item
    ): ParsingResult[GenericStoredTopologyTransaction] =
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
        rejectionReason <- item.rejectionReason.traverse(
          String300.fromProtoPrimitive(_, "rejection_reason")
        )
        transaction <- SignedTopologyTransaction.fromTrustedByteStringPVV(item.transaction)
      } yield StoredTopologyTransaction(
        sequenced,
        validFrom,
        validUntil,
        transaction,
        rejectionReason,
      )
    value.items
      .traverse(parseItem)
      .map(StoredTopologyTransactions(_))
  }

  def empty: GenericStoredTopologyTransactions =
    StoredTopologyTransactions[TopologyChangeOp, TopologyMapping](Seq())

  override def name: String = "topology transactions"
}

object TopologyTransactions {

  /** Returns a list that only contains the latest serial per unique key without duplicates. The
    * input transactions might be returned in a different order.
    */
  def collectLatestByUniqueKey[
      T <: TopologyTransactionLike[TopologyChangeOp, TopologyMapping]
  ](transactions: Seq[T]): Seq[T] =
    transactions
      .groupBy1(_.mapping.uniqueKey)
      .view
      .mapValues { transactionsForUniqueKey =>
        transactionsForUniqueKey.maxBy1(_.serial)
      }
      .values
      .toSeq
}
