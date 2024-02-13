// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.topology.store

import cats.syntax.functorFilter.*
import cats.syntax.parallel.*
import cats.syntax.traverse.*
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.logging.pretty.{Pretty, PrettyPrinting}
import com.digitalasset.canton.serialization.ProtoConverter
import com.digitalasset.canton.serialization.ProtoConverter.ParsingResult
import com.digitalasset.canton.topology.admin.v30
import com.digitalasset.canton.topology.processing.{
  AuthorizedTopologyTransaction,
  EffectiveTime,
  SequencedTime,
}
import com.digitalasset.canton.topology.transaction.TopologyChangeOp.{
  Add,
  Positive,
  Remove,
  Replace,
}
import com.digitalasset.canton.topology.transaction.*
import com.digitalasset.canton.util.FutureInstances.*
import com.digitalasset.canton.version.*

import scala.concurrent.{ExecutionContext, Future}

final case class StoredTopologyTransactions[+Op <: TopologyChangeOp](
    result: Seq[StoredTopologyTransaction[Op]]
) extends HasVersionedWrapper[StoredTopologyTransactions[TopologyChangeOp]]
    with PrettyPrinting {

  override protected def companionObj
      : HasVersionedMessageCompanionCommon[StoredTopologyTransactions[TopologyChangeOp]] =
    StoredTopologyTransactions

  override def pretty: Pretty[StoredTopologyTransactions.this.type] = prettyOfParam(
    _.result
  )

  def toTopologyState: List[TopologyStateElement[TopologyMapping]] =
    result.map(_.transaction.transaction.element).toList

  def toDomainTopologyTransactions: Seq[SignedTopologyTransaction[Op]] =
    result.map(_.transaction)

  def toProtoV30: v30.TopologyTransactions = v30.TopologyTransactions(
    items = result.map { item =>
      v30.TopologyTransactions.Item(
        sequenced = Some(item.sequenced.toProtoPrimitive),
        validFrom = Some(item.validFrom.toProtoPrimitive),
        validUntil = item.validUntil.map(_.toProtoPrimitive),
        // these transactions are serialized as versioned topology transactions
        transaction = item.transaction.getCryptographicEvidence,
      )
    }
  )

  def toAuthorizedTopologyTransactions[T <: TopologyMapping](
      collector: PartialFunction[TopologyMapping, T]
  ): Seq[AuthorizedTopologyTransaction[T]] = {

    val transactions: Seq[SignedTopologyTransaction[TopologyChangeOp]] = result.map(_.transaction)

    transactions.flatMap {
      case sit @ SignedTopologyTransaction(
            TopologyStateUpdate(Add, TopologyStateUpdateElement(_, mapping)),
            _key,
            _,
          ) =>
        collector
          .lift(mapping)
          .map { matched =>
            AuthorizedTopologyTransaction(sit.uniquePath, matched, sit)
          }
          .toList
      case _ => Seq()
    }
  }

  def collectOfType[T <: TopologyChangeOp](implicit
      checker: TopologyChangeOp.OpTypeChecker[T]
  ): StoredTopologyTransactions[T] = StoredTopologyTransactions(
    result.mapFilter(TopologyChangeOp.select[T])
  )

  def split: (
      StoredTopologyTransactions[Add],
      StoredTopologyTransactions[Remove],
      StoredTopologyTransactions[Replace],
  ) = {
    val (adds, removes, replaces) = TopologyTransactionSplitter[Op, StoredTopologyTransaction](
      collection = result,
      opProjector = _.transaction.operation,
      addSelector = TopologyChangeOp.select[Add](_),
      removeSelector = TopologyChangeOp.select[Remove](_),
      replaceSelector = TopologyChangeOp.select[Replace](_),
    )

    (
      StoredTopologyTransactions(adds),
      StoredTopologyTransactions(removes),
      StoredTopologyTransactions(replaces),
    )
  }

  def positiveTransactions: PositiveStoredTopologyTransactions = {
    val (adds, _, replaces) = split
    PositiveStoredTopologyTransactions(adds, replaces)
  }

  /** The timestamp of the last topology transaction (if there is at least one)
    * adjusted by topology change delay
    */
  def lastChangeTimestamp: Option[CantonTimestamp] = result
    .map(_.sequenced.value)
    .maxOption
}

object StoredTopologyTransactions
    extends HasVersionedMessageCompanion[
      StoredTopologyTransactions[TopologyChangeOp],
    ] {

  val supportedProtoVersions: SupportedProtoVersions = SupportedProtoVersions(
    ProtoVersion(30) -> ProtoCodec(
      ProtocolVersion.v30,
      supportedProtoVersion(v30.TopologyTransactions)(fromProtoV30),
      _.toProtoV30.toByteString,
    )
  )

  def fromProtoV30(
      value: v30.TopologyTransactions
  ): ParsingResult[StoredTopologyTransactions[TopologyChangeOp]] = {
    def parseItem(
        item: v30.TopologyTransactions.Item
    ): ParsingResult[StoredTopologyTransaction[TopologyChangeOp]] = {
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
        validUntil <- item.validFrom.traverse(EffectiveTime.fromProtoPrimitive)
        transaction <- SignedTopologyTransaction.fromByteStringUnsafe(
          item.transaction
        )
      } yield StoredTopologyTransaction(
        sequenced,
        validFrom,
        validUntil,
        transaction,
      )
    }
    value.items
      .traverse(parseItem)
      .map(StoredTopologyTransactions(_))
  }

  final case class CertsAndRest[+Op <: TopologyChangeOp](
      certs: Seq[StoredTopologyTransaction[Op]],
      rest: Seq[StoredTopologyTransaction[Op]],
  )

  def empty[Op <: TopologyChangeOp]: StoredTopologyTransactions[Op] =
    StoredTopologyTransactions(Seq())

  override def name: String = "topology transactions"
}

final case class PositiveStoredTopologyTransactions(
    adds: StoredTopologyTransactions[Add],
    replaces: StoredTopologyTransactions[Replace],
) {
  def toIdentityState: List[TopologyStateElement[TopologyMapping]] =
    adds.toTopologyState ++ replaces.toTopologyState

  def combine: StoredTopologyTransactions[Positive] = StoredTopologyTransactions(
    adds.result ++ replaces.result
  )

  def signedTransactions: PositiveSignedTopologyTransactions = PositiveSignedTopologyTransactions(
    SignedTopologyTransactions(adds.toDomainTopologyTransactions),
    SignedTopologyTransactions(replaces.toDomainTopologyTransactions),
  )
}

final case class SignedTopologyTransactions[+Op <: TopologyChangeOp](
    result: Seq[SignedTopologyTransaction[Op]]
) {
  def isEmpty: Boolean = result.isEmpty
  def size: Int = result.size

  def collectOfType[T <: TopologyChangeOp](implicit
      checker: TopologyChangeOp.OpTypeChecker[T]
  ): SignedTopologyTransactions[T] = SignedTopologyTransactions(
    result.mapFilter(TopologyChangeOp.select[T])
  )

  def split: (
      SignedTopologyTransactions[Add],
      SignedTopologyTransactions[Remove],
      SignedTopologyTransactions[Replace],
  ) = {
    val (adds, removes, replaces) = TopologyTransactionSplitter[Op, SignedTopologyTransaction](
      collection = result,
      opProjector = _.operation,
      addSelector = TopologyChangeOp.select[Add](_),
      removeSelector = TopologyChangeOp.select[Remove](_),
      replaceSelector = TopologyChangeOp.select[Replace](_),
    )

    (
      SignedTopologyTransactions(adds),
      SignedTopologyTransactions(removes),
      SignedTopologyTransactions(replaces),
    )
  }

  def splitForStateUpdate
      : (Seq[UniquePath], Seq[SignedTopologyTransaction[TopologyChangeOp.Positive]]) = {
    val (adds, removes, replaces) = split
    val deactivate = removes.result.map(_.uniquePath) ++ replaces.result.map(_.uniquePath)
    val positive = adds.result ++ replaces.result
    (deactivate, positive)
  }

  def filter(predicate: SignedTopologyTransaction[Op] => Boolean): SignedTopologyTransactions[Op] =
    this.copy(result = result.filter(predicate))

  def filter(
      predicate: SignedTopologyTransaction[Op] => Future[Boolean]
  )(implicit executionContext: ExecutionContext): Future[SignedTopologyTransactions[Op]] = {
    result.parTraverseFilter(tx => predicate(tx).map(Option.when(_)(tx))).map(this.copy)
  }
}

final case class PositiveSignedTopologyTransactions(
    adds: SignedTopologyTransactions[Add],
    replaces: SignedTopologyTransactions[Replace],
) {
  def filter(
      predicate: SignedTopologyTransaction[Positive] => Boolean
  ): PositiveSignedTopologyTransactions =
    this.copy(adds = adds.filter(predicate), replaces = replaces.filter(predicate))
}

object TopologyTransactionSplitter {
  import TopologyChangeOp.*

  def apply[Op <: TopologyChangeOp, F[_ <: TopologyChangeOp]](
      collection: Seq[F[TopologyChangeOp]],
      opProjector: F[TopologyChangeOp] => TopologyChangeOp,
      addSelector: F[TopologyChangeOp] => Option[F[Add]],
      removeSelector: F[TopologyChangeOp] => Option[F[Remove]],
      replaceSelector: F[TopologyChangeOp] => Option[F[Replace]],
  ): (Seq[F[Add]], Seq[F[Remove]], Seq[F[Replace]]) = {

    val (adds, removes, replaces) = {
      (
        Vector.newBuilder[F[Add]],
        Vector.newBuilder[F[Remove]],
        Vector.newBuilder[F[Replace]],
      )
    }
    // normally, most of the txs are adds, so we preallocate the size
    adds.sizeHint(collection.size)
    collection
      .map(e => (e, opProjector(e)))
      .foreach {
        case (element, Add) => addSelector(element).foreach(adds.addOne)
        case (element, Remove) => removeSelector(element).foreach(removes.addOne)
        case (element, Replace) => replaceSelector(element).foreach(replaces.addOne)
      }
    (adds.result(), removes.result(), replaces.result())
  }
}
