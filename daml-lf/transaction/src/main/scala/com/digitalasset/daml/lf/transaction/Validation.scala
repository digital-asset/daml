// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf
package transaction

import com.daml.lf.data.ImmArray
import com.daml.lf.data.Ref.{Name, TypeConName}
import com.daml.lf.transaction.Node.KeyWithMaintainers
import com.daml.lf.value.Value
import scalaz.Equal

import scala.annotation.tailrec
import scala.collection.compat._
import scala.collection.compat.immutable.LazyList

private final class Validation[Nid, Cid](implicit ECid: Equal[Cid]) {

  import scalaz.std.option._
  import scalaz.syntax.equal._

  private[this] def tyConIsReplayedBy(
      recordedTyCon: Option[TypeConName],
      replayedTyCon: Option[TypeConName],
  ): Boolean =
    recordedTyCon.isEmpty || recordedTyCon == replayedTyCon

  private[this] def nameIsReplayedBy(tuple: (Option[Name], Option[Name])): Boolean =
    tuple match {
      case (None, _) => true
      case (recordedName, replayedName) => recordedName == replayedName
    }

  private[this] def keyIsReplayedBy(
      recorded: KeyWithMaintainers[Value[Cid]],
      replayed: KeyWithMaintainers[Value[Cid]],
  ): Boolean = {
    valueIsReplayedBy(recorded.key, replayed.key) && recorded.maintainers == replayed.maintainers
  }

  private[this] def keyIsReplayedBy(
      recorded: Option[KeyWithMaintainers[Value[Cid]]],
      replayed: Option[KeyWithMaintainers[Value[Cid]]],
  ): Boolean = {
    (recorded, replayed) match {
      case (None, None) => true
      case (Some(recordedValue), Some(replayedValue)) =>
        keyIsReplayedBy(recordedValue, replayedValue)
      case _ => false
    }
  }

  private[this] def resultIsReplayedBy(
      recorded: Option[Value[Cid]],
      replayed: Option[Value[Cid]],
  ) =
    (recorded, replayed) match {
      case (None, _) => true
      case (Some(recordedValue), Some(replayedValue)) =>
        valueIsReplayedBy(recordedValue, replayedValue)
      case _ => false
    }

  private[this] def byKeyIsReplacedBy(
      version: TransactionVersion,
      recorded: Boolean,
      replayed: Boolean,
  ): Boolean = {
    import scala.Ordering.Implicits.infixOrderingOps
    if (version >= TransactionVersion.minByKey) {
      recorded == replayed
    } else {
      true
    }
  }

  private[this] def keys[K](entries: ImmArray[(K, _)]): Iterator[K] =
    entries.iterator.map(_._1)

  private[this] def values[V](entries: ImmArray[(_, V)]): Iterator[V] =
    entries.iterator.map(_._2)

  private def valueIsReplayedBy(
      recorded: Value[Cid],
      replayed: Value[Cid],
  ): Boolean = {

    import Value._

    @tailrec
    def loop(tuples: LazyList[(Value[Cid], Value[Cid])]): Boolean =
      tuples match {
        case LazyList.cons(tuple, rest) =>
          tuple match {
            case (ValueEnum(recordedTyCon, recordedName), ValueEnum(replayedTyCon, replayedName)) =>
              tyConIsReplayedBy(recordedTyCon, replayedTyCon) &&
                recordedName == replayedName &&
                loop(rest)
            case (recordedLeaf: ValueCidlessLeaf, replayedLeaf: ValueCidlessLeaf) =>
              recordedLeaf == replayedLeaf &&
                loop(rest)
            case (ValueContractId(recordedValue), ValueContractId(replayedValue)) =>
              recordedValue === replayedValue &&
                loop(rest)
            case (
                  ValueRecord(recordedTyCon, recordedFields),
                  ValueRecord(replayedTyCon, replayedFields),
                ) =>
              tyConIsReplayedBy(recordedTyCon, replayedTyCon) &&
                recordedFields.length == replayedFields.length &&
                (keys(recordedFields) zip keys(replayedFields)).forall(nameIsReplayedBy) &&
                loop((values(recordedFields) zip values(recordedFields)) ++: rest)
            case (
                  ValueVariant(recordedTyCon, recordedVariant, recordedValue),
                  ValueVariant(replayedTyCon, replayedVariant, replayedValue),
                ) =>
              tyConIsReplayedBy(recordedTyCon, replayedTyCon) &&
                recordedVariant == replayedVariant &&
                loop((recordedValue, replayedValue) +: rest)
            case (ValueList(recordedValues), ValueList(replayedValues)) =>
              recordedValues.length == replayedValues.length &&
                loop((recordedValues.iterator zip replayedValues.iterator) ++: rest)
            case (ValueOptional(recordedValue), ValueOptional(replayedValue)) =>
              (recordedValue, replayedValue) match {
                case (Some(recorded), Some(replayed)) => loop((recorded, replayed) +: rest)
                case (None, None) => loop(rest)
                case _ => false
              }
            case (ValueTextMap(recordedEntries), ValueTextMap(replayedEntries)) =>
              recordedEntries.length == replayedEntries.length &&
                (keys(recordedEntries.toImmArray) sameElements keys(replayedEntries.toImmArray)) &&
                loop(
                  (values(recordedEntries.toImmArray) zip values(replayedEntries.toImmArray)) ++:
                    rest
                )
            case (ValueGenMap(recordedEntries), ValueGenMap(replayedEntries)) =>
              recordedEntries.length == replayedEntries.length &&
                loop(
                  (keys(recordedEntries) zip keys(replayedEntries)) ++:
                    (values(recordedEntries) zip values(replayedEntries)) ++:
                    rest
                )
            case _ =>
              false
          }
        case LazyList() =>
          true
      }
    loop(LazyList((recorded, replayed)))
  }

  /** Whether `replayed` is the result of reinterpreting this transaction.
    *
    * @param recorded : the transaction to be validated.
    * @param replayed : the transaction resulting from the reinterpretation of
    *   the root nodes of [[recorded]].
    * @note This function is asymmetric in order to provide backward compatibility.
    *      For instance, some field may be undefined in the [[recorded]] transaction
    *      while present in the [[replayed]] one.
    */
  private def isReplayedBy(
      recorded: VersionedTransaction[Nid, Cid],
      replayed: VersionedTransaction[Nid, Cid],
  ): Either[ReplayMismatch[Nid, Cid], Unit] = {

    type Exe = Node.NodeExercises[Nid, Cid]

    sealed trait StackEntry
    final case class ExerciseEntry(exe1: Exercise, exe2: Exercise) extends StackEntry
    final case class RollbackEntry(rb1: Rollback, rb2: Rollback) extends StackEntry
    final case class Exercise(
        nid: Nid,
        exe: Exe,
        children: LazyList[Nid],
    )
    final case class Rollback(
        nid: Nid,
        children: LazyList[Nid],
    )

    @tailrec
    def loop(
        nids1: LazyList[Nid],
        nids2: LazyList[Nid],
        stack: List[StackEntry] = List.empty,
    ): Either[ReplayMismatch[Nid, Cid], Unit] =
      (nids1, nids2) match {
        case (LazyList.cons(nid1, rest1), LazyList.cons(nid2, rest2)) =>
          (recorded.nodes(nid1), replayed.nodes(nid2)) match {
            case (
                  Node.NodeCreate(
                    coid1,
                    templateId1,
                    arg1,
                    agreementText1,
                    optLocation1 @ _,
                    signatories1,
                    stakeholders1,
                    key1,
                    version1,
                  ),
                  Node.NodeCreate(
                    coid2,
                    templateId2,
                    arg2,
                    agreementText2,
                    optLocation2 @ _,
                    signatories2,
                    stakeholders2,
                    key2,
                    version2,
                  ),
                )
                if version1 == version2 &&
                  coid1 === coid2 &&
                  templateId1 == templateId2 &&
                  valueIsReplayedBy(arg1, arg2) &&
                  agreementText1 == agreementText2 &&
                  signatories1 == signatories2 &&
                  stakeholders1 == stakeholders2 &&
                  keyIsReplayedBy(key1, key2) =>
              loop(rest1, rest2, stack)
            case (
                  Node.NodeFetch(
                    coid1,
                    templateId1,
                    optLocation1 @ _,
                    actingParties1,
                    signatories1,
                    stakeholders1,
                    key1,
                    byKey1,
                    version1,
                  ),
                  Node.NodeFetch(
                    coid2,
                    templateId2,
                    optLocation2 @ _,
                    actingParties2,
                    signatories2,
                    stakeholders2,
                    key2,
                    byKey2,
                    version2,
                  ),
                )
                if version1 == version2 &&
                  coid1 === coid2 &&
                  templateId1 == templateId2 &&
                  (actingParties1.isEmpty || actingParties1 == actingParties2) &&
                  signatories1 == signatories2 &&
                  stakeholders1 == stakeholders2 &&
                  (key1.isEmpty || keyIsReplayedBy(key1, key2)) &&
                  byKeyIsReplacedBy(version1, byKey1, byKey2) =>
              loop(rest1, rest2, stack)
            case (
                  exe1 @ Node.NodeExercises(
                    targetCoid1,
                    templateId1,
                    choiceId1,
                    optLocation1 @ _,
                    consuming1,
                    actingParties1,
                    chosenValue1,
                    stakeholders1,
                    signatories1,
                    choiceObservers1,
                    children1 @ _,
                    exerciseResult1 @ _,
                    key1,
                    byKey1,
                    version1,
                  ),
                  exe2 @ Node.NodeExercises(
                    targetCoid2,
                    templateId2,
                    choiceId2,
                    optLocation2 @ _,
                    consuming2,
                    actingParties2,
                    chosenValue2,
                    stakeholders2,
                    signatories2,
                    choiceObservers2,
                    children2 @ _,
                    exerciseResult2 @ _,
                    key2,
                    byKey2,
                    version2,
                  ),
                )
                // results are checked after the children
                if version1 == version2 &&
                  targetCoid1 === targetCoid2 &&
                  templateId1 == templateId2 &&
                  choiceId1 == choiceId2 &&
                  consuming1 == consuming2 &&
                  actingParties1 == actingParties2 &&
                  valueIsReplayedBy(chosenValue1, chosenValue2) &&
                  stakeholders1 == stakeholders2 &&
                  signatories1 == signatories2 &&
                  choiceObservers1 == choiceObservers2 &&
                  (key1.isEmpty || keyIsReplayedBy(key1, key2)) &&
                  byKeyIsReplacedBy(version1, byKey1, byKey2) =>
              loop(
                children1.iterator.to(LazyList),
                children2.iterator.to(LazyList),
                ExerciseEntry(Exercise(nid1, exe1, rest1), Exercise(nid2, exe2, rest2)) :: stack,
              )
            case (
                  Node.NodeLookupByKey(templateId1, optLocation1 @ _, key1, result1, version1),
                  Node.NodeLookupByKey(templateId2, optLocation2 @ _, key2, result2, version2),
                )
                if version1 == version2 &&
                  templateId1 == templateId2 &&
                  keyIsReplayedBy(key1, key2) &&
                  result1 === result2 =>
              loop(rest1, rest2, stack)
            case (
                  Node.NodeRollback(
                    children1
                  ),
                  Node.NodeRollback(
                    children2
                  ),
                ) =>
              loop(
                children1.iterator.to(LazyList),
                children2.iterator.to(LazyList),
                RollbackEntry(Rollback(nid1, rest1), Rollback(nid2, rest2)) :: stack,
              )
            case _ =>
              Left(ReplayNodeMismatch(recorded, nid1, replayed, nid2))
          }

        case (LazyList(), LazyList()) =>
          stack match {
            case ExerciseEntry(Exercise(nid1, exe1, nids1), Exercise(nid2, exe2, nids2)) :: rest =>
              if (resultIsReplayedBy(exe1.exerciseResult, exe2.exerciseResult))
                loop(nids1, nids2, rest)
              else
                Left(ReplayNodeMismatch(recorded, nid1, replayed, nid2))
            case RollbackEntry(Rollback(_, nids1), Rollback(_, nids2)) :: rest =>
              loop(nids1, nids2, rest)
            case Nil =>
              Right(())
          }

        case (LazyList.cons(nid1, _), LazyList()) =>
          Left(ReplayedNodeMissing(recorded, nid1, replayed))

        case (LazyList(), LazyList.cons(nid2, _)) =>
          Left(RecordedNodeMissing(recorded, replayed, nid2))

      }

    loop(recorded.roots.iterator.to(LazyList), replayed.roots.iterator.to(LazyList))

  }

}

object Validation {

  def isReplayedBy[Nid, Cid](
      recorded: VersionedTransaction[Nid, Cid],
      replayed: VersionedTransaction[Nid, Cid],
  )(implicit ECid: Equal[Cid]): Either[ReplayMismatch[Nid, Cid], Unit] =
    new Validation().isReplayedBy(recorded, replayed)

  // package private for test.
  private[lf] def valueIsReplayedBy[Cid](recorded: Value[Cid], replayed: Value[Cid])(implicit
      ECid: Equal[Cid]
  ): Boolean =
    new Validation().valueIsReplayedBy(recorded, replayed)

}

sealed abstract class ReplayMismatch[Nid, Cid] extends Product with Serializable {
  def recordedTransaction: VersionedTransaction[Nid, Cid]
  def replayedTransaction: VersionedTransaction[Nid, Cid]

  def msg: String =
    s"recreated and original transaction mismatch $recordedTransaction expected, but $replayedTransaction is recreated"
}

final case class ReplayNodeMismatch[Nid, Cid](
    override val recordedTransaction: VersionedTransaction[Nid, Cid],
    recordedNode: Nid,
    override val replayedTransaction: VersionedTransaction[Nid, Cid],
    replayedNode: Nid,
) extends ReplayMismatch[Nid, Cid]

final case class RecordedNodeMissing[Nid, Cid](
    override val recordedTransaction: VersionedTransaction[Nid, Cid],
    override val replayedTransaction: VersionedTransaction[Nid, Cid],
    replayedNode: Nid,
) extends ReplayMismatch[Nid, Cid]

final case class ReplayedNodeMissing[Nid, Cid](
    override val recordedTransaction: VersionedTransaction[Nid, Cid],
    recordedNode: Nid,
    override val replayedTransaction: VersionedTransaction[Nid, Cid],
) extends ReplayMismatch[Nid, Cid]
