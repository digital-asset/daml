// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf
package transaction
package test

import com.daml.lf.data._
import com.daml.lf.language.LanguageVersion
import com.daml.lf.transaction.{Transaction => Tx}
import com.daml.lf.value.Value.ContractId

import scala.Ordering.Implicits.infixOrderingOps
import scala.annotation.tailrec
import scala.collection.immutable.HashMap
import scala.language.implicitConversions

final class TransactionBuilder(pkgTxVersion: Ref.PackageId => TransactionVersion) {

  import TransactionBuilder._

  private[this] val ids: Iterator[NodeId] = Iterator.from(0).map(NodeId(_))
  private[this] var nodes: Map[NodeId, TxNode] = HashMap.empty
  private[this] var children: Map[NodeId, BackStack[NodeId]] =
    HashMap.empty.withDefaultValue(BackStack.empty)
  private[this] var roots: BackStack[NodeId] = BackStack.empty

  private[this] def newNode(node: Node): NodeId = {
    val nodeId = ids.next()
    nodes += (nodeId -> node)
    nodeId
  }

  def add(node: Node): NodeId = ids.synchronized {
    val nodeId = newNode(node)
    roots = roots :+ nodeId
    nodeId
  }

  def add(node: Node, parentId: NodeId): NodeId = ids.synchronized {
    lazy val nodeId = newNode(node) // lazy to avoid getting the next id if the method later throws
    nodes(parentId) match {
      case _: TxExercise | _: TxRollback =>
        children += parentId -> (children(parentId) :+ nodeId)
      case _ =>
        throw new IllegalArgumentException(
          s"Node ${parentId.index} either does not exist or is not an exercise or rollback"
        )
    }
    nodeId
  }

  def build(): Tx.Transaction = ids.synchronized {
    import TransactionVersion.Ordering
    val finalNodes = nodes.transform {
      case (nid, rb: TxRollBack) =>
        rb.copy(children = children(nid).toImmArray)
      case (nid, exe: TxExercise) =>
        exe.copy(children = children(nid).toImmArray)
      case (_, node: Node.LeafOnlyActionNode[ContractId]) =>
        node
    }
    val finalRoots = roots.toImmArray
    val txVersion = finalRoots.iterator.foldLeft(TransactionVersion.minVersion)((acc, nodeId) =>
      finalNodes(nodeId).optVersion match {
        case Some(version) => acc max version
        case None => acc max TransactionVersion.minExceptions
      }
    )
    VersionedTransaction(txVersion, finalNodes, finalRoots)
  }

  def buildSubmitted(): SubmittedTransaction = SubmittedTransaction(build())

  def buildCommitted(): CommittedTransaction = CommittedTransaction(build())

  def newCid: ContractId = TransactionBuilder.newV1Cid

  def versionContract(contract: Value.ContractInst[Value]): value.Value.ContractInst[TxValue] =
    Value.ContractInst.map1[Value, TxValue](transactionValue(contract.template))(contract)

  private[this] def transactionValue(templateId: Ref.TypeConName): Value => TxValue =
    value.Value.VersionedValue(pkgTxVersion(templateId.packageId), _)

  def create(
      id: ContractId,
      templateId: Ref.Identifier,
      argument: Value,
      signatories: Set[Ref.Party],
      observers: Set[Ref.Party],
      key: Option[Value] = None,
  ): Create =
    create(id, templateId, argument, signatories, observers, key, signatories)

  def create(
      id: ContractId,
      templateId: Ref.Identifier,
      argument: Value,
      signatories: Set[Ref.Party],
      observers: Set[Ref.Party],
      key: Option[Value],
      maintainers: Set[Ref.Party],
  ): Create = {
    Create(
      coid = id,
      templateId = templateId,
      arg = argument,
      agreementText = "",
      signatories = signatories,
      stakeholders = signatories | observers,
      key = key.map(KeyWithMaintainers(_, maintainers)),
      version = pkgTxVersion(templateId.packageId),
    )
  }

  def exercise(
      contract: Create,
      choice: Ref.Name,
      consuming: Boolean,
      actingParties: Set[Ref.Party],
      argument: Value,
      result: Option[Value] = None,
      choiceObservers: Set[Ref.Party] = Set.empty,
      byKey: Boolean = true,
  ): Exercise =
    Exercise(
      choiceObservers = choiceObservers,
      targetCoid = contract.coid,
      templateId = contract.coinst.template,
      choiceId = choice,
      consuming = consuming,
      actingParties = actingParties,
      chosenValue = argument,
      stakeholders = contract.stakeholders,
      signatories = contract.signatories,
      children = ImmArray.Empty,
      exerciseResult = result,
      key = contract.key,
      byKey = byKey,
      version = pkgTxVersion(contract.coinst.template.packageId),
    )

  def exerciseByKey(
      contract: Create,
      choice: Ref.Name,
      consuming: Boolean,
      actingParties: Set[Ref.Party],
      argument: Value,
  ): Exercise =
    exercise(contract, choice, consuming, actingParties, argument, byKey = true)

  def fetch(contract: Create, byKey: Boolean = false): Fetch =
    Fetch(
      coid = contract.coid,
      templateId = contract.coinst.template,
      actingParties = contract.signatories.map(Ref.Party.assertFromString),
      signatories = contract.signatories,
      stakeholders = contract.stakeholders,
      key = contract.key,
      byKey = byKey,
      version = pkgTxVersion(contract.coinst.template.packageId),
    )

  def fetchByKey(contract: Create): Fetch =
    fetch(contract, byKey = true)

  def lookupByKey(contract: Create, found: Boolean): LookupByKey =
    LookupByKey(
      templateId = contract.coinst.template,
      key = contract.key.get,
      result = if (found) Some(contract.coid) else None,
      version = pkgTxVersion(contract.coinst.template.packageId),
    )

  def rollback(): Rollback =
    Rollback(
      children = ImmArray.Empty
    )
}

object TransactionBuilder {

  type Value = value.Value[ContractId]
  type TxValue = value.Value.VersionedValue[ContractId]
  type Node = Node.GenNode[NodeId, ContractId]
  type TxNode = Node.GenNode[NodeId, ContractId]

  type Create = Node.NodeCreate[ContractId]
  type Exercise = Node.NodeExercises[NodeId, ContractId]
  type Fetch = Node.NodeFetch[ContractId]
  type LookupByKey = Node.NodeLookupByKey[ContractId]
  type Rollback = Node.NodeRollback[NodeId]
  type KeyWithMaintainers = Node.KeyWithMaintainers[Value]

  type TxExercise = Node.NodeExercises[NodeId, ContractId]
  type TxRollback = Node.NodeRollback[NodeId]
  type TxKeyWithMaintainers = Node.KeyWithMaintainers[TxValue]
  type TxRollBack = Node.NodeRollback[NodeId]

  val Value = value.Value
  val Create = Node.NodeCreate
  val Exercise = Node.NodeExercises
  val Fetch = Node.NodeFetch
  val LookupByKey = Node.NodeLookupByKey
  val Rollback = Node.NodeRollback
  val KeyWithMaintainers = Node.KeyWithMaintainers

  def apply(
      pkgLangVersion: Ref.PackageId => LanguageVersion = _ => LanguageVersion.StableVersions.max
  ): TransactionBuilder =
    new TransactionBuilder(pkgId => TransactionVersion.assignNodeVersion(pkgLangVersion(pkgId)))

  private val newHash: () => crypto.Hash = {
    val bytes = Array.ofDim[Byte](crypto.Hash.underlyingHashLength)
    scala.util.Random.nextBytes(bytes)
    crypto.Hash.secureRandom(crypto.Hash.assertFromByteArray(bytes))
  }

  def record(fields: (String, String)*): Value =
    Value.ValueRecord(
      tycon = None,
      fields = fields.view
        .map { case (name, value) =>
          (Some(Ref.Name.assertFromString(name)), Value.ValueText(value))
        }
        .to(ImmArray),
    )

  def newV1Cid: ContractId.V1 = ContractId.V1(newHash())

  def newCid: ContractId = newV1Cid

  def just(node: Node, nodes: Node*): Tx.Transaction = {
    val builder = TransactionBuilder()
    val _ = builder.add(node)
    for (node <- nodes) {
      val _ = builder.add(node)
    }
    builder.build()
  }

  def justSubmitted(node: Node, nodes: Node*): SubmittedTransaction =
    SubmittedTransaction(just(node, nodes: _*))

  def justCommitted(node: Node, nodes: Node*): CommittedTransaction =
    CommittedTransaction(just(node, nodes: _*))

  // not valid transactions.
  val Empty: Tx.Transaction =
    VersionedTransaction(
      TransactionVersion.minVersion, // A normalized empty tx is V10
      HashMap.empty,
      ImmArray.Empty,
    )
  val EmptySubmitted: SubmittedTransaction = SubmittedTransaction(Empty)
  val EmptyCommitted: CommittedTransaction = CommittedTransaction(Empty)

  def assignVersion[Cid](
      v0: Value,
      supportedVersions: VersionRange[TransactionVersion] = TransactionVersion.StableVersions,
  ): Either[String, TransactionVersion] = {
    @tailrec
    def go(
        currentVersion: TransactionVersion,
        values0: FrontStack[Value],
    ): Either[String, TransactionVersion] = {
      import Value._
      if (currentVersion >= supportedVersions.max) {
        Right(currentVersion)
      } else {
        values0 match {
          case FrontStack() => Right(currentVersion)
          case FrontStackCons(value, values) =>
            value match {
              // for things supported since version 1, we do not need to check
              case ValueRecord(_, fs) => go(currentVersion, fs.map(v => v._2) ++: values)
              case ValueVariant(_, _, arg) => go(currentVersion, arg +: values)
              case ValueList(vs) => go(currentVersion, vs.toImmArray ++: values)
              case ValueContractId(_) | ValueInt64(_) | ValueText(_) | ValueTimestamp(_) |
                  ValueParty(_) | ValueBool(_) | ValueDate(_) | ValueUnit | ValueNumeric(_) =>
                go(currentVersion, values)
              case ValueOptional(x) =>
                go(currentVersion, x.fold(values)(_ +: values))
              case ValueTextMap(map) =>
                go(currentVersion, map.values ++: values)
              case ValueEnum(_, _) =>
                go(currentVersion, values)
              case ValueGenMap(entries) =>
                val newValues = entries.iterator.foldLeft(values) { case (acc, (key, value)) =>
                  key +: value +: acc
                }
                go(currentVersion max TransactionVersion.minGenMap, newValues)
            }
        }
      }
    }

    go(supportedVersions.min, FrontStack(v0)) match {
      case Right(inferredVersion) if supportedVersions.max < inferredVersion =>
        Left(s"inferred version $inferredVersion is not supported")
      case res =>
        res
    }

  }
  @throws[IllegalArgumentException]
  def assertAssignVersion(
      v0: Value,
      supportedVersions: VersionRange[TransactionVersion] = TransactionVersion.DevVersions,
  ): TransactionVersion =
    data.assertRight(assignVersion(v0, supportedVersions))

  def asVersionedValue(
      value: Value,
      supportedVersions: VersionRange[TransactionVersion] = TransactionVersion.DevVersions,
  ): Either[String, TxValue] =
    assignVersion(value, supportedVersions).map(Value.VersionedValue(_, value))

  @throws[IllegalArgumentException]
  def assertAsVersionedValue(
      value: Value,
      supportedVersions: VersionRange[TransactionVersion] = TransactionVersion.DevVersions,
  ): TxValue =
    data.assertRight(asVersionedValue(value, supportedVersions))

  object Implicits {

    implicit val defaultPackageId: Ref.PackageId = Ref.PackageId.assertFromString("pkgId")

    implicit def toContractId(s: String): ContractId =
      ContractId.assertFromString(s)

    implicit def toParty(s: String): Ref.Party =
      Ref.Party.assertFromString(s)

    implicit def toParties(s: Iterable[String]): Set[Ref.IdString.Party] =
      s.iterator.map(Ref.Party.assertFromString).toSet

    implicit def toName(s: String): Ref.Name =
      Ref.Name.assertFromString(s)

    implicit def toPackageId(s: String): Ref.PackageId =
      Ref.PackageId.assertFromString(s)

    implicit def toQualifiedName(s: String): Ref.QualifiedName =
      Ref.QualifiedName.assertFromString(s)

    implicit def toIdentifier(s: String)(implicit defaultPackageId: Ref.PackageId): Ref.Identifier =
      Ref.Identifier(defaultPackageId, s)

    implicit def toTimestamp(s: String): Time.Timestamp =
      Time.Timestamp.assertFromString(s)

    implicit def toDate(s: String): Time.Date =
      Time.Date.assertFromString(s)

    implicit def toNumeric(s: String): Numeric =
      Numeric.assertFromString(s)

    implicit def toOption[X](s: String)(implicit toX: String => X): Option[X] =
      if (s.isEmpty) None else Some(toX(s))

    implicit def toFields(list: ImmArray[(String, Value)]): ImmArray[(Option[Ref.Name], Value)] =
      list.map { case (name, value) => toOption(name)(toName) -> value }

  }

}
