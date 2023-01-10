// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf
package transaction
package test

import com.daml.lf.data._
import com.daml.lf.language.LanguageVersion
import com.daml.lf.value.Value
import com.daml.lf.value.Value.{ContractId, ContractInstance, VersionedContractInstance}

import java.util.concurrent.atomic.AtomicInteger
import scala.Ordering.Implicits.infixOrderingOps
import scala.annotation.tailrec
import scala.collection.immutable.HashMap
import scala.language.implicitConversions

final class TransactionBuilder(pkgTxVersion: Ref.PackageId => TransactionVersion) {

  private[this] val ids: Iterator[NodeId] = Iterator.from(0).map(NodeId(_))
  private[this] var nodes: Map[NodeId, Node] = HashMap.empty
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
      case _: Node.Exercise | _: Node.Rollback =>
        children += parentId -> (children(parentId) :+ nodeId)
      case _ =>
        throw new IllegalArgumentException(
          s"Node ${parentId.index} either does not exist or is not an exercise or rollback"
        )
    }
    nodeId
  }

  def build(): VersionedTransaction = ids.synchronized {
    import TransactionVersion.Ordering
    val finalNodes = nodes.transform {
      case (nid, rb: Node.Rollback) =>
        rb.copy(children = children(nid).toImmArray)
      case (nid, exe: Node.Exercise) =>
        exe.copy(children = children(nid).toImmArray)
      case (_, node: Node.LeafOnlyAction) =>
        node
    }
    val txVersion = finalNodes.values.foldLeft(TransactionVersion.minVersion)((acc, node) =>
      node.optVersion match {
        case Some(version) => acc max version
        case None => acc max TransactionVersion.minExceptions
      }
    )
    val finalRoots = roots.toImmArray
    VersionedTransaction(txVersion, finalNodes, finalRoots)
  }

  def buildSubmitted(): SubmittedTransaction = SubmittedTransaction(build())

  def buildCommitted(): CommittedTransaction = CommittedTransaction(build())

  def newCid: ContractId = TransactionBuilder.newV1Cid

  private val counter = new AtomicInteger()
  private def freshSuffix = counter.incrementAndGet().toString

  def newParty = Ref.Party.assertFromString("party" + freshSuffix)
  def newPackageId = Ref.PackageId.assertFromString("pkgId" + freshSuffix)
  def newModName = Ref.DottedName.assertFromString("Mod" + freshSuffix)
  def newChoiceName = Ref.Name.assertFromString("Choice" + freshSuffix)
  def newIdentifierName = Ref.DottedName.assertFromString("T" + freshSuffix)
  def newIdenfier = Ref.Identifier(newPackageId, Ref.QualifiedName(newModName, newIdentifierName))

  def versionContract(contract: Value.ContractInstance): value.Value.VersionedContractInstance =
    Versioned(pkgTxVersion(contract.template.packageId), contract)

  def create(
      id: ContractId,
      templateId: Ref.Identifier,
      argument: Value,
      signatories: Set[Ref.Party],
      observers: Set[Ref.Party],
      key: Option[Value] = None,
  ): Node.Create =
    create(id, templateId, argument, signatories, observers, key, signatories)

  def create(
      id: ContractId,
      templateId: Ref.Identifier,
      argument: Value,
      signatories: Set[Ref.Party],
      observers: Set[Ref.Party],
      key: Option[Value],
      maintainers: Set[Ref.Party],
  ): Node.Create = {
    Node.Create(
      coid = id,
      templateId = templateId,
      arg = argument,
      agreementText = "",
      signatories = signatories,
      stakeholders = signatories | observers,
      key = key.map(Node.KeyWithMaintainers(_, maintainers)),
      version = pkgTxVersion(templateId.packageId),
    )
  }

  def exercise(
      contract: Node.Create,
      choice: Ref.Name,
      consuming: Boolean,
      actingParties: Set[Ref.Party],
      argument: Value,
      interfaceId: Option[Ref.TypeConName] = None,
      result: Option[Value] = None,
      choiceObservers: Set[Ref.Party] = Set.empty,
      byKey: Boolean = true,
  ): Node.Exercise =
    Node.Exercise(
      choiceObservers = choiceObservers,
      targetCoid = contract.coid,
      templateId = contract.templateId,
      interfaceId = interfaceId,
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
      version = pkgTxVersion(contract.templateId.packageId),
    )

  def exerciseByKey(
      contract: Node.Create,
      choice: Ref.Name,
      consuming: Boolean,
      actingParties: Set[Ref.Party],
      argument: Value,
  ): Node.Exercise =
    exercise(contract, choice, consuming, actingParties, argument, byKey = true)

  def fetch(
      contract: Node.Create,
      byKey: Boolean = false,
  ): Node.Fetch =
    Node.Fetch(
      coid = contract.coid,
      templateId = contract.templateId,
      actingParties = contract.signatories.map(Ref.Party.assertFromString),
      signatories = contract.signatories,
      stakeholders = contract.stakeholders,
      key = contract.key,
      byKey = byKey,
      version = pkgTxVersion(contract.templateId.packageId),
    )

  def fetchByKey(contract: Node.Create): Node.Fetch =
    fetch(contract, byKey = true)

  def lookupByKey(contract: Node.Create, found: Boolean = true): Node.LookupByKey =
    Node.LookupByKey(
      templateId = contract.templateId,
      key = contract.key.get,
      result = if (found) Some(contract.coid) else None,
      version = pkgTxVersion(contract.templateId.packageId),
    )

  def rollback(): Node.Rollback =
    Node.Rollback(
      children = ImmArray.Empty
    )
}

object TransactionBuilder {

  type TxValue = value.Value.VersionedValue

  type KeyWithMaintainers = Node.KeyWithMaintainers
  type TxKeyWithMaintainers = Node.VersionedKeyWithMaintainers

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

  def just(node: Node, nodes: Node*): VersionedTransaction = {
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
  val Empty: VersionedTransaction =
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
        values0.pop match {
          case None => Right(currentVersion)
          case Some((value, values)) =>
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
    assignVersion(value, supportedVersions).map(Versioned(_, value))

  @throws[IllegalArgumentException]
  def assertAsVersionedValue(
      value: Value,
      supportedVersions: VersionRange[TransactionVersion] = TransactionVersion.DevVersions,
  ): TxValue =
    data.assertRight(asVersionedValue(value, supportedVersions))

  def asVersionedContract(
      contract: ContractInstance,
      supportedVersions: VersionRange[TransactionVersion] = TransactionVersion.DevVersions,
  ): Either[String, VersionedContractInstance] =
    assignVersion(contract.arg, supportedVersions)
      .map(Versioned(_, contract))

  def assertAsVersionedContract(
      contract: ContractInstance,
      supportedVersions: VersionRange[TransactionVersion] = TransactionVersion.DevVersions,
  ): VersionedContractInstance =
    data.assertRight(asVersionedContract(contract, supportedVersions))

  object Implicits {

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

    implicit def toDottedName(s: String): Ref.DottedName =
      Ref.DottedName.assertFromString(s)

    implicit def toQualifiedName(s: String): Ref.QualifiedName =
      Ref.QualifiedName.assertFromString(s)

    implicit val defaultPackageId: Ref.PackageId = "default Package ID"

    implicit def toIdentifier(s: String)(implicit defaultPackageId: Ref.PackageId): Ref.Identifier =
      Ref.Identifier(defaultPackageId, s)

    implicit def toTimestamp(s: String): Time.Timestamp =
      Time.Timestamp.assertFromString(s)

    implicit def toDate(s: String): Time.Date =
      Time.Date.assertFromString(s)

    implicit def toNumeric(s: String): Numeric =
      Numeric.assertFromString(s)

    private def toOption[X](s: String)(implicit toX: String => X): Option[X] =
      if (s.isEmpty) None else Some(toX(s))

    private def toTuple[X1, Y1, X2, Y2](
        tuple: (X1, Y1)
    )(implicit toX2: X1 => X2, toY2: Y1 => Y2): (X2, Y2) =
      (toX2(tuple._1), toY2(tuple._2))

    implicit def toOptionIdentifier(s: String)(implicit
        defaultPackageId: Ref.PackageId
    ): Option[Ref.Identifier] = toOption(s)

    implicit def toOptionName(s: String): Option[Ref.Name] = toOption(s)

    implicit def toField(t: (String, Value)): (Option[Ref.Name], Value) = toTuple(t)

    implicit def toTypeId(
        x: TemplateOrInterface[String, String]
    ): TemplateOrInterface[Ref.TypeConName, Ref.TypeConName] =
      x match {
        case TemplateOrInterface.Template(value) => TemplateOrInterface.Template(value)
        case TemplateOrInterface.Interface(value) => TemplateOrInterface.Interface(value)
      }

  }

  def valueRecord(id: Option[Ref.Identifier], fields: (Option[Ref.Name], Value)*) =
    Value.ValueRecord(id, fields.to(ImmArray))

}
