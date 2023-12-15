// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf
package transaction
package test

import com.daml.lf.data._
import com.daml.lf.value.Value
import com.daml.lf.value.Value.{ContractId, ContractInstance, VersionedContractInstance}

import scala.Ordering.Implicits.infixOrderingOps
import scala.annotation.tailrec
import scala.collection.immutable.HashMap
import scala.language.implicitConversions

object TransactionBuilder {

  type TxValue = value.Value.VersionedValue

  type KeyWithMaintainers = GlobalKeyWithMaintainers
  type TxKeyWithMaintainers = Versioned[GlobalKeyWithMaintainers]

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
    val builder = new NodeIdTransactionBuilder()
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
                go(currentVersion, newValues)
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

    implicit def toParties(s: Iterable[String]): Set[Ref.Party] =
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

    implicit def toTypeConRef(s: String)(implicit defaultPackageId: Ref.PackageId): Ref.TypeConRef =
      Ref.TypeConRef(Ref.PackageRef.Id(defaultPackageId), s)

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
