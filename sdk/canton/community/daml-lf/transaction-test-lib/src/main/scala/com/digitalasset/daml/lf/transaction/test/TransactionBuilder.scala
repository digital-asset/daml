// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.daml.lf
package transaction
package test

import com.digitalasset.daml.lf.data._
import com.digitalasset.daml.lf.value.Value
import com.digitalasset.daml.lf.value.Value.ContractId

import scala.collection.immutable.{HashMap, TreeSet}
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

  private def newTimestamp(): Time.Timestamp = {
    val randomMicrosSince0 = scala.util.Random.nextLong(
      Time.Timestamp.MaxValue.micros - Time.Timestamp.MinValue.micros + 1
    )
    Time.Timestamp.assertFromLong(randomMicrosSince0 + Time.Timestamp.MinValue.micros)
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
  def newV2Cid: ContractId.V2 = ContractId.V2.unsuffixed(newTimestamp(), newHash())

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
      SerializationVersion.minVersion, // A normalized empty tx is V10
      HashMap.empty,
      ImmArray.Empty,
    )
  val EmptySubmitted: SubmittedTransaction = SubmittedTransaction(Empty)
  val EmptyCommitted: CommittedTransaction = CommittedTransaction(Empty)

  def assignVersion[Cid](
      supportedVersions: VersionRange.Inclusive[SerializationVersion] =
        SerializationVersion.StableVersions
  ): SerializationVersion = supportedVersions.min

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

    implicit def toPackageRef(s: String): Ref.PackageRef =
      Ref.PackageRef.assertFromString(s)

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
    ): TemplateOrInterface[Ref.TypeConId, Ref.TypeConId] =
      x match {
        case TemplateOrInterface.Template(value) => TemplateOrInterface.Template(value)
        case TemplateOrInterface.Interface(value) => TemplateOrInterface.Interface(value)
      }

  }

  def valueRecord(id: Option[Ref.Identifier], fields: (Option[Ref.Name], Value)*) =
    Value.ValueRecord(id, fields.to(ImmArray))

  private[this] val DummyCid = Value.ContractId.V1.assertFromString("00" + "00" * 32)
  private[this] val DummyParties = List(Ref.Party.assertFromString("DummyParty"))

  /** Creates a FatContractInstance with dummy contract ID, signatories, observers, creation time and authentication
    * data. The signatories and observers may be overridden with non-dummy values if necessary. For testing purposes
    * only.
    */
  def fatContractInstanceWithDummyDefaults(
      version: SerializationVersion,
      packageName: Ref.PackageName,
      template: Ref.Identifier,
      arg: Value,
      signatories: Iterable[Ref.Party] = DummyParties,
      observers: Iterable[Ref.Party] = List.empty,
      contractKeyWithMaintainers: Option[GlobalKeyWithMaintainers] = None,
  ): FatContractInstance =
    FatContractInstanceImpl(
      version = version,
      contractId = DummyCid,
      packageName = packageName,
      templateId = template,
      createArg = arg,
      signatories = TreeSet.from(signatories),
      stakeholders = TreeSet.from(observers ++ signatories),
      contractKeyWithMaintainers = contractKeyWithMaintainers,
      createdAt = CreationTime.CreatedAt(Time.Timestamp.MinValue),
      authenticationData = Bytes.Empty,
    )
}
