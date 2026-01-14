// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.daml.lf
package transaction

import data.{Bytes, Ref, Time}
import value.{CidContainer, Value}

import scala.collection.immutable.TreeSet

// This should replace value.ThinContractInstance in the whole daml/canton codespace
// TODO: Rename to ContractInstance once value.ThinContractInstance is properly deprecated
sealed abstract class FatContractInstance extends CidContainer[FatContractInstance] {
  type CreatedAtTime <: CreationTime

  val version: SerializationVersion
  val contractId: Value.ContractId
  val packageName: Ref.PackageName
  val templateId: Ref.TypeConId
  val createArg: Value
  val signatories: TreeSet[Ref.Party]
  val stakeholders: TreeSet[Ref.Party]
  val contractKeyWithMaintainers: Option[GlobalKeyWithMaintainers]
  val createdAt: CreatedAtTime
  val authenticationData: Bytes
  private[lf] def toImplementation: FatContractInstanceImpl[CreatedAtTime] =
    this.asInstanceOf[FatContractInstanceImpl[CreatedAtTime]]
  final lazy val maintainers: TreeSet[Ref.Party] =
    contractKeyWithMaintainers.fold(TreeSet.empty[Ref.Party])(k => TreeSet.from(k.maintainers))
  final lazy val nonMaintainerSignatories: TreeSet[Ref.Party] = signatories -- maintainers
  final lazy val nonSignatoryStakeholders: TreeSet[Ref.Party] = stakeholders -- signatories
  final def updateCreateAt(
      updatedTime: Time.Timestamp
  ): FatContractInstance { type CreatedAtTime = CreationTime.CreatedAt } =
    mapCreatedAt(_ => CreationTime.CreatedAt(updatedTime))

  def mapCreatedAt[NewCreatedAtTime <: CreationTime](
      f: CreatedAtTime => NewCreatedAtTime
  ): FatContractInstance { type CreatedAtTime = NewCreatedAtTime }
  def traverseCreateAt[NewCreatedAtTime <: CreationTime, L](
      f: CreatedAtTime => Either[L, NewCreatedAtTime]
  ): Either[L, FatContractInstance { type CreatedAtTime = NewCreatedAtTime }]

  def setAuthenticationData(authenticationData: Bytes): FatContractInstance

  def toCreateNode = Node.Create(
    coid = contractId,
    packageName = packageName,
    templateId = templateId,
    arg = createArg,
    signatories = signatories,
    stakeholders = stakeholders,
    keyOpt = contractKeyWithMaintainers,
    version = version,
  )

  def nonVerboseWithoutTrailingNones: FatContractInstance
}

private[lf] final case class FatContractInstanceImpl[Time <: CreationTime](
    version: SerializationVersion,
    contractId: Value.ContractId,
    packageName: Ref.PackageName,
    templateId: Ref.TypeConId,
    createArg: Value,
    signatories: TreeSet[Ref.Party],
    stakeholders: TreeSet[Ref.Party],
    contractKeyWithMaintainers: Option[GlobalKeyWithMaintainers],
    createdAt: Time,
    authenticationData: Bytes,
) extends FatContractInstance
    with CidContainer[FatContractInstanceImpl[Time]] {

  override type CreatedAtTime = Time

  // TODO (change implementation of KeyWithMaintainers.maintainer to TreeSet)
  require(maintainers.isInstanceOf[TreeSet[Ref.Party]], "maintainers should be a TreeSet")
  require(maintainers.subsetOf(signatories), "maintainers should be a subset of signatories")
  require(signatories.nonEmpty, "signatories should be non empty")
  require(signatories.subsetOf(stakeholders), "signatories should be a subset of stakeholders")
  require(
    createdAt != CreationTime.Now || (!contractId.isAbsolute && !contractId.isLocal),
    "Creation time 'now' is not allowed for local and absolute contract ids",
  )
  require(
    contractKeyWithMaintainers.forall(_.globalKey.templateId == templateId),
    "template ID of the contract key must match the template ID of the contract",
  )

  override def mapCid(f: Value.ContractId => Value.ContractId): FatContractInstanceImpl[Time] = {
    copy(
      contractId = f(contractId),
      createArg = createArg.mapCid(f),
    )
  }

  override def mapCreatedAt[NewCreatedAtTime <: CreationTime](
      f: Time => NewCreatedAtTime
  ): FatContractInstance { type CreatedAtTime = NewCreatedAtTime } = {
    val newCreatedAtTime = f(createdAt)
    if (newCreatedAtTime eq createdAt)
      this.asInstanceOf[FatContractInstance { type CreatedAtTime = NewCreatedAtTime }]
    else copy(createdAt = newCreatedAtTime)
  }

  override def traverseCreateAt[NewCreatedAtTime <: CreationTime, L](
      f: Time => Either[L, NewCreatedAtTime]
  ): Either[L, FatContractInstance { type CreatedAtTime = NewCreatedAtTime }] =
    f(createdAt) match {
      case Right(newCreatedAtTime) =>
        if (newCreatedAtTime eq createdAt)
          Right(this.asInstanceOf[FatContractInstance { type CreatedAtTime = NewCreatedAtTime }])
        else Right(copy(createdAt = newCreatedAtTime))
      case Left(err) => Left(err)
    }

  override def setAuthenticationData(authenticationData: Bytes): FatContractInstanceImpl[Time] = {
    assert(authenticationData.nonEmpty)
    copy(authenticationData = authenticationData)
  }

  override def nonVerboseWithoutTrailingNones: FatContractInstance = FatContractInstanceImpl(
    version,
    contractId,
    packageName,
    templateId,
    createArg.nonVerboseWithoutTrailingNones,
    signatories: TreeSet[Ref.Party],
    stakeholders: TreeSet[Ref.Party],
    contractKeyWithMaintainers.map(_.nonVerbose),
    createdAt: Time,
    authenticationData: Bytes,
  )
}

object FatContractInstance {

  def fromCreateNode[T <: CreationTime](
      create: Node.Create,
      createTime: T,
      authenticationData: Bytes,
  ): FatContractInstance { type CreatedAtTime = T } =
    FatContractInstanceImpl(
      version = create.version,
      contractId = create.coid,
      packageName = create.packageName,
      templateId = create.templateId,
      createArg = create.arg,
      signatories = TreeSet.from(create.signatories),
      stakeholders = TreeSet.from(create.stakeholders),
      contractKeyWithMaintainers =
        create.keyOpt.map(k => k.copy(maintainers = TreeSet.from(k.maintainers))),
      createdAt = createTime,
      authenticationData = authenticationData,
    )
}

/** Trait for specifying the creation time of a contract */
sealed trait CreationTime extends Product with Serializable
object CreationTime {

  /** The creation time of a contract as an absolute timestamp.
    * This is ledger time of the creating transaction.
    */
  final case class CreatedAt(time: Time.Timestamp) extends CreationTime

  /** A symbolic point in time for contracts created in the same transaction,
    * when the ledger time of the transaction is not yet known.
    */
  case object Now extends CreationTime

  def encode(creationTime: CreationTime): Long =
    creationTime match {
      case CreatedAt(time) => time.micros
      case Now =>
        // Long.MinValue is outside of the range of valid micros for Timestamps
        Long.MinValue
    }

  def decode(encoded: Long): Either[String, CreationTime] =
    if (encoded == Long.MinValue) Right(Now)
    else Time.Timestamp.fromLong(encoded).map(CreatedAt)

  def assertDecode(encoded: Long): CreationTime =
    data.assertRight(decode(encoded))
}
