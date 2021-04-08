// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf
package transaction

import com.daml.lf.data.Ref

import scala.collection.immutable.TreeSet

object Util {

  import value.Value
  import value.Value._

  // Equivalent to serialization + unserialization.
  // Fails if :
  // - `value0` contains GenMap and version < 1.11
  def normalizeValue(
      value0: Value[ContractId],
      version: TransactionVersion,
  ): Either[String, Value[ContractId]] =
    try {
      Right(assertNormalizeValue(value0, version))
    } catch {
      case e: IllegalArgumentException => Left(e.getMessage)
    }

  // unsafe version of `normalize`
  @throws[IllegalArgumentException]
  def assertNormalizeValue(
      value0: Value[ContractId],
      version: TransactionVersion,
  ): Value[ContractId] = {

    import Ordering.Implicits.infixOrderingOps

    val allowGenMap = version >= TransactionVersion.minGenMap
    val eraseType = version >= TransactionVersion.minTypeErasure

    def handleTypeInfo[X](x: Option[X]) =
      if (eraseType) {
        None
      } else {
        x
      }

    def go(value: Value[ContractId]): Value[ContractId] =
      value match {
        case ValueEnum(tyCon, cons) =>
          ValueEnum(handleTypeInfo(tyCon), cons)
        case ValueRecord(tyCon, fields) =>
          ValueRecord(
            handleTypeInfo(tyCon),
            fields.map { case (fieldName, value) => handleTypeInfo(fieldName) -> go(value) },
          )
        case ValueBuiltinException(tag, value) =>
          ValueBuiltinException(tag, go(value))
        case ValueVariant(tyCon, variant, value) =>
          ValueVariant(handleTypeInfo(tyCon), variant, go(value))
        case _: ValueCidlessLeaf | _: ValueContractId[_] => value
        case ValueList(values) =>
          ValueList(values.map(go))
        case ValueOptional(value) =>
          ValueOptional(value.map(go))
        case ValueTextMap(value) =>
          ValueTextMap(value.mapValue(go))
        case ValueGenMap(entries) =>
          if (allowGenMap) {
            ValueGenMap(entries.map { case (k, v) => go(k) -> go(v) })
          } else {
            throw new IllegalArgumentException(
              s"GenMap are not allowed in transaction version $version"
            )
          }
      }

    go(value0)

  }

  def normalizeVersionedValue(
      value: VersionedValue[ContractId]
  ): Either[String, VersionedValue[ContractId]] =
    normalizeValue(value.value, value.version).map(normalized => value.copy(value = normalized))

  def normalizeContract(
      contract: ContractInst[VersionedValue[ContractId]]
  ): Either[String, ContractInst[VersionedValue[ContractId]]] =
    normalizeVersionedValue(contract.arg).map(normalized => contract.copy(arg = normalized))

  def normalizeKey(
      key: Node.KeyWithMaintainers[Value[ContractId]],
      version: TransactionVersion,
  ): Either[String, Node.KeyWithMaintainers[Value[ContractId]]] =
    normalizeValue(key.key, version).map(normalized => key.copy(key = normalized))

  def normalizeOptKey(
      key: Option[Node.KeyWithMaintainers[Value[ContractId]]],
      version: TransactionVersion,
  ): Either[String, Option[Node.KeyWithMaintainers[Value[ContractId]]]] =
    key match {
      case Some(value) => normalizeKey(value, version).map(Some(_))
      case None => Right(None)
    }

  /** Compute the inputs to a DAML transaction (in traversal order), that is, the referenced contracts, parties,
    *  and key.
    */
  def collectInputs(
      tx: Transaction.Transaction,
      addContract: ContractId => Unit,
      addParty: Ref.Party => Unit,
      addGlobalKey: GlobalKey => Unit,
  ): Unit = {

    val localContract = tx.localContracts

    def addContractInput(coid: ContractId): Unit =
      if (!localContract.isDefinedAt(coid))
        addContract(coid)

    def addPartyInputs(parties: Set[Ref.Party]): Unit =
      parties.toList.sorted(Ref.Party.ordering).foreach(addParty)

    def addContractKey(
        tmplId: Ref.Identifier,
        key: Node.KeyWithMaintainers[Value[ContractId]],
    ): Unit =
      addGlobalKey(GlobalKey.assertBuild(tmplId, key.key))

    tx.foreach { case (_, node) =>
      node match {
        case _: Node.NodeRollback[_] =>
          // TODO https://github.com/digital-asset/daml/issues/8020
          sys.error("rollback nodes are not supported")
        case fetch: Node.NodeFetch[Value.ContractId] =>
          addContractInput(fetch.coid)
          fetch.key.foreach(addContractKey(fetch.templateId, _))

        case create: Node.NodeCreate[Value.ContractId] =>
          create.key.foreach(addContractKey(create.templateId, _))

        case exe: Node.NodeExercises[NodeId, Value.ContractId] =>
          addContractInput(exe.targetCoid)
          exe.key.foreach(addContractKey(exe.templateId, _))

        case lookup: Node.NodeLookupByKey[Value.ContractId] =>
          // We need both the contract key state and the contract state. The latter is used to verify
          // that the submitter can access the contract.
          lookup.result.foreach(addContractInput)
          addContractKey(lookup.templateId, lookup.key)
      }

      addPartyInputs(node.informeesOfNode)
    }
  }

  def computeInputs(tx: Transaction.Transaction): Inputs = {
    val contractIds = TreeSet.newBuilder[ContractId](ContractId.`Cid Order`.toScalaOrdering)
    val parties = TreeSet.newBuilder[Ref.Party](Ref.Party.ordering)
    val globalKeys = TreeSet.newBuilder[GlobalKey]
    collectInputs(tx, contractIds.+=, parties.+=, globalKeys.+=)
    Inputs(
      contractIds.result(),
      parties.result(),
      globalKeys.result(),
    )
  }

  /** Compute the effects of a DAML transaction, that is, the created and consumed contracts. */
  def computeEffects(tx: Transaction.Transaction): Effects = {
    // TODO(JM): Skip transient contracts in createdContracts/updateContractKeys. E.g. rewrite this to
    //  fold bottom up (with reversed roots!) and skip creates of archived contracts.
    tx.fold(Effects.Empty) { case (effects, (_, node)) =>
      node match {
        case _: Node.NodeRollback[_] =>
          // TODO https://github.com/digital-asset/daml/issues/8020
          sys.error("rollback nodes are not supported")
        case _: Node.NodeFetch[Value.ContractId] =>
          effects
        case create: Node.NodeCreate[Value.ContractId] =>
          effects.copy(
            createdContracts = create.coid -> create :: effects.createdContracts,
            updatedContractKeys = create.key
              .fold(effects.updatedContractKeys)(keyWithMaintainers =>
                effects.updatedContractKeys.updated(
                  GlobalKey.assertBuild(create.coinst.template, keyWithMaintainers.key),
                  Some(create.coid),
                )
              ),
          )

        case exe: Node.NodeExercises[NodeId, Value.ContractId] =>
          if (exe.consuming) {
            effects.copy(
              consumedContracts = exe.targetCoid :: effects.consumedContracts,
              updatedContractKeys = exe.key
                .fold(effects.updatedContractKeys)(key =>
                  effects.updatedContractKeys.updated(
                    GlobalKey.assertBuild(exe.templateId, key.key),
                    None,
                  )
                ),
            )
          } else {
            effects
          }
        case _: Node.NodeLookupByKey[Value.ContractId] =>
          effects
      }
    }
  }

}
