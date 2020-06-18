// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf
package transaction
package test

import com.daml.lf.data.{ImmArray, Ref}
import com.daml.lf.data.Ref.{ChoiceName, Name}
import com.daml.lf.transaction.Node.GenNode
import com.daml.lf.transaction.{Transaction => Tx}
import com.daml.lf.value.Value.{ContractId, ContractInst}
import scala.collection.immutable.HashMap

final class TransactionBuilder {

  import TransactionBuilder._

  private[this] val newHash: () => crypto.Hash = {
    val bytes = Array.ofDim[Byte](crypto.Hash.underlyingHashLength)
    scala.util.Random.nextBytes(bytes)
    crypto.Hash.secureRandom(crypto.Hash.assertFromByteArray(bytes))
  }

  private val ids = Iterator.from(0).map(NodeId(_))
  private var nodes = HashMap.newBuilder[NodeId, TxNode]
  private val roots = ImmArray.newBuilder[NodeId]

  // not thread safe
  private[this] def newNode(node: Node): NodeId = {
    val nodeId = ids.next() // lazy to avoid getting the next id if the method later throws
    nodes += (nodeId -> version(node))
    nodeId
  }

  def add(node: Node): NodeId = ids.synchronized {
    val nodeId = newNode(node)
    roots += nodeId
    nodeId
  }

  def add(node: Node, parent: NodeId): NodeId = ids.synchronized {
    lazy val nodeId = newNode(node) // lazy to avoid getting the next id if the method later throws
    nodes = nodes.mapResult { ns =>
      val exercise = ns(parent) match {
        case exe: TxExercise => exe
        case _ =>
          throw new IllegalArgumentException(
            s"Node ${parent.index} either does not exist or is not an exercise")
      }
      ns.updated(parent, exercise.copy(children = exercise.children.slowSnoc(nodeId)))
    }
    nodeId
  }

  def build(): Tx.Transaction = ids.synchronized {
    TransactionVersions.assertAsVersionedTransaction(GenTransaction(nodes.result(), roots.result()))
  }

  def newCid: ContractId = ContractId.V1(newHash())

}

object TransactionBuilder {

  type Value = value.Value[ContractId]
  type TxValue = value.Value.VersionedValue[ContractId]
  type NodeId = Tx.NodeId
  type Node = Node.GenNode[NodeId, ContractId, Value]
  type TxNode = Node.GenNode[NodeId, ContractId, TxValue]

  type Create = Node.NodeCreate[ContractId, Value]
  type Exercise = Node.NodeExercises[NodeId, ContractId, Value]
  type Fetch = Node.NodeFetch[ContractId, Value]
  type LookupByKey = Node.NodeLookupByKey[ContractId, Value]
  type KeyWithMaintainers = com.daml.lf.transaction.Node.KeyWithMaintainers[Value]

  type TxExercise = Node.NodeExercises[NodeId, ContractId, TxValue]
  type TxKeyWithMaintainers = com.daml.lf.transaction.Node.KeyWithMaintainers[TxValue]

  private val ValueVersions = com.daml.lf.value.ValueVersions
  private val LfValue = com.daml.lf.value.Value

  private val NodeId = com.daml.lf.transaction.Transaction.NodeId
  private val Create = com.daml.lf.transaction.Node.NodeCreate
  private val Exercise = com.daml.lf.transaction.Node.NodeExercises
  private val Fetch = com.daml.lf.transaction.Node.NodeFetch
  private val LookupByKey = com.daml.lf.transaction.Node.NodeLookupByKey

  private val KeyWithMaintainers = com.daml.lf.transaction.Node.KeyWithMaintainers

  def version(v: Value): TxValue =
    ValueVersions.assertAsVersionedValue(v)

  def version(contract: ContractInst[Value]): ContractInst[TxValue] =
    ContractInst.map1[Value, TxValue](version)(contract)

  def version(key: KeyWithMaintainers): TxKeyWithMaintainers =
    KeyWithMaintainers.map1[Value, TxValue](version)(key)

  def version(node: Node): TxNode =
    GenNode.map3(identity[NodeId], identity[ContractId], version(_: Value))(node)

  def record(fields: (String, String)*): Value =
    LfValue.ValueRecord(
      tycon = None,
      fields = ImmArray(
        fields.map {
          case (name, value) =>
            (Some(Name.assertFromString(name)), LfValue.ValueText(value))
        },
      )
    )

  def tuple(values: String*): Value =
    record(values.zipWithIndex.map { case (v, i) => s"_$i" -> v }: _*)

  def keyWithMaintainers(maintainers: Seq[String], value: String): KeyWithMaintainers =
    KeyWithMaintainers(
      key = tuple(maintainers :+ value: _*),
      maintainers = maintainers.map(Ref.Party.assertFromString).toSet,
    )

  def create(
      id: String,
      template: String,
      argument: Value,
      signatories: Seq[String],
      observers: Seq[String],
      key: Option[String],
  ): Create =
    Create(
      coid = ContractId.assertFromString(id),
      coinst = ContractInst(
        template = Ref.Identifier.assertFromString(template),
        arg = argument,
        agreementText = "",
      ),
      optLocation = None,
      signatories = signatories.map(Ref.Party.assertFromString).toSet,
      stakeholders = signatories.union(observers).map(Ref.Party.assertFromString).toSet,
      key = key.map(keyWithMaintainers(maintainers = signatories, _)),
    )

  def exercise(
      contract: Create,
      choice: String,
      consuming: Boolean,
      actingParties: Set[String],
      argument: Value,
  ): Exercise =
    Exercise(
      targetCoid = contract.coid,
      templateId = contract.coinst.template,
      choiceId = ChoiceName.assertFromString(choice),
      optLocation = None,
      consuming = consuming,
      actingParties = actingParties.map(Ref.Party.assertFromString),
      chosenValue = argument,
      stakeholders = contract.stakeholders,
      signatories = contract.signatories,
      children = ImmArray.empty,
      exerciseResult = None,
      key = contract.key,
    )

  def fetch(contract: Create): Fetch =
    Fetch(
      coid = contract.coid,
      templateId = contract.coinst.template,
      optLocation = None,
      actingParties = Some(contract.signatories.map(Ref.Party.assertFromString)),
      signatories = contract.signatories,
      stakeholders = contract.stakeholders,
      key = contract.key,
    )

  def lookupByKey(contract: Create, found: Boolean): LookupByKey =
    LookupByKey(
      templateId = contract.coinst.template,
      optLocation = None,
      key = contract.key.get,
      result = if (found) Some(contract.coid) else None
    )

  def just(node: Node, nodes: Node*): Tx.Transaction = {
    val builder = new TransactionBuilder
    val _ = builder.add(node)
    for (node <- nodes) {
      val _ = builder.add(node)
    }
    builder.build()
  }

  def justSubmitted(node: Node, nodes: Node*): Tx.SubmittedTransaction =
    Tx.SubmittedTransaction(just(node, nodes: _*))

  def justCommitted(node: Node, nodes: Node*): Tx.CommittedTransaction =
    Tx.CommittedTransaction(just(node, nodes: _*))

  // not a valid transaction.
  val Empty: Tx.Transaction =
    Tx.CommittedTransaction(
      TransactionVersions.assertAsVersionedTransaction(
        GenTransaction(HashMap.empty, ImmArray.empty)
      )
    )

}
