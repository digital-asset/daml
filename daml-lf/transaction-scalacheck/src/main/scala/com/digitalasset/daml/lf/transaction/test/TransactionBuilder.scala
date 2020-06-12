// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf
package transaction
package test

import com.daml.lf.data.{ImmArray, Ref}
import com.daml.lf.data.Ref.{ChoiceName, Name}
import com.daml.lf.transaction.{Transaction => Tx}
import com.daml.lf.value.Value.{ContractId, ContractInst}

import scala.collection.immutable.HashMap
import scala.util.control.NonFatal

// For test purpose only
final class TransactionBuilder {

  import TransactionBuilder._

  private[this] val newHash: () => crypto.Hash = {
    val a = Array.ofDim[Byte](crypto.Hash.underlyingHashLength)
    scala.util.Random.nextBytes(a)
    crypto.Hash.secureRandom(crypto.Hash.assertFromByteArray(a))
  }

  private val ids = Iterator.from(0).map(NodeId(_))
  private var nodes = HashMap.newBuilder[NodeId, Node]
  private val roots = ImmArray.newBuilder[NodeId]

  def add(node: Node): NodeId = ids.synchronized {
    val nodeId = ids.next()
    nodes += (nodeId -> node)
    roots += nodeId
    nodeId
  }

  def add(node: Node, parent: NodeId): NodeId = ids.synchronized {
    lazy val nodeId = ids.next() // lazy to avoid getting the next id if the method later throws
    nodes = nodes.mapResult { ns =>
      val exercise = try {
        ns(parent).asInstanceOf[Exercise]
      } catch {
        case NonFatal(e) =>
          throw new IllegalArgumentException(
            s"Node ${parent.index} either does not exist or is not an exercise",
            e)
      }
      ns.updated(parent, exercise.copy(children = exercise.children.slowSnoc(nodeId)))
    }
    nodes += (nodeId -> node)
    nodeId
  }

  def build(): Tx.Transaction = ids.synchronized {
    GenTransaction(nodes.result(), roots.result())
  }

  def newCid: ContractId = ContractId.V1(newHash())

}

object TransactionBuilder {

  type Value = value.Value.VersionedValue[ContractId]
  type NodeId = Tx.NodeId
  type Node = Node.GenNode.WithTxValue[NodeId, ContractId]

  type Create = Node.NodeCreate.WithTxValue[ContractId]
  type Exercise = Node.NodeExercises.WithTxValue[NodeId, ContractId]
  type Fetch = Node.NodeFetch.WithTxValue[ContractId]
  type LookupByKey = Node.NodeLookupByKey.WithTxValue[ContractId]

  private val ValueVersions = com.daml.lf.value.ValueVersions
  private val LfValue = com.daml.lf.value.Value
  private val Value = com.daml.lf.value.Value.VersionedValue

  private val NodeId = com.daml.lf.transaction.Transaction.NodeId
  private val Create = com.daml.lf.transaction.Node.NodeCreate
  private val Exercise = com.daml.lf.transaction.Node.NodeExercises
  private val Fetch = com.daml.lf.transaction.Node.NodeFetch
  private val LookupByKey = com.daml.lf.transaction.Node.NodeLookupByKey
  private type KeyWithMaintainers = com.daml.lf.transaction.Node.KeyWithMaintainers[Value]
  private val KeyWithMaintainers = com.daml.lf.transaction.Node.KeyWithMaintainers

  private val LatestLfValueVersion = ValueVersions.acceptedVersions.last

  def record(fields: (String, String)*): Value =
    Value(
      LatestLfValueVersion,
      LfValue.ValueRecord(
        tycon = None,
        fields = ImmArray(
          fields.map {
            case (name, value) =>
              (Some(Name.assertFromString(name)), LfValue.ValueText(value))
          },
        )
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
    Tx.CommittedTransaction(GenTransaction(HashMap.empty, ImmArray.empty))

}
