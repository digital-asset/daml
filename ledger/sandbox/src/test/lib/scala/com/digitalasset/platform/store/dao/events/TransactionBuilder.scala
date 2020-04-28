// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.store.dao.events

import com.daml.lf.data.ImmArray
import com.daml.lf.data.Ref.{ChoiceName, Name}

import scala.collection.immutable.HashMap
import scala.util.control.NonFatal

final class TransactionBuilder {

  import TransactionBuilder._

  private val ids = Iterator.from(1).map(NodeId(_))
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
    nodeId
  }

  def build(): Transaction = ids.synchronized {
    Transaction(nodes.result(), roots.result())
  }

}

object TransactionBuilder {

  private val ValueVersions = com.daml.lf.value.ValueVersions
  private val LfValue = com.daml.lf.value.Value
  private val Value = com.daml.lf.value.Value.VersionedValue

  private val NodeId = com.daml.lf.transaction.Transaction.NodeId
  private val Transaction = com.daml.lf.transaction.GenTransaction
  private val Create = com.daml.lf.transaction.Node.NodeCreate
  private val Exercise = com.daml.lf.transaction.Node.NodeExercises
  private val Fetch = com.daml.lf.transaction.Node.NodeFetch
  private val LookupByKey = com.daml.lf.transaction.Node.NodeLookupByKey
  private type LeafOnlyNode = com.daml.lf.transaction.Node.LeafOnlyNode.WithTxValue[ContractId]
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
      maintainers = maintainers.map(Party.assertFromString).toSet,
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
      coinst = Contract(
        template = Identifier.assertFromString(template),
        arg = argument,
        agreementText = "",
      ),
      optLocation = None,
      signatories = signatories.map(Party.assertFromString).toSet,
      stakeholders = signatories.union(observers).map(Party.assertFromString).toSet,
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
      actingParties = actingParties.map(Party.assertFromString),
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
      actingParties = Some(contract.signatories.map(Party.assertFromString)),
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

  def just(node: Node, nodes: Node*): Transaction = {
    val builder = new TransactionBuilder
    val _ = builder.add(node)
    for (node <- nodes) {
      val _ = builder.add(node)
    }
    builder.build()
  }

}
