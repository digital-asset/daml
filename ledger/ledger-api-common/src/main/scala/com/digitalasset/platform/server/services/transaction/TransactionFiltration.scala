// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.platform.server.services.transaction

import com.digitalasset.daml.lf.data.Ref.{Identifier, Party}
import com.digitalasset.daml.lf.transaction.Node.GenNode
import com.digitalasset.daml.lf.transaction.{GenTransaction, Node}
import com.digitalasset.ledger.api.domain.TransactionFilter

import scala.collection.{breakOut, immutable, mutable}

// This will be tested transitively by the semantic test suite.
object TransactionFiltration {

  private def templateId[I, C, V](node: GenNode[I, C, V]): Option[Identifier] = node match {
    case l: Node.NodeLookupByKey[I @unchecked, V @unchecked] => Some(l.templateId)
    case c: Node.NodeCreate[I @unchecked, V @unchecked] => Some(c.coinst.template)
    case e: Node.NodeExercises[I @unchecked, C @unchecked, V @unchecked] => Some(e.templateId)
    case _ => None
  }

  private def children[I](node: GenNode[I, _, _]): Iterator[I] = node match {
    case e: Node.NodeExercises[I @unchecked, _, _] => e.children.iterator
    case _ => Iterator.empty
  }

  private def collapse[T, U](invertedTransactionFilter: InvertedTransactionFilter[T, U]) =
    invertedTransactionFilter.specificSubscriptions
      .map {
        case (templateId, parties) =>
          (templateId, parties union invertedTransactionFilter.globalSubscribers)
      }
      .withDefaultValue(invertedTransactionFilter.globalSubscribers)

  implicit class RichTransactionFilter(val transactionFilter: TransactionFilter) extends AnyVal {

    /**
      * @return A nonempty map if with NodeId -> String mappings if any of them are visible.
      *         None otherwise.
      */
    def filter[Nid, Cid, Val](
        transaction: GenTransaction[Nid, Cid, Val],
        nidToString: Nid => String): Option[immutable.Map[String, immutable.Set[Party]]] = {

      val partiesByTemplate =
        collapse(
          InvertedTransactionFilter
            .extractFrom(transactionFilter))

      val filteredPartiesByNode = mutable.Map.empty[Nid, immutable.Set[Party]]
      val inheritedWitnessesByNode =
        mutable.Map.empty[Nid, immutable.Set[Party]].withDefaultValue(Set.empty)

      transaction.foreach(
        GenTransaction.TopDown, { (nodeId, node) =>
          templateId(node).foreach { tpl =>
            val requestingParties = partiesByTemplate(tpl)
            val inheritedWitnesses = inheritedWitnessesByNode(nodeId)
            val explicitWitnesses = explicitWitnessesForNode(node)
            val allWitnesses = inheritedWitnesses union explicitWitnesses
            val requestingWitnesses = requestingParties intersect allWitnesses

            filteredPartiesByNode += ((nodeId, requestingWitnesses))
            inheritedWitnessesByNode ++= children(node).map(_ -> allWitnesses)
          }
        }
      )

      if (filteredPartiesByNode.exists(_._2.nonEmpty)) {
        val nodeIdToParty: Map[String, immutable.Set[Party]] = filteredPartiesByNode.map {
          case (k, v) => (nidToString(k), v)
        }(breakOut)
        Some(nodeIdToParty)
      } else None
    }

    private def explicitWitnessesForNode(node: GenNode[_, _, _]): Set[Party] = node match {
      case n: Node.NodeCreate[_, _] => n.signatories union n.stakeholders
      case n: Node.NodeFetch[_] => n.signatories union n.stakeholders
      case n: Node.NodeExercises[_, _, _] =>
        if (n.consuming)
          n.signatories union n.stakeholders
        else
          n.signatories
      case _: Node.NodeLookupByKey[_, _] => Set.empty
    }
  }

}
