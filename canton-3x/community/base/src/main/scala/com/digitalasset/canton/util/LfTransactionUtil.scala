// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.util

import cats.{Monad, Order}
import com.daml.lf.data.*
import com.daml.lf.transaction.TransactionVersion
import com.daml.lf.value.Value
import com.digitalasset.canton.LfPartyId
import com.digitalasset.canton.protocol.*

import scala.annotation.nowarn

/** Helper functions to work with `com.digitalasset.daml.lf.transaction.GenTransaction`.
  * Using these helper functions is useful to provide a buffer from upstream changes.
  */
object LfTransactionUtil {

  implicit val orderTransactionVersion: Order[TransactionVersion] =
    Order.by[TransactionVersion, String](_.protoValue)(Order.fromOrdering)

  /** Return the template associated to a node.
    * Note: unlike [[nodeTemplates]] below, it does not return the interface package
    *       for exercise by interface nodes.
    */
  def nodeTemplate(node: LfActionNode): LfTemplateId = node match {
    case n: LfNodeCreate => n.coinst.template
    case n: LfNodeFetch => n.templateId
    case n: LfNodeExercises => n.templateId
    case n: LfNodeLookupByKey => n.templateId
  }

  /** Return the templates associated to a node. */
  def nodeTemplates(node: LfActionNode): Seq[LfTemplateId] = node match {
    case n: LfNodeCreate => Seq(n.coinst.template)
    case n: LfNodeFetch => Seq(n.templateId)
    case n: LfNodeExercises => n.templateId +: n.interfaceId.toList
    case n: LfNodeLookupByKey => Seq(n.templateId)
  }

  def consumedContractId(node: LfActionNode): Option[LfContractId] = node match {
    case _: LfNodeCreate => None
    case _: LfNodeFetch => None
    case nx: LfNodeExercises if nx.consuming => Some(nx.targetCoid)
    case _: LfNodeExercises => None
    case _: LfNodeLookupByKey => None
  }

  def contractId(node: LfActionNode): Option[LfContractId] = node match {
    case n: LfNodeCreate => Some(n.coid)
    case n: LfNodeFetch => Some(n.coid)
    case n: LfNodeExercises => Some(n.targetCoid)
    case n: LfNodeLookupByKey => n.result
  }

  def usedContractId(node: LfActionNode): Option[LfContractId] = node match {
    case n: LfNodeCreate => None
    case n: LfNodeFetch => Some(n.coid)
    case n: LfNodeExercises => Some(n.targetCoid)
    case n: LfNodeLookupByKey => n.result
  }

  /** All contract IDs referenced with a Daml `com.daml.lf.value.Value` */
  def referencedContractIds(value: Value): Set[LfContractId] = value.cids

  /** Whether or not a node has a random seed */
  def nodeHasSeed(node: LfNode): Boolean = node match {
    case _: LfNodeCreate => true
    case _: LfNodeExercises => true
    case _: LfNodeFetch => false
    case _: LfNodeLookupByKey => false
    case _: LfNodeRollback => false
  }

  private[this] def suffixForDiscriminator(
      unicumOfDiscriminator: LfHash => Option[Unicum],
      cantonContractId: CantonContractIdVersion,
  )(discriminator: LfHash): Bytes = {
    /* If we can't find the discriminator we leave it unchanged,
     * because this could refer to an input contract of the transaction.
     * The well-formedness checks ensure that unsuffixed discriminators of created contracts are fresh,
     * i.e., we suffix a discriminator either everywhere in the transaction or nowhere
     * even though the map from discriminators to unicum is built up in post-order of the nodes.
     */
    unicumOfDiscriminator(discriminator).fold(Bytes.Empty)(_.toContractIdSuffix(cantonContractId))
  }

  def suffixContractInst(
      unicumOfDiscriminator: LfHash => Option[Unicum],
      cantonContractId: CantonContractIdVersion,
  )(contractInst: LfContractInst): Either[String, LfContractInst] = {
    contractInst.unversioned
      .suffixCid(suffixForDiscriminator(unicumOfDiscriminator, cantonContractId))
      .map(unversionedContractInst => // traverse being added in daml-lf
        contractInst.map(_ => unversionedContractInst)
      )
  }

  def suffixNode(
      unicumOfDiscriminator: LfHash => Option[Unicum],
      cantonContractId: CantonContractIdVersion,
  )(node: LfActionNode): Either[String, LfActionNode] = {
    node.suffixCid(suffixForDiscriminator(unicumOfDiscriminator, cantonContractId))
  }

  /** Monadic visit to all nodes of the transaction in execution order.
    * Exercise nodes are visited twice: when execution reaches them and when execution leaves their body.
    * Crashes on malformed transactions (see `com.daml.lf.transaction.GenTransaction.isWellFormed`)
    */
  @nowarn("msg=match may not be exhaustive")
  def foldExecutionOrderM[F[_], A](tx: LfTransaction, initial: A)(
      exerciseBegin: (LfNodeId, LfNodeExercises, A) => F[A]
  )(
      leaf: (LfNodeId, LfLeafOnlyActionNode, A) => F[A]
  )(exerciseEnd: (LfNodeId, LfNodeExercises, A) => F[A])(
      rollbackBegin: (LfNodeId, LfNodeRollback, A) => F[A]
  )(rollbackEnd: (LfNodeId, LfNodeRollback, A) => F[A])(implicit F: Monad[F]): F[A] = {

    F.tailRecM(FrontStack.from(tx.roots.map(_ -> false)) -> initial) {
      case (FrontStack(), x) => F.pure(Right(x))
      case (FrontStackCons((nodeId, upwards), toVisit), x) =>
        tx.nodes(nodeId) match {
          case ne: LfNodeExercises =>
            if (upwards) F.map(exerciseEnd(nodeId, ne, x))(y => Left(toVisit -> y))
            else
              F.map(exerciseBegin(nodeId, ne, x))(y =>
                Left((ne.children.map(_ -> false) ++: (nodeId -> true) +: toVisit) -> y)
              )
          case nl: LfLeafOnlyActionNode => F.map(leaf(nodeId, nl, x))(y => Left(toVisit -> y))
          case nr: LfNodeRollback =>
            if (upwards) F.map(rollbackEnd(nodeId, nr, x))(y => Left(toVisit -> y))
            else
              F.map(rollbackBegin(nodeId, nr, x))(y =>
                Left((nr.children.map(_ -> false) ++: (nodeId -> true) +: toVisit) -> y)
              )
        }
    }
  }

  /** Given internally consistent transactions, compute their consumed contract ids. */
  def consumedContractIds(
      transactions: Iterable[LfVersionedTransaction]
  ): Set[LfContractId] =
    transactions.foldLeft(Set.empty[LfContractId]) { case (consumed, tx) =>
      consumed | tx.consumedContracts
    }

  /** Yields the signatories of the node's contract, or key maintainers for nodes without signatories.
    */
  val signatoriesOrMaintainers: LfActionNode => Set[LfPartyId] = {
    case n: LfNodeCreate => n.signatories
    case n: LfNodeFetch => n.signatories
    case n: LfNodeExercises => n.signatories
    case n: LfNodeLookupByKey => n.keyMaintainers
  }

  def stateKnownTo(node: LfActionNode): Set[LfPartyId] = node match {
    case n: LfNodeCreate => n.keyOpt.fold(n.stakeholders)(_.maintainers)
    case n: LfNodeFetch => n.stakeholders
    case n: LfNodeExercises => n.stakeholders
    case n: LfNodeLookupByKey =>
      n.result match {
        case None => n.keyMaintainers
        // TODO(#3013) use signatories or stakeholders
        case Some(_) => n.keyMaintainers
      }
  }

  /** Yields the the acting parties of the node, if applicable
    *
    * @throws java.lang.IllegalArgumentException if a Fetch node does not contain the acting parties.
    */
  val actingParties: LfActionNode => Set[LfPartyId] = {
    case _: LfNodeCreate => Set.empty

    case node: LfNodeFetch =>
      if (node.actingParties.isEmpty)
        throw new IllegalArgumentException(s"Fetch node $node without acting parties.")
      else
        node.actingParties

    case n: LfNodeExercises => n.actingParties

    case nl: LfNodeLookupByKey => nl.keyMaintainers
  }

  /** Compute the informees of a transaction based on the ledger model definition.
    *
    * Refer to https://docs.daml.com/concepts/ledger-model/ledger-privacy.html#projections
    */
  def informees(transaction: LfVersionedTransaction): Set[LfPartyId] = {
    val nodes: Set[LfActionNode] = transaction.nodes.values.collect { case an: LfActionNode =>
      an
    }.toSet
    nodes.flatMap(_.informeesOfNode)
  }

  val children: LfNode => Seq[LfNodeId] = {
    case ex: LfNodeExercises => ex.children.toSeq
    case _ => Seq.empty
  }

  /** Yields the light-weight version (i.e. without exercise children and result) of this node.
    *
    * @throws java.lang.UnsupportedOperationException if `node` is a rollback.
    */
  def lightWeight(node: LfActionNode): LfActionNode = {
    node match {
      case n: LfNodeCreate => n
      case n: LfNodeFetch => n
      case n: LfNodeExercises => n.copy(children = ImmArray.empty)
      case n: LfNodeLookupByKey => n
    }
  }

  def metadataFromExercise(node: LfNodeExercises): ContractMetadata =
    ContractMetadata.tryCreate(node.signatories, node.stakeholders, node.versionedKeyOpt)

  def metadataFromCreate(node: LfNodeCreate): ContractMetadata =
    ContractMetadata.tryCreate(node.signatories, node.stakeholders, node.versionedKeyOpt)

  def metadataFromFetch(node: LfNodeFetch): ContractMetadata =
    ContractMetadata.tryCreate(node.signatories, node.stakeholders, node.versionedKeyOpt)

}
