// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf.engine

import cats.Semigroup
import cats.implicits.catsSyntaxSemigroup
import com.daml.lf.data.Ref.{PackageId, Party}
import com.daml.lf.data._
import com.daml.lf.ledger._
import com.daml.lf.transaction._
import com.daml.lf.value.Value.ContractId

import scala.annotation.tailrec
import scala.collection.View

object Blinding {

  /** Given a transaction provide concise information on visibility
    * for all stakeholders
    *
    * We keep this in Engine since it needs the packages and your
    * typical engine already has a way to look those up and we do not
    * want to reinvent the wheel.
    *
    *  @param tx transaction to be blinded
    */
  def blind(tx: VersionedTransaction): BlindingInfo =
    BlindingTransaction.calculateBlindingInfo(tx)

  /** Returns the part of the transaction which has to be divulged to the given party.
    *
    * Note that if the child of a root node is divulged but the parent isn't, the child
    * will become a root note itself. Such nodes are "uprooted" in order, in the sense
    * that nodes that come before when traversing depth first, left to right will appear
    * first in the roots list.
    *
    * This also mean that there might be more roots in the divulged transaction than in
    * the original transaction.
    *
    * This function will crash if the transaction provided is malformed -- that is, if the
    * transaction has Nid references that are not present in its nodes. Use `isWellFormed`
    * if you are getting the transaction from a third party.
    */
  def divulgedTransaction(
      divulgences: Relation[NodeId, Party],
      party: Party,
      tx: Transaction,
  ): Transaction = {
    val partyDivulgences = Relation.invert(divulgences)(party)
    // Note that this relies on the local divulgence to be well-formed:
    // if an exercise node is divulged to A but some of its descendants
    // aren't the resulting transaction will not be well formed.
    val filteredNodes = tx.nodes.filter { case (k, _) => partyDivulgences.contains(k) }

    @tailrec
    def go(
        filteredRoots: BackStack[NodeId],
        remainingRoots: FrontStack[NodeId],
    ): ImmArray[NodeId] =
      remainingRoots.pop match {
        case None => filteredRoots.toImmArray
        case Some((root, remainingRoots)) =>
          if (partyDivulgences.contains(root)) {
            go(filteredRoots :+ root, remainingRoots)
          } else {
            tx.nodes(root) match {
              case nr: Node.Rollback =>
                go(filteredRoots, nr.children ++: remainingRoots)
              case _: Node.Fetch | _: Node.Create | _: Node.LookupByKey =>
                go(filteredRoots, remainingRoots)
              case ne: Node.Exercise =>
                go(filteredRoots, ne.children ++: remainingRoots)
            }
          }
      }

    Transaction(
      roots = go(BackStack.empty, tx.roots.toFrontStack),
      nodes = filteredNodes,
    )
  }

  private[engine] def partyPackages(
      tx: VersionedTransaction,
      disclosure: Relation[NodeId, Party],
      contractVisibility: Relation[ContractId, Party],
      contractPackages: Map[ContractId, Ref.PackageId],
  ): Map[Party, RequiredPackages] = {
    disclosedPartyPackages(tx, disclosure) ++
      contractPartyPackages(contractPackages, contractVisibility)
  }.groupMapReduce(_._1)(_._2)(_ |+| _)

  // These are the packages needed for input contract validation
  private def contractPartyPackages(
      contractPackages: Map[ContractId, Ref.PackageId],
      contractVisibility: Relation[ContractId, Party],
  ): View[(Party, RequiredPackages)] =
    for {
      (contractId, packageId) <- contractPackages.view
      party <- contractVisibility.getOrElse(contractId, Set.empty)
    } yield party -> RequiredPackages(readOnly = Set(packageId), execute = Set.empty)

  // These are the package needed for reinterpretation
  private[engine] def disclosedPartyPackages(
      tx: VersionedTransaction,
      disclosure: Relation[NodeId, Party],
  ): View[(Party, RequiredPackages)] =
    disclosure.view.flatMap { case (nodeId, parties) =>
      def readOnly(tyCon: Ref.TypeConName) =
        parties.view.map(
          _ -> RequiredPackages(readOnly = Set(tyCon.packageId), execute = Set.empty)
        )
      def execute(tyCon: Ref.TypeConName) =
        parties.view.map(
          _ -> RequiredPackages(readOnly = Set.empty, execute = Set(tyCon.packageId))
        )

      tx.nodes(nodeId) match {
        case fetch: Node.Fetch =>
          readOnly(fetch.templateId) ++ fetch.interfaceId.toList.view.flatMap(readOnly)
        case action: Node.LeafOnlyAction =>
          readOnly(action.templateId)
        case exe: Node.Exercise =>
          execute(exe.templateId) ++ exe.interfaceId.toList.view.flatMap(execute)
        case _: Node.Rollback =>
          Iterable.empty
      }
    }

  /** Calculate the packages needed by each party in order to reinterpret its projection.
    *
    * This needs to include both packages needed by the engine at reinterpretation time
    * and the originating contract package needed for contract model conformance checking.
    *
    * @param tx               transaction whose packages are required
    * @param contractPackages the contracts used by the transaction together with their creating packages
    */
  def partyPackages(
      tx: VersionedTransaction,
      contractPackages: Map[ContractId, Ref.PackageId] = Map.empty,
  ): Map[Party, RequiredPackages] = {
    val (BlindingInfo(disclosure, _), contractVisibility) =
      BlindingTransaction.calculateBlindingInfoWithContactVisibility(tx)
    partyPackages(tx, disclosure, contractVisibility, contractPackages)
  }
}

case class RequiredPackages(readOnly: Set[PackageId], execute: Set[PackageId]){
  def flattened: Set[PackageRequirement] = readOnly.map(ReadOnlyPackageRequirement) ++ execute.map(ExecutablePackageRequirement)
}

sealed trait PackageRequirement extends Product with Serializable{
  def pkgId: PackageId
}
final case class ExecutablePackageRequirement(pkgId: PackageId) extends PackageRequirement
final case class ReadOnlyPackageRequirement(pkgId: PackageId) extends PackageRequirement

object RequiredPackages {

  val empty: RequiredPackages = RequiredPackages(Set.empty, Set.empty)

  implicit val packagesUsedSemigroup: Semigroup[RequiredPackages] = Semigroup.instance { (x, y) =>
    val execute = x.execute ++ y.execute
    val readOnly = (x.readOnly ++ y.readOnly) -- execute
    RequiredPackages(
      readOnly = readOnly,
      execute = execute,
    )
  }
}
