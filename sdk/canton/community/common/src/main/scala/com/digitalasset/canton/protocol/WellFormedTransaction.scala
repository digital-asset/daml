// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.protocol

import cats.data.{NonEmptyChain, Validated}
import cats.syntax.either.*
import cats.syntax.foldable.*
import cats.syntax.functor.*
import com.daml.nonempty.NonEmpty
import com.digitalasset.canton.data.ActionDescription
import com.digitalasset.canton.protocol.RollbackContext.{RollbackScope, RollbackSibling}
import com.digitalasset.canton.protocol.WellFormedTransaction.Stage
import com.digitalasset.canton.topology.PartyId
import com.digitalasset.canton.util.ShowUtil.*
import com.digitalasset.canton.util.{Checked, LfTransactionUtil, MonadUtil}
import com.digitalasset.canton.{checked, protocol}
import com.digitalasset.daml.lf.data.ImmArray

import scala.collection.immutable.HashMap
import scala.collection.mutable

/** Used to mark a transaction to be well-formed. That means:
  *   - `tx` is well-formed according to the Daml-LF definition, i.e., a root node is not child of
  *     another node and every non-root node has exactly one parent and is reachable from a root
  *     node. (No cycles, no sharing, no orphaned node).
  *   - All node Ids are non-negative.
  *   - The type parameter `S` determines whether all create nodes have suffixed IDs or none.
  *   - Create nodes have unique contract ids of shape
  *     `com.digitalasset.daml.lf.value.Value.ContractId`.
  *   - The contract id of a create node is not referenced before the node.
  *   - The contract id of a rolled back create node is not referenced outside its rollback scope in
  *     an active-contracts position.
  *   - Every action node has at least one signatory.
  *   - Every signatory is also a stakeholder.
  *   - Fetch actors are defined.
  *   - All created contract instances and choice arguments in the transaction can be serialized.
  *   - [[metadata]] contains seeds exactly for those node IDs from `tx` that should have a seed
  *     (creates and exercises).
  *   - Keys of transaction nodes don't contain contract IDs.
  *   - The maintainers of keys are non-empty.
  *   - ByKey nodes have a key.
  *   - All parties referenced by the transaction can be converted to
  *     [[com.digitalasset.canton.topology.PartyId]]
  *   - Every rollback node has at least one child and no rollback node appears at the root unless
  *     the transaction has been merged by Canton.
  */
final case class WellFormedTransaction[+S <: Stage] private (
    private val tx: LfVersionedTransaction,
    metadata: TransactionMetadata,
)(stage: S) {
  def unwrap: LfVersionedTransaction = tx

  def withoutVersion: LfTransaction = tx.transaction

  def seedFor(nodeId: LfNodeId): Option[LfHash] = metadata.seeds.get(nodeId)

  /** Adjust the node IDs in an LF transaction by the given (positive or negative) offset
    *
    * For example, an offset of 1 increases the NodeId of every node by 1. Ensures that the
    * transaction stays wellformed.
    *
    * @throws java.lang.IllegalArgumentException
    *   if `offset` is negative or a node ID overflow would occur
    */
  def tryAdjustNodeIds(offset: Int): WellFormedTransaction[S] = {
    require(offset >= 0, s"Offset must be non-negative: $offset")

    def adjustNodeId(nid: LfNodeId): LfNodeId = {
      val newIndex = nid.index + offset
      require(newIndex >= 0, s"Overflow for node id ${nid.index}")
      LfNodeId(nid.index + offset)
    }

    if (offset == 0) this
    else {
      val adjustedTx = tx.mapNodeId(adjustNodeId)
      val adjustedMetadata = metadata.copy(
        seeds = metadata.seeds.map { case (nodeId, seed) => adjustNodeId(nodeId) -> seed }
      )
      new WellFormedTransaction(adjustedTx, adjustedMetadata)(stage)
    }
  }
}

object WellFormedTransaction {

  /** Determines whether the IDs of created contracts in a transaction are suffixed and whether the
    * suffix must be absolute, and whether the transactions have been merged.
    */
  sealed trait Stage extends Product with Serializable {
    def withSuffixes: Boolean
    def onlyAbsoluteSuffixes: Boolean
    def merged: Boolean
  }

  /** All IDs of created contracts must have empty suffixes. */
  case object WithoutSuffixes extends Stage {
    override def withSuffixes: Boolean = false
    override def onlyAbsoluteSuffixes: Boolean = false
    override def merged: Boolean = false
  }
  type WithoutSuffixes = WithoutSuffixes.type

  /** All IDs of created contracts must have non-empty suffixes, possibly relative. The transaction
    * not yet merged.
    */
  case object WithSuffixes extends Stage {
    override def withSuffixes: Boolean = true
    override def onlyAbsoluteSuffixes: Boolean = false
    override def merged: Boolean = false
  }
  type WithSuffixes = WithSuffixes.type

  /** All IDs of created contracts must have non-empty absolute suffixes. The transaction not yet
    * merged.
    */
  case object WithAbsoluteSuffixes extends Stage {
    override def withSuffixes: Boolean = true
    override def onlyAbsoluteSuffixes: Boolean = true
    override def merged: Boolean = false
  }
  type WithAbsoluteSuffixes = WithAbsoluteSuffixes.type

  /** All IDs of created contracts must have non-empty absolute suffixes and transaction has been
    * "merged".
    */
  case object WithSuffixesAndMerged extends Stage {
    override def withSuffixes: Boolean = true
    override def onlyAbsoluteSuffixes: Boolean = true
    override def merged: Boolean = true
  }
  type WithSuffixesAndMerged = WithSuffixesAndMerged.type

  /** Creates a [[WellFormedTransaction]] if possible or an error message otherwise.
    */
  def check[S <: Stage](
      tx: LfVersionedTransaction,
      metadata: TransactionMetadata,
      stage: S,
  ): Either[String, WellFormedTransaction[S]] = {

    val result = for {
      _ <- checkForest(tx)
      _ <- checkNonNegativeNodeIds(tx)
      _ <- checkSeeds(tx, metadata.seeds)
      _ <- checkByKeyNodes(tx)
      _ <- checkCreatedContracts(tx, stage)
      _ <- checkFetchActors(tx)
      _ <- checkSignatoriesAndStakeholders(tx)
      _ <- checkNoContractIdsInKeysAndNonemptyMaintainers(tx)
      _ <- checkPartyNames(tx)
      _ <- checkSerialization(tx)
      _ <- checkRollbackNodes(tx, stage)
    } yield tx

    result.toEitherMergeNonaborts.bimap(
      _.toList.sorted.mkString(", "),
      new WellFormedTransaction(_, metadata)(stage),
    )
  }

  private def checkForest(
      tx: LfVersionedTransaction
  ): Checked[NonEmptyChain[String], String, Unit] = {
    val noForest = tx.transaction.isWellFormed
    val errors = noForest.toList.map(err => s"${err.reason}: ${err.nid.index}")
    Checked.fromEither(NonEmptyChain.fromSeq(errors).toLeft(()))
  }

  private def checkNonNegativeNodeIds(
      tx: LfVersionedTransaction
  ): Checked[Nothing, String, Unit] = {
    val negativeNodeIds = tx.nodes.keys.filter(_.index < 0)
    Checked.fromEitherNonabort(())(
      Either.cond(
        negativeNodeIds.isEmpty,
        (),
        s"Negative node IDs: ${negativeNodeIds.map(_.index).mkString(", ")}",
      )
    )
  }

  private def checkSeeds(
      tx: LfVersionedTransaction,
      seeds: Map[LfNodeId, LfHash],
  ): Checked[Nothing, String, Unit] = {
    val missingSeedsB = mutable.ListBuffer.newBuilder[LfNodeId]
    val superfluousSeedsB = mutable.ListBuffer.newBuilder[LfNodeId]

    tx.foreach { (nodeId, node) =>
      if (LfTransactionUtil.nodeHasSeed(node)) {
        if (!seeds.contains(nodeId)) missingSeedsB += nodeId
      } else if (seeds.contains(nodeId)) superfluousSeedsB += nodeId
    }

    val missingSeeds = missingSeedsB.result()
    val superfluousSeeds = superfluousSeedsB.result()

    val missing =
      Validated.condNec(
        missingSeeds.isEmpty,
        (),
        s"Nodes without seeds: ${missingSeeds.map(_.index).mkString(", ")}",
      )
    val superfluous = Validated.condNec(
      superfluousSeeds.isEmpty,
      (),
      s"Nodes with superfluous seeds: ${superfluousSeeds.map(_.index).mkString(", ")}",
    )

    Checked.fromEitherNonaborts(())(missing.product(superfluous).void.toEither)
  }

  private def checkByKeyNodes(tx: LfVersionedTransaction): Checked[Nothing, String, Unit] = {
    val byKeyNodesWithoutKey =
      tx.nodes.collect {
        case (nodeId, node: LfActionNode) if node.byKey && node.keyOpt.isEmpty => nodeId
      }.toList
    Checked.fromEitherNonabort(())(
      Either.cond(
        byKeyNodesWithoutKey.isEmpty,
        (),
        show"byKey nodes without a key: $byKeyNodesWithoutKey",
      )
    )
  }

  private def checkCreatedContracts(
      tx: LfVersionedTransaction,
      stage: Stage,
  ): Checked[Nothing, String, Unit] = {
    @SuppressWarnings(Array("org.wartremover.warts.Var"))
    var rbContext = RollbackContext.empty
    val referenced = mutable.Map.empty[LfContractId, LfNodeId]
    val created = mutable.Map.empty[LfContractId, (LfNodeId, RollbackScope)]

    val suffixViolations = mutable.ListBuffer.newBuilder[LfNodeId]
    val absoluteSuffixViolations = mutable.ListBuffer.newBuilder[LfContractId]

    // Check that references to a locally created contract are within the rollback scope of the creation.
    // Must only be checked for inputs to a node because reference inside an LF value can happen,
    // e.g., if the contract ID escapes its rollback scope via exceptions.
    def checkRollbackVisibility(nodeId: LfNodeId)(
        refId: LfContractId
    ): Checked[Nothing, String, Unit] =
      created.get(refId).traverse_ { case (createNodeId, createdScope) =>
        val referenceRbScope = rbContext.rollbackScope
        Checked.fromEitherNonabort(())(
          Either.cond(
            referenceRbScope.startsWith(createdScope),
            (),
            s"Contract $refId created node with $createNodeId in rollback scope ${createdScope
                .mkString(".")} referenced outside in rollback scope ${referenceRbScope.mkString(".")} of node $nodeId",
          )
        )
      }

    def addReference(nodeId: LfNodeId)(
        refId: LfContractId
    ): Unit = {
      referenced += (refId -> nodeId)
      if (stage.onlyAbsoluteSuffixes && !refId.isAbsolute)
        absoluteSuffixViolations += refId
    }

    def addReferencesByLfValue(nodeId: LfNodeId, refIds: LazyList[LfContractId]): Unit =
      refIds.foreach(addReference(nodeId))

    LfTransactionUtil
      .foldExecutionOrderM(tx.transaction, ()) { (nodeId, nodeExercise, _) =>
        val argRefs = nodeExercise.chosenValue.cids
        addReference(nodeId)(nodeExercise.targetCoid)
        addReferencesByLfValue(nodeId, argRefs.to(LazyList))
        checkRollbackVisibility(nodeId)(nodeExercise.targetCoid)
      } {
        case (nodeId, nf: LfNodeFetch, _) =>
          addReference(nodeId)(nf.coid)
          checkRollbackVisibility(nodeId)(nf.coid)
        case (nodeId, lookup: LfNodeLookupByKey, _) =>
          lookup.result.traverse_ { cid =>
            addReference(nodeId)(cid)
            checkRollbackVisibility(nodeId)(cid)
          }
        case (nodeId, nc: LfNodeCreate, _) =>
          val argRefs = nc.arg.cids
          addReferencesByLfValue(nodeId, argRefs.to(LazyList))
          for {
            _ <- referenced.get(nc.coid).traverse_ { otherNodeId =>
              Checked.continue(
                s"Contract id ${nc.coid.coid} created in node $nodeId is referenced before in $otherNodeId"
              )
            }
            _ <- created.put(nc.coid, (nodeId, rbContext.rollbackScope)).traverse_ {
              case (otherNodeId, _otherRbScope) =>
                Checked.continue(
                  s"Contract id ${nc.coid.coid} is created in nodes $otherNodeId and $nodeId"
                )
            }
            _ = if (nc.coid.isLocal == stage.withSuffixes) suffixViolations += nodeId else ()
          } yield ()
      } { (nodeId, ne, _) =>
        val resultRefs = ne.exerciseResult.map(_.cids).getOrElse(Set.empty)
        addReferencesByLfValue(nodeId, resultRefs.to(LazyList))
        Checked.unit
      } { (_, _, _) =>
        rbContext = rbContext.enterRollback
        Checked.unit
      } { (_, _, _) =>
        rbContext = rbContext.exitRollback
        Checked.unit
      }
      .flatMap { _ =>
        val suffixProblems = suffixViolations.result()
        Checked.fromEitherNonabort(())(
          Either.cond(
            suffixProblems.isEmpty,
            (),
            if (stage.withSuffixes)
              s"Created contracts of nodes lack suffixes: ${suffixProblems.map(_.index).mkString(", ")}"
            else
              s"Created contracts have suffixes in nodes ${suffixProblems.map(_.index).mkString(", ")}",
          )
        )
      }
      .flatMap { _ =>
        val absoluteSuffixProblems = absoluteSuffixViolations.result()
        Checked.fromEitherNonabort(())(
          Either.cond(
            absoluteSuffixProblems.isEmpty,
            (),
            s"Contract IDs without absolute suffixes: ${absoluteSuffixProblems.mkString(", ")}",
          )
        )
      }

  }

  private def checkNoContractIdsInKeysAndNonemptyMaintainers(
      tx: LfVersionedTransaction
  ): Checked[Nothing, String, Unit] =
    Checked.fromEitherNonaborts(())(
      tx.nodes
        .to(LazyList)
        .traverse_ {
          case (nodeId, node: LfActionNode) =>
            node.keyOpt match {
              case Some(k) =>
                val noCid = Validated.Valid(k.value)
                val nonemptyMaintainers =
                  Validated.condNec(
                    k.maintainers.nonEmpty,
                    (),
                    s"Key of node ${nodeId.index} has no maintainer",
                  )
                noCid.product(nonemptyMaintainers).void
              case None => Validated.Valid(())
            }
          case (_nodeId, _rn: LfNodeRollback) => Validated.Valid(())
        }
        .toEither
    )

  private def checkFetchActors(
      tx: LfVersionedTransaction
  ): Checked[NonEmptyChain[String], String, Unit] = {
    val missingFetchActors = tx.nodes.collect {
      case (nodeId, node: LfNodeFetch) if node.actingParties.isEmpty => nodeId.index
    }
    Checked.cond(
      missingFetchActors.isEmpty,
      (),
      NonEmptyChain.one(
        s"fetch nodes with unspecified acting parties at nodes ${missingFetchActors.mkString(", ")}"
      ),
    )
  }

  private def checkSignatoriesAndStakeholders(
      tx: LfVersionedTransaction
  ): Checked[Nothing, String, Unit] = {
    val noSignatoriesOrMaintainers = tx.nodes.collect {
      case (nodeId, node: LfActionNode)
          if LfTransactionUtil.signatoriesOrMaintainers(node).isEmpty =>
        nodeId.index
    }

    for {
      _ <-
        if (noSignatoriesOrMaintainers.isEmpty) Checked.unit
        else
          Checked.continue(
            s"neither signatories nor maintainers present at nodes ${noSignatoriesOrMaintainers.mkString(", ")}"
          )
      _ <- tx.nodes.to(LazyList).traverse_ {
        case (nodeId, an: LfActionNode) =>
          // Since we check for the fetch actors before, informees does not throw.
          val missingInformees =
            LfTransactionUtil.signatoriesOrMaintainers(an) -- an.informeesOfNode
          if (missingInformees.isEmpty) Checked.unit
          else
            Checked.continue(s"signatory or maintainer not declared as informee: ${missingInformees
                .mkString(", ")} at node ${nodeId.index}")
        case (_nodeId, _rn: LfNodeRollback) => Checked.unit
      }
    } yield ()
  }

  private def checkSerialization(tx: LfVersionedTransaction): Checked[Nothing, String, Unit] =
    tx.nodes.to(LazyList).traverse_ {
      case (nodeId, create: LfNodeCreate) =>
        Checked.fromEitherNonabort(())(
          SerializableRawContractInstance
            .create(create.versionedCoinst)
            .leftMap(err =>
              show"unable to serialize contract instance in node $nodeId: ${err.errorMessage.unquoted}"
            )
            .void
        )
      case (nodeId, exercise: LfNodeExercises) =>
        Checked.fromEitherNonabort(())(
          ActionDescription
            .serializeChosenValue(exercise.versionedChosenValue)
            .leftMap(err => show"unable to serialize chosen value in node $nodeId: ${err.unquoted}")
            .void
        )
      case (_, _: LfNodeFetch) => Checked.result(())
      case (_, _: LfNodeLookupByKey) => Checked.result(())
      case (_, _: LfNodeRollback) => Checked.result(())
    }

  private def checkPartyNames(tx: LfVersionedTransaction): Checked[Nothing, String, Unit] = {
    val lfPartyIds =
      tx.nodes.values
        .collect { case node: LfActionNode => node.informeesOfNode }
        .toSet
        .flatten
    lfPartyIds.to(LazyList).traverse_ { lfPartyId =>
      Checked.fromEitherNonabort(())(
        PartyId
          .fromLfParty(lfPartyId)
          .bimap(err => s"Unable to parse party: $err", _ => ())
      )
    }
  }

  private def checkRollbackNodes(
      tx: LfVersionedTransaction,
      stage: Stage,
  ): Checked[Nothing, String, Unit] =
    for {
      // check that root nodes of "unmerged transactions" never include rollback node (should have been peeled off by DAMLe.reinterpret)
      // Ensuring that no rollback nodes appear at the top in pre-merged transactions avoids the need to reconcile
      // rollback nodes described by the ViewParticipantData.rollbackContext and "duplicate" rollback nodes reintroduced
      // by DAMLe.reinterpret.
      _ <-
        if (stage.merged) Checked.unit
        else
          Checked.fromEitherNonabort(())(
            Either.cond(
              tx.roots.map(tx.nodes).toSeq.collectFirst { case nr: LfNodeRollback => nr }.isEmpty,
              (),
              "rollback node(s) not expected at the root of unmerged transaction",
            )
          )
      // ensure all rollback nodes always have at least one child
      _ <- tx.nodes
        .collect { case n @ (_, LfNodeRollback(children)) if children.length == 0 => n }
        .to(LazyList)
        .traverse_ { case (nodeId, _) =>
          Checked.continue(s"Rollback node $nodeId does not have children")
        }
    } yield ()

  sealed trait InvalidInput
  object InvalidInput extends {
    final case class InvalidParty(cause: String) extends InvalidInput
  }

  /** Sanity check the transaction before submission for any invalid input values
    *
    * Generally, the well-formedness check is used to detect faulty or malicious behaviour. This
    * method here can be used as a pre-check during submission to filter out any user input errors.
    */
  def sanityCheckInputs(
      tx: LfVersionedTransaction
  ): Either[InvalidInput, Unit] =
    for {
      _ <- checkPartyNames(tx).toEitherMergeNonaborts.leftMap(err =>
        InvalidInput.InvalidParty(err.head)
      )
    } yield ()

  /** Creates a [[WellFormedTransaction]]
    *
    * @throws java.lang.IllegalArgumentException
    *   if the given transaction is malformed
    */
  def checkOrThrow[S <: Stage](
      lfTransaction: LfVersionedTransaction,
      metadata: TransactionMetadata,
      state: S,
  ): WellFormedTransaction[S] =
    check(lfTransaction, metadata, state)
      .valueOr(err => throw new IllegalArgumentException(err))

  /** Merges a list of well-formed transactions into one, adjusting node IDs as necessary. All
    * transactions must have the same version.
    *
    * Root-level LfActionNodes with an associated rollback scope are embedded in rollback node
    * ancestors according to this scheme:
    *   1. Every root node is embedded in as many rollback nodes as level appear in its rollback
    *      scope.
    *   1. Nodes with shared, non-empty rollback scope prefixes (or full matches) share the same
    *      rollback nodes (or all on fully matching rollback scopes).
    *   1. While the lf-engine "collapses" away some rollback nodes as part of normalization,
    *      merging does not perform any normalization as the daml indexer/ReadService-consumer does
    *      not require rollback-normalized lf-transactions.
    */
  def merge(
      transactionsWithRollbackScope: NonEmpty[
        Seq[WithRollbackScope[WellFormedTransaction[WithAbsoluteSuffixes]]]
      ]
  ): Either[String, WellFormedTransaction[WithSuffixesAndMerged]] = {
    val mergedNodes = HashMap.newBuilder[LfNodeId, LfNode]
    val mergedRoots = Iterable.newBuilder[LfNodeId]
    val mergedSeeds = Map.newBuilder[LfNodeId, LfHash]

    val mutableRbNodes =
      mutable.HashMap[LfNodeId, mutable.ArrayDeque[LfNodeId]]() // mutable so we can append children

    val transactions = transactionsWithRollbackScope.map(_.unwrap)
    val ledgerTimes = transactions.map(_.metadata.ledgerTime).distinct
    val preparationTimes = transactions.map(_.metadata.preparationTime).distinct
    val versions = transactions.map(_.tx.version).distinct
    for {
      ledgerTime <- Either.cond(
        ledgerTimes.sizeCompare(1) == 0,
        ledgerTimes.head1,
        s"Different ledger times: ${ledgerTimes.mkString(", ")}",
      )
      preparationTime <- Either.cond(
        preparationTimes.sizeCompare(1) == 0,
        preparationTimes.head1,
        s"Different preparation times: ${preparationTimes.mkString(", ")}",
      )
      version = protocol.maxSerializationVersion(versions)
      _ <- MonadUtil
        .foldLeftM[Either[String, *], (Int, List[(RollbackSibling, LfNodeId)]), WithRollbackScope[
          WellFormedTransaction[WithAbsoluteSuffixes]
        ]](
          (0, List.empty), // start after node-id 0 with empty rollback scope
          transactionsWithRollbackScope.toList,
        ) { case ((freeNodeId, rbScopeWithNodeIds), WithRollbackScope(rbScope, wfTx)) =>
          val headNodeIds = wfTx.unwrap.nodes.keys

          val (rbPops, rbPushes) =
            RollbackScope.popsAndPushes(
              rbScopeWithNodeIds.map { case (rbSibling, _) => rbSibling },
              rbScope,
            )

          for {
            _ <- Either.cond(
              headNodeIds.forall(_.index >= 0),
              (),
              s"Cannot merge transactions with negative node ids",
            )
            maxNodeIdHead = headNodeIds.map(_.index).maxOption.getOrElse(0)
            nextFresh =
              freeNodeId + maxNodeIdHead + 1 + rbPushes // reserve node id space for rollback nodes to be pushed
            _ <- Either.cond(nextFresh >= 0, (), "Node id overflow during merge")
          } yield {
            val rbScopeCommon = rbScopeWithNodeIds.dropRight(rbPops)
            val rbScopeToPush = rbScope.drop(rbScopeCommon.length).zipWithIndex

            // Create new rollback nodes and connect rollback parents and children
            val newRbScope = rbScopeToPush.foldLeft(rbScopeCommon) {
              case (rbScopeParent, (rbSiblingIndex, nodeIdIndexIncrement)) =>
                val childNodeId = LfNodeId(freeNodeId + nodeIdIndexIncrement)
                rbScopeParent.lastOption match {
                  case Some((_, parentNodeId)) =>
                    mutableRbNodes(parentNodeId) += childNodeId
                  case None =>
                    mergedRoots += childNodeId
                }
                mutableRbNodes += childNodeId -> new mutable.ArrayDeque[LfNodeId]()
                rbScopeParent :+ ((rbSiblingIndex, childNodeId))
            }

            require(rbScope == newRbScope.map(_._1))

            // Add regular transaction nodes
            val adjustedHead = checked(wfTx.tryAdjustNodeIds(freeNodeId + rbPushes))
            mergedNodes ++= adjustedHead.unwrap.nodes

            // Record regular transaction root as roots in the absence of rollback nodes.
            newRbScope.lastOption.fold {
              val _ = mergedRoots ++= adjustedHead.unwrap.roots.toSeq
            } {
              // Otherwise place transaction roots under innermost rollback node.
              case (_rbChildIndex, rbParentNodeId) =>
                val _ = mutableRbNodes(rbParentNodeId) ++= adjustedHead.unwrap.roots.toSeq
            }
            mergedSeeds ++= adjustedHead.metadata.seeds
            (nextFresh, newRbScope)
          }
        }

      // Build actual rollback nodes now that we know all their children
      rollbackNodes = mutableRbNodes.map { case (nid, children) =>
        nid -> LfNodeRollback(children.to(ImmArray))
      }

      wrappedTx = LfVersionedTransaction(
        version,
        mergedNodes.result() ++ rollbackNodes,
        mergedRoots.result().to(ImmArray),
      )
      mergedMetadata = TransactionMetadata(ledgerTime, preparationTime, mergedSeeds.result())
      // TODO(M98) With tighter conditions on freshness of contract IDs, we shouldn't need this check.
      result <- check(wrappedTx, mergedMetadata, WithSuffixesAndMerged)
    } yield result
  }
}
