// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf.kv.transactions

import com.daml.lf.data.Ref.Party
import com.daml.lf.data.{FrontStack, FrontStackCons, ImmArray, Ref}
import com.daml.lf.kv.ConversionError
import com.daml.lf.transaction.TransactionOuterClass.Node
import com.daml.lf.transaction.{TransactionCoder, TransactionOuterClass, TransactionVersion}
import com.daml.lf.value.ValueOuterClass

import scala.annotation.tailrec
import scala.jdk.CollectionConverters._
import scala.util.Try
import scalaz._
import Scalaz._
import com.daml.lf.value.ValueCoder.DecodeError

object TransactionTraversal {

  // Helper to traverse the transaction, top-down, while keeping track of the
  // witnessing parties of each node.
  def traverseTransactionWithWitnesses(rawTx: RawTransaction)(
      f: (RawTransaction.NodeId, RawTransaction.Node, Set[Ref.Party]) => Unit
  ): Either[ConversionError, Unit] =
    for {
      parsedTransaction <- parseTransaction(rawTx)
      (txVersion, nodes, initialToVisit) = parsedTransaction
      _ <- traverseWitnesses(f, txVersion, nodes, initialToVisit)
    } yield ()

  // Helper to traverse the transaction, top-down, while keeping track of the
  // witnessing parties of each package.
  def extractPerPackageWitnesses(
      rawTx: RawTransaction
  ): Either[ConversionError, Map[String, Set[Ref.Party]]] =
    for {
      parsedTransaction <- parseTransaction(rawTx)
      (txVersion, nodes, initialToVisit) = parsedTransaction
      result <- traverseWitnessesWithPackages(txVersion, nodes, initialToVisit)
    } yield result

  private def parseTransaction(rawTx: RawTransaction): Either[
    ConversionError,
    (TransactionVersion, Map[String, Node], FrontStack[(RawTransaction.NodeId, Set[Party])]),
  ] = {
    for {
      tx <- Try(TransactionOuterClass.Transaction.parseFrom(rawTx.byteString)).toEither.left.map(
        throwable => ConversionError.ParseError(throwable.getMessage)
      )
      txVersion <- TransactionVersion.fromString(tx.getVersion).left.map(ConversionError.ParseError)
      nodes = tx.getNodesList.iterator.asScala.map(node => node.getNodeId -> node).toMap
      initialToVisit = tx.getRootsList.asScala.view
        .map(RawTransaction.NodeId(_) -> Set.empty[Ref.Party])
        .to(FrontStack)
    } yield { (txVersion, nodes, initialToVisit) }
  }

  @tailrec
  private def traverseWitnessesWithPackages(
      txVersion: TransactionVersion,
      nodes: Map[String, Node],
      toVisit: FrontStack[(RawTransaction.NodeId, Set[Ref.Party])],
      packagesToParties: Map[String, Set[Ref.Party]] = Map.empty,
  ): Either[ConversionError, Map[String, Set[Ref.Party]]] = {
    def addPackage(templateId: ValueOuterClass.Identifier, witnesses: Set[Party]) = {
      val currentNodePackagesWithWitnesses = Map(
        templateId.getPackageId -> witnesses
      )
      packagesToParties |+| currentNodePackagesWithWitnesses
    }
    toVisit match {
      case FrontStack() => Right(packagesToParties)
      case FrontStackCons((nodeId, parentWitnesses), toVisit) =>
        val node = nodes(nodeId.value)
        lazy val witnesses = informeesOfNode(txVersion, node).map(_ union parentWitnesses)
        node.getNodeTypeCase match {
          case Node.NodeTypeCase.EXERCISE =>
            witnesses match {
              case Left(value) => Left(value)
              case Right(witnesses) =>
                val exercise = node.getExercise
                // Recurse into children (if any).
                val next = exercise.getChildrenList.asScala.view
                  .map(RawTransaction.NodeId(_) -> witnesses)
                  .to(ImmArray)
                val currentNodePackagesWithWitnesses =
                  Map(exercise.getTemplateId.getPackageId -> witnesses) ++
                    Option
                      .when(exercise.hasInterfaceId)(
                        exercise.getInterfaceId.getPackageId -> witnesses
                      )
                      .toList
                      .toMap
                traverseWitnessesWithPackages(
                  txVersion,
                  nodes,
                  next ++: toVisit,
                  packagesToParties |+| currentNodePackagesWithWitnesses,
                )
            }
          case Node.NodeTypeCase.FETCH =>
            val templateId = node.getFetch.getTemplateId
            witnesses match {
              case Left(error) => Left(error)
              case Right(witnesses) =>
                traverseWitnessesWithPackages(
                  txVersion,
                  nodes,
                  toVisit,
                  addPackage(templateId, witnesses),
                )
            }
          case Node.NodeTypeCase.CREATE =>
            val templateId = node.getCreate.getTemplateId
            witnesses match {
              case Left(error) => Left(error)
              case Right(witnesses) =>
                traverseWitnessesWithPackages(
                  txVersion,
                  nodes,
                  toVisit,
                  addPackage(templateId, witnesses),
                )
            }
          case Node.NodeTypeCase.LOOKUP_BY_KEY =>
            val templateId = node.getLookupByKey.getTemplateId
            witnesses match {
              case Left(error) => Left(error)
              case Right(witnesses) =>
                traverseWitnessesWithPackages(
                  txVersion,
                  nodes,
                  toVisit,
                  addPackage(templateId, witnesses),
                )
            }
          case Node.NodeTypeCase.ROLLBACK =>
            // Rollback nodes have only the parent witnesses
            val rollback = node.getRollback
            // Recurse into children (if any).
            val next = rollback.getChildrenList.asScala.view
              .map(RawTransaction.NodeId(_) -> parentWitnesses)
              .to(ImmArray)
            traverseWitnessesWithPackages(
              txVersion,
              nodes,
              next ++: toVisit,
              packagesToParties,
            )
          case Node.NodeTypeCase.NODETYPE_NOT_SET =>
            Left(ConversionError.DecodeError(DecodeError("NodeType not set.")))
        }
    }
  }

  @tailrec
  private def traverseWitnesses(
      f: (RawTransaction.NodeId, RawTransaction.Node, Set[Ref.Party]) => Unit,
      txVersion: TransactionVersion,
      nodes: Map[String, Node],
      toVisit: FrontStack[(RawTransaction.NodeId, Set[Ref.Party])],
  ): Either[ConversionError, Unit] = {
    toVisit match {
      case FrontStack() => Right(())
      case FrontStackCons((nodeId, parentWitnesses), toVisit) =>
        val node = nodes(nodeId.value)
        informeesOfNode(txVersion, node) match {
          case Left(error) => Left(error)
          case Right(nodeWitnesses) =>
            val witnesses = parentWitnesses union nodeWitnesses
            // Here node.toByteString is safe.
            // Indeed node is a submessage of the transaction `rawTx` we got serialized
            // as input of `traverseTransactionWithWitnesses` and successfully decoded, i.e.
            // `rawTx` requires less than 2GB to be serialized, so does <node`.
            // See com.daml.SafeProto for more details about issues with the toByteString method.
            f(nodeId, RawTransaction.Node(node.toByteString), witnesses)
            // Recurse into children (if any).
            node.getNodeTypeCase match {
              case Node.NodeTypeCase.EXERCISE =>
                val next = node.getExercise.getChildrenList.asScala.view
                  .map(RawTransaction.NodeId(_) -> witnesses)
                  .to(ImmArray)
                traverseWitnesses(f, txVersion, nodes, next ++: toVisit)
              case _ =>
                traverseWitnesses(f, txVersion, nodes, toVisit)
            }
        }
    }
  }

  private[this] def informeesOfNode(
      txVersion: TransactionVersion,
      node: TransactionOuterClass.Node,
  ) =
    TransactionCoder
      .protoActionNodeInfo(txVersion, node)
      .map(_.informeesOfNode)
      .leftMap(ConversionError.DecodeError)

}
