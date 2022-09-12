// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf.kv.transactions

import com.daml.lf.data.Ref.Party
import com.daml.lf.data.{FrontStack, FrontStackCons, ImmArray, Ref}
import com.daml.lf.kv.ConversionError
import com.daml.lf.transaction.TransactionOuterClass.Node
import com.daml.lf.transaction.{TransactionCoder, TransactionOuterClass, TransactionVersion}
import com.daml.lf.value.{ValueCoder, ValueOuterClass}

import scala.annotation.tailrec
import scala.jdk.CollectionConverters._
import scala.util.Try
import scalaz._
import Scalaz._

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
    toVisit match {
      case FrontStack() => Right(packagesToParties)
      case FrontStackCons((nodeId, parentWitnesses), toVisit) =>
        val node = nodes(nodeId.value)
        informeesOfNode(txVersion, node) match {
          case Left(error) => Left(ConversionError.DecodeError(error))
          case Right(nodeWitnesses) =>
            val witnesses = parentWitnesses union nodeWitnesses
            def addPackage(templateId: ValueOuterClass.Identifier) = {
              val currentNodePackagesWithWitnesses = Map(
                templateId.getPackageId -> witnesses
              )
              packagesToParties |+| currentNodePackagesWithWitnesses
            }
            node.getNodeTypeCase match {
              case Node.NodeTypeCase.EXERCISE =>
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
              case Node.NodeTypeCase.FETCH =>
                traverseWitnessesWithPackages(
                  txVersion,
                  nodes,
                  toVisit,
                  addPackage(node.getFetch.getTemplateId),
                )
              case Node.NodeTypeCase.CREATE =>
                traverseWitnessesWithPackages(
                  txVersion,
                  nodes,
                  toVisit,
                  addPackage(node.getCreate.getTemplateId),
                )
              case Node.NodeTypeCase.LOOKUP_BY_KEY =>
                traverseWitnessesWithPackages(
                  txVersion,
                  nodes,
                  toVisit,
                  addPackage(node.getLookupByKey.getTemplateId),
                )
              case Node.NodeTypeCase.NODETYPE_NOT_SET | Node.NodeTypeCase.ROLLBACK =>
                traverseWitnessesWithPackages(txVersion, nodes, toVisit, packagesToParties)
            }
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
          case Left(error) => Left(ConversionError.DecodeError(error))
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
  ): Either[ValueCoder.DecodeError, Set[Ref.Party]] =
    TransactionCoder
      .protoActionNodeInfo(txVersion, node)
      .map(_.informeesOfNode)

}
