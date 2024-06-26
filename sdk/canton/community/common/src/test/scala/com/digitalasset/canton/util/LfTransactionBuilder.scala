// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.util

import cats.Monad
import cats.data.StateT
import com.daml.lf.data.Ref.QualifiedName
import com.daml.lf.data.{ImmArray, Ref}
import com.daml.lf.language.LanguageVersion
import com.daml.lf.value.Value
import com.digitalasset.canton.protocol.*
import com.digitalasset.canton.{LfInterfaceId, LfPackageId}

import scala.collection.immutable.HashMap

object LfTransactionBuilder {

  type NodeIdState = Int

  type LfAction = (LfNodeId, Map[LfNodeId, LfActionNode])

  // Helper methods for Daml-LF types
  val defaultLanguageVersion: LanguageVersion = LanguageVersion.default
  val defaultTransactionVersion: LfTransactionVersion = LfTransactionVersion.minVersion

  val defaultPackageId: LfPackageId = LfPackageId.assertFromString("pkg")
  val defaultTemplateId: Ref.Identifier =
    Ref.Identifier(defaultPackageId, QualifiedName.assertFromString("module:template"))
  val defaultPackageName: Ref.PackageName = Ref.PackageName.assertFromString("pkgName")
  val defaultInterfaceId: LfInterfaceId = defaultTemplateId

  val defaultGlobalKey: LfGlobalKey = LfGlobalKey.assertBuild(
    defaultTemplateId,
    Value.ValueUnit,
    defaultPackageName,
  )

  def allocateNodeId[M[_]](implicit monadInstance: Monad[M]): StateT[M, NodeIdState, LfNodeId] =
    for {
      nodeId <- StateT.get[M, NodeIdState]
      _ <- StateT.set[M, NodeIdState](nodeId + 1)
    } yield LfNodeId(nodeId)

  def exerciseFromLf[M[_]](lfExercise: LfNodeExercises, children: List[LfAction])(implicit
      monadInstance: Monad[M]
  ): StateT[M, NodeIdState, LfAction] =
    for {
      nodeId <- allocateNodeId[M]
      childrenIds = children.map(_._1)
      childrenMap = children.map(_._2).fold(Map.empty[LfNodeId, LfActionNode])(_ ++ _)
      nodeWithChildren = lfExercise.copy(children = childrenIds.to(ImmArray))
    } yield (nodeId, childrenMap ++ Map(nodeId -> nodeWithChildren))

  def createFromLf[M[_]](
      lfCreate: LfNodeCreate
  )(implicit monadInstance: Monad[M]): StateT[M, NodeIdState, LfAction] =
    for {
      nodeId <- allocateNodeId[M]
    } yield (nodeId, Map(nodeId -> lfCreate))

  def fetchFromLf[M[_]](
      lfFetch: LfNodeFetch
  )(implicit monadInstance: Monad[M]): StateT[M, NodeIdState, LfAction] =
    for {
      nodeId <- allocateNodeId[M]
    } yield (nodeId, Map(nodeId -> lfFetch))

  def initialState: NodeIdState = 0

  def usedPackages(action: LfAction): Set[LfPackageId] = action match {
    case (_, nodeMap) =>
      val nodeSet = nodeMap.values

      nodeSet.map {
        case c: LfNodeCreate => c.coinst.template.packageId
        case e: LfNodeExercises => e.templateId.packageId
        case f: LfNodeFetch => f.templateId.packageId
        case l: LfNodeLookupByKey => l.templateId.packageId
      }.toSet
  }

  /** Turn a state containing a list of actions into a transaction.
    *
    * @param state The (monadic) list of actions
    */
  def toTransaction[M[_]](
      state: StateT[M, NodeIdState, List[LfAction]]
  )(implicit monadInstance: Monad[M]): M[LfTransaction] =
    state
      .map(
        _.foldRight((List.empty[LfNodeId], Map.empty[LfNodeId, LfNode], Set.empty[LfPackageId])) {
          case (act @ (actionRoot, actionMap), (roots, nodeMap, pkgs)) =>
            (actionRoot +: roots, nodeMap ++ actionMap, pkgs ++ usedPackages(act))
        }
      )
      .map { case (rootNodes, nodeMap, _actuallyUsedPkgs) =>
        LfTransaction(nodes = HashMap(nodeMap.toSeq*), roots = rootNodes.to(ImmArray))
      }
      .runA(initialState)
}
