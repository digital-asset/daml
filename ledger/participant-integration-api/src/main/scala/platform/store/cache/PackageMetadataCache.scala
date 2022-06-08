// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.store.cache

import com.daml.daml_lf_dev.DamlLf
import com.daml.ledger.offset.Offset
import com.daml.lf.archive.Decode
import com.daml.lf.data.Ref
import com.daml.lf.data.Ref.DottedName

import java.util.concurrent.atomic.AtomicReference
import scala.collection.mutable

/** For each operation, the following holds:
  * - The result MUST include all packages uploaded at or before
  *   the current ledger end
  * - The result MAY include packages uploaded after the current
  *   ledger end
  */
trait PackageMetadataCache {

  /** List of templates implementing an interface */
  def getInterfaceImplementations(id: Ref.Identifier): Set[Ref.Identifier]

  /** Return the offset at which the given interface was defined,
    * i.e., at which the corresponding package was uploaded.
    * Returns None if the interface is unknown.
    */
  def interfaceAddedAt(id: Ref.Identifier): Option[Offset]

  /** Same as interfaceAddedAt, but for templates */
  def templateAddedAt(id: Ref.Identifier): Option[Offset]
}

// TODO DPP-1068: use a proper cache instead of a global variable
object SingletonPackageMetadataCache extends PackageMetadataCache {

  case class State(
      definedAt: Map[Ref.Identifier, Offset],
      implementations: Map[Ref.Identifier, Set[Ref.Identifier]],
  ) {
    def mergeWith(other: State): State = {
      val resultImplementations = mutable.Map.empty[Ref.Identifier, Set[Ref.Identifier]]
      resultImplementations.addAll(implementations.toSeq)
      other.implementations.foreach { case (id, otherTemplateIds) =>
        resultImplementations.updateWith(id) {
          case None => Some(otherTemplateIds)
          case Some(thisTemplateIds) => Some(thisTemplateIds ++ otherTemplateIds)
        }
      }

      // Template/interface IDs can not be defined in two different packages, there will be no collisions
      val resultDefinedAt = definedAt ++ other.definedAt

      State(
        definedAt = resultDefinedAt,
        implementations = resultImplementations.toMap,
      )
    }
  }
  object State {
    def empty: State = State(Map.empty, Map.empty)
  }
  private val state = new AtomicReference[State](State.empty)

  def add(archives: List[DamlLf.Archive], offset: Offset): Unit = {
    val newState = archivesToState(archives, offset)
    state.updateAndGet(_.mergeWith(newState))
    ()
  }

  private def archivesToState(archives: List[DamlLf.Archive], offset: Offset): State = {
    val newDefinitions = mutable.Map.empty[Ref.Identifier, Offset]
    val newImplementations = mutable.Map.empty[Ref.Identifier, Set[Ref.Identifier]]

    archives.foreach(archive => {
      val (packageId, ast) = Decode
        .decodeArchive(archive, true)
        .getOrElse(throw new RuntimeException("error handling not implemented"))
      ast.modules.foreach { case (moduleName, module) =>
        def identifier(name: DottedName) =
          Ref.Identifier(packageId, Ref.QualifiedName(moduleName, name))

        module.templates.keys.foreach(tid => newDefinitions.addOne(identifier(tid) -> offset))
        module.interfaces.keys.foreach(iid => newDefinitions.addOne(identifier(iid) -> offset))
        module.templates.foreach { case (tid, t) =>
          t.implements.values.foreach(i =>
            newImplementations.updateWith(i.interfaceId) {
              case None => Some(Set(identifier(tid)))
              case Some(previous) => Some(previous + identifier(tid))
            }
          )
        }
      }
    })

    State(
      definedAt = newDefinitions.toMap,
      implementations = newImplementations.toMap,
    )
  }

  def getInterfaceImplementations(id: Ref.Identifier): Set[Ref.Identifier] =
    state.get.implementations(id)

  def interfaceAddedAt(id: Ref.Identifier): Option[Offset] = state.get.definedAt.get(id)

  def templateAddedAt(id: Ref.Identifier): Option[Offset] = state.get.definedAt.get(id)

}
