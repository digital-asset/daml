// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.store.cache

import com.daml.daml_lf_dev.DamlLf
import com.daml.ledger.offset.Offset
import com.daml.lf.archive.Decode
import com.daml.lf.data.Ref
import com.daml.lf.data.Ref.DottedName
import com.daml.lf.language.Ast.Package

import java.util.concurrent.atomic.AtomicReference
import scala.collection.mutable

/** For each operation, the following holds:
  *
  * - The result MUST include all packages uploaded at or before
  *   the current ledger end
  *
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

  // TODO DPP-1068: This synchronous call requires the cache to store all packages in a decoded form in memory.
  //   - Verify whether we need to deduplicate the storage between this and the Engine.compiledPackages
  //   - Verify whether this doesn't increase memory requirements too much
  /** Returns the decoded package, if it exists */
  def getPackage(id: Ref.PackageId): Option[Package]
}

// TODO DPP-1068: use a proper cache instead of a global variable
object SingletonPackageMetadataCache extends PackageMetadataCache {

  case class State(
      definedAt: Map[Ref.Identifier, Offset],
      implementations: Map[Ref.Identifier, Set[Ref.Identifier]],
      decodedPackages: Map[Ref.PackageId, Package],
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

      // Templates/interfaces/packages are immutable, there will be no conflicting map values
      val resultDefinedAt = definedAt ++ other.definedAt
      val resultDecodedPackages = decodedPackages ++ other.decodedPackages

      State(
        definedAt = resultDefinedAt,
        implementations = resultImplementations.toMap,
        decodedPackages = resultDecodedPackages,
      )
    }
  }
  object State {
    def empty: State = State(Map.empty, Map.empty, Map.empty)
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
    val newPackages = mutable.Map.empty[Ref.PackageId, Package]

    archives.foreach(archive => {
      Decode
        .decodeArchive(archive, true)
        .fold(
          error => {
            // Note: some unit tests use dummy packages, and we want to gracefully handle broken packages
            // coming from the ReadService.
            // TODO DPP-1068: Proper error logging
            Console.println(s"Archive could not be decoded: $error")
          },
          result => {
            val (packageId, ast) = result
            newPackages.addOne(packageId -> ast)

            ast.modules.foreach { case (moduleName, module) =>
              def identifier(name: DottedName) =
                Ref.Identifier(packageId, Ref.QualifiedName(moduleName, name))

              module.templates.keys.foreach(tid => newDefinitions.addOne(identifier(tid) -> offset))
              module.interfaces.keys
                .foreach(iid => newDefinitions.addOne(identifier(iid) -> offset))
              module.templates.foreach { case (tid, t) =>
                t.implements.values.foreach(i =>
                  newImplementations.updateWith(i.interfaceId) {
                    case None => Some(Set(identifier(tid)))
                    case Some(previous) => Some(previous + identifier(tid))
                  }
                )
              }
            }
          },
        )
    })

    State(
      definedAt = newDefinitions.toMap,
      implementations = newImplementations.toMap,
      decodedPackages = newPackages.toMap,
    )
  }

  override def getPackage(id: Ref.PackageId): Option[Package] =
    state.get.decodedPackages.get(id)

  override def getInterfaceImplementations(id: Ref.Identifier): Set[Ref.Identifier] =
    state.get.implementations(id)

  override def interfaceAddedAt(id: Ref.Identifier): Option[Offset] =
    state.get.definedAt.get(id)

  override def templateAddedAt(id: Ref.Identifier): Option[Offset] =
    state.get.definedAt.get(id)

}
