// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.daml.lf.codegen

import com.digitalasset.daml.lf.data.{BackStack, ImmArray, Ref}
import com.digitalasset.daml.lf.typesig.PackageSignature
import PackageSignature.TypeDecl
import com.typesafe.scalalogging.StrictLogging

import scala.annotation.tailrec
import scala.collection.mutable
import scala.concurrent.{ExecutionContext, Future}

private[codegen] sealed trait Node

private[codegen] final case class Module(modules: Map[String, Module], types: Map[String, Type])
    extends Node

private[codegen] final case class Type(typ: Option[TypeDecl], types: Map[String, Type]) extends Node

private[codegen] final case class InterfaceTree(
    modules: Map[String, Module],
    interface: PackageSignature,
    auxiliarySignatures: NodeWithContext.AuxiliarySignatures,
) {

  def process(f: NodeWithContext => Future[Unit])(implicit ec: ExecutionContext): Future[Unit] = {
    bfs(Future.unit) { case (a, nodeWithContext) =>
      a.zipWith(f(nodeWithContext))((_, _) => ())(ec)
    }
  }

  def bfs[A](z: A)(f: (A, NodeWithContext) => A): A = {
    val nodeWithLineages = mutable.Queue.empty[NodeWithContext]
    for ((name, module) <- modules) {
      nodeWithLineages += ModuleWithContext(
        interface,
        auxiliarySignatures,
        BackStack.empty,
        name,
        module,
      )
    }
    @tailrec
    def go(result: A): A = {
      if (nodeWithLineages.isEmpty) {
        result
      } else {
        val nodeWithContext = nodeWithLineages.dequeue()
        nodeWithLineages ++= nodeWithContext.childrenLineages
        go(f(result, nodeWithContext))
      }
    }
    go(z)
  }
}

private[codegen] sealed trait NodeWithContext extends Product with Serializable {
  def interface: PackageSignature
  def lineage: ImmArray[(String, Node)]
  def modulesLineage: BackStack[(String, Module)]
  def name: String
  def childrenLineages: Iterable[NodeWithContext]
  def typesLineages: Iterable[TypeWithContext]

  final def packageId: Ref.PackageId = interface.packageId
}

private[codegen] object NodeWithContext {

  /** Signatures from other packages involved in the codegen run. */
  type AuxiliarySignatures = Map[Ref.PackageId, PackageSignature]
}

private[codegen] final case class ModuleWithContext(
    interface: PackageSignature,
    auxiliarySignatures: NodeWithContext.AuxiliarySignatures,
    modulesLineage: BackStack[(String, Module)],
    name: String,
    module: Module,
) extends NodeWithContext {
  override def childrenLineages: Iterable[NodeWithContext] = {
    val newModulesLineage = modulesLineage :+ (name -> module)
    module.modules.map { case (childName, childModule) =>
      ModuleWithContext(
        interface,
        auxiliarySignatures,
        newModulesLineage,
        childName,
        childModule,
      )
    } ++ typesLineages
  }

  override def typesLineages: Iterable[TypeWithContext] = module.types.map {
    case (childName, childType) =>
      TypeWithContext(
        interface,
        auxiliarySignatures,
        modulesLineage :+ (name -> module),
        BackStack.empty,
        childName,
        childType,
      )
  }
  override def lineage: ImmArray[(String, Node)] = (modulesLineage :+ (name -> module)).toImmArray
}

private[codegen] final case class TypeWithContext(
    interface: PackageSignature,
    auxiliarySignatures: NodeWithContext.AuxiliarySignatures,
    modulesLineage: BackStack[(String, Module)],
    typesLineage: BackStack[(String, Type)],
    name: String,
    `type`: Type,
) extends NodeWithContext {
  override def childrenLineages: Iterable[NodeWithContext] = typesLineages
  override def typesLineages: Iterable[TypeWithContext] = `type`.types.map {
    case (childName, childType) =>
      TypeWithContext(
        interface,
        auxiliarySignatures,
        modulesLineage,
        typesLineage :+ (name -> `type`),
        childName,
        childType,
      )
  }
  override def lineage: ImmArray[(String, Node)] =
    modulesLineage.toImmArray.slowAppend[(String, Node)](typesLineage.toImmArray)

  /* The name of this in the module */
  def fullName: Ref.DottedName =
    Ref.DottedName.assertFromSegments(typesLineage.map(_._1).:+(name).toImmArray.toSeq)

  def module: Ref.ModuleName =
    Ref.ModuleName.assertFromSegments(modulesLineage.map(_._1).toImmArray.toSeq)

  def qualifiedName: Ref.QualifiedName = Ref.QualifiedName(module, fullName)

  def identifier: Ref.Identifier = Ref.Identifier(packageId, qualifiedName)
}

private[codegen] object InterfaceTree extends StrictLogging {

  def fromInterface(
      interface: PackageSignature,
      auxSigs: NodeWithContext.AuxiliarySignatures,
  ): InterfaceTree = {
    val builder = new InterfaceTreeBuilder(new mutable.HashMap())
    interface.typeDecls.foreach { case (identifier, typ) =>
      builder.insert(identifier, typ)
    }
    builder.build(interface, auxSigs)
  }

  private sealed trait NodeBuilder

  private final class ModuleBuilder(
      modules: mutable.HashMap[String, ModuleBuilder],
      types: mutable.HashMap[String, TypeBuilder],
  ) extends NodeBuilder {
    def build(): Module =
      Module(modules.view.mapValues(_.build()).toMap, types.view.mapValues(_.build()).toMap)

    @tailrec
    def insert(module: ImmArray[String], name: ImmArray[String], `type`: TypeDecl): Unit = {
      if (module.isEmpty) {
        // at this point name cannot be empty
        assert(name.length > 0)
        if (name.length == 1) {
          types.getOrElseUpdate(name.head, TypeBuilder.fromType(`type`)).setTypeOrThrow(`type`)
        } else {
          val tail = name.tail
          types.getOrElseUpdate(name.head, TypeBuilder.empty).insert(tail.head, tail.tail, `type`)
        }
      } else {
        modules.getOrElseUpdate(module.head, ModuleBuilder.empty).insert(module.tail, name, `type`)
      }
    }
  }

  private object ModuleBuilder {
    def empty = new ModuleBuilder(mutable.HashMap.empty, mutable.HashMap.empty)
  }

  private final class TypeBuilder(
      var typ: Option[TypeDecl],
      children: mutable.HashMap[String, TypeBuilder],
  ) extends NodeBuilder {
    def build(): Type = {
      typ match {
        // we allow TypeBuilder nodes with no TypeDecl if they have children nodes
        case None if children.isEmpty =>
          throw new IllegalStateException(s"Found a Type node without a type at build() time")
        case definedTypeOpt => Type(definedTypeOpt, children.view.mapValues(_.build()).toMap)
      }
    }
    @tailrec
    def insert(name: String, names: ImmArray[String], `type`: TypeDecl): Unit = {
      if (names.isEmpty) {
        children
          .getOrElseUpdate(name, new TypeBuilder(Some(`type`), mutable.HashMap.empty))
          .setTypeOrThrow(`type`)
      } else {
        children
          .getOrElseUpdate(name, new TypeBuilder(None, mutable.HashMap.empty))
          .insert(names.head, names.tail, `type`)
      }
    }

    def setTypeOrThrow(typ: TypeDecl): Unit = {
      this.typ match {
        case Some(otherTyp) if typ != otherTyp =>
          throw new IllegalStateException(
            s"Found a Type node with two different types, $typ and $otherTyp. This should not happen"
          )
        case _ => this.typ = Some(typ)
      }
    }
  }

  private object TypeBuilder {
    def empty = new TypeBuilder(None, mutable.HashMap.empty)
    def fromType(`type`: TypeDecl) = new TypeBuilder(Some(`type`), mutable.HashMap.empty)
  }

  private final class InterfaceTreeBuilder(
      children: mutable.HashMap[String, ModuleBuilder]
  ) {

    def build(
        interface: PackageSignature,
        auxSigs: NodeWithContext.AuxiliarySignatures,
    ): InterfaceTree =
      InterfaceTree(children.view.mapValues(_.build()).toMap, interface, auxSigs)

    def insert(qualifiedName: Ref.QualifiedName, `type`: TypeDecl): Unit = {
      children
        .getOrElseUpdate(qualifiedName.module.segments.head, ModuleBuilder.empty)
        .insert(qualifiedName.module.segments.tail, qualifiedName.name.segments, `type`)
    }
  }
}
