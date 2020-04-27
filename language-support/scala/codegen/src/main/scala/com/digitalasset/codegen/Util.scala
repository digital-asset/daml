// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.codegen

import com.daml.codegen.dependencygraph.{OrderedDependencies, TypeDeclOrTemplateWrapper}
import com.daml.lf.iface.{Type => IType, _}
import com.daml.lf.data.Ref
import com.daml.lf.data.ImmArray.ImmArraySeq

import java.io.File

import scala.reflect.runtime.universe._
import scala.collection.generic.CanBuildFrom
import scala.collection.TraversableLike
import scalaz.{Tree => _, _}
import scalaz.std.list._

/**
  *  In order to avoid endlessly passing around "packageName" and "iface" to
  *  utility functions we initialise a class with these values and allow all the
  *  methods to have access to them.
  */
@SuppressWarnings(Array("org.wartremover.warts.Any"))
abstract class Util(val packageName: String, val outputDir: File) { self =>

  import Util._

  type Interface

  type TemplateInterface

  type RefType = TypeConNameOrPrimType

  val iface: Interface

  val packageNameElems: Array[String] = packageName.split('.')

  private[codegen] def orderedDependencies(library: Interface)
    : OrderedDependencies[Ref.Identifier, TypeDeclOrTemplateWrapper[TemplateInterface]]

  def templateAndTypeFiles(wp: WriteParams[TemplateInterface]): TraversableOnce[FilePlan]

  /**
    * Convert the metadataAlias into a [[DamlScalaName]] based on the `codeGenDeclKind`.
    *
    * {{{
    *   > mkDamlScalaName(Contract, QualifiedName.assertFromString("foo.bar:Test"))
    *   DamlScalaName("foo.bar", "TestContract")
    * }}}
    */
  def mkDamlScalaName(
      codeGenDeclKind: CodeGenDeclKind,
      metadataAlias: Ref.QualifiedName): DamlScalaName

  /**
    * A Scala class/object package suffix and name.
    *
    * @param packageSuffixParts the package suffix of the class. This will be appended to
    *                           the [[packageNameElems]] to create the package for this
    *                           Scala class/object
    * @param name the name of the class/object
    */
  case class DamlScalaName(packageSuffixParts: Array[String], name: String) {

    /** Components of the package for this class/object */
    def packageNameParts = packageNameElems ++ packageSuffixParts

    /** The package for this class/object */
    def packageName = packageNameParts.mkString(".")

    /** Components of the fully qualified name for this class/object */
    def qualifiedNameParts = packageNameParts :+ name

    /** The fully qualified name of this class/object */
    def qualifiedName = qualifiedNameParts.mkString(".")

    private[this] def qualifierTree =
      packageNameParts.foldLeft(q"_root_": Tree)((l, n) => q"$l.${TermName(n)}")

    def qualifiedTypeName = tq"$qualifierTree.${TypeName(name)}"
    def qualifiedTermName = q"$qualifierTree.${TermName(name)}"

    /** The file name where this class/object must be stored */
    def toFileName: File = {
      val relPathParts = packageNameParts.map(_.toLowerCase) :+ s"${name}.scala"
      val relPath = relPathParts.mkString(File.separator)
      new File(outputDir, relPath)
    }

    def toRefTreeWithInnerTypes(innerTypes: Array[String]): RefTree = {
      qualifiedNameToRefTree(qualifiedNameParts ++ innerTypes)
    }

    def toRefTree: RefTree = toRefTreeWithInnerTypes(Array())

    override def toString: String =
      "DamlScalaName(" + packageSuffixParts.mkString("[", ", ", "]") + ", " + name + ")"

    override def equals(ojb: Any): Boolean = ojb match {
      case that: DamlScalaName =>
        that.packageSuffixParts.deep == this.packageSuffixParts.deep && that.name == this.name
      case _ =>
        false
    }

    override def hashCode(): Int = (this.packageSuffixParts.deep, this.name).hashCode()
  }

  /**
    *  This method is responsible for generating Scala reflection API "tree"s.
    *  It maps from Core package interface types to:
    *  a) the wrapper types defined in the typed-ledger-api project
    *  b) the classes that will be generated for the user defined types defined in
    *     the "typeDecls" field of the Package.PackageInterface value.
    */
  def genTypeToScalaType(genType: IType): Tree

  /**
    * Generate the function that converts an `ArgumentValue` into the given `Type`
    *
    * @param genType the output type of the generated function
    * @return the function that converts an `ArgumentValue` into the given `Type`
    */
  def genArgumentValueToGenType(genType: IType): Tree

  def paramRefAndGenTypeToArgumentValue(paramRef: Tree, genType: IType): Tree

  def templateCount(interface: Interface): Int
}

object Util {
  // error message or optional message before writing, file target, and
  // sequence of trees to write as Scala source code
  type FilePlan = String \/ (Option[String], File, Iterable[Tree])

  final case class WriteParams[+TmplI](
      templateIds: Map[Ref.Identifier, TmplI],
      definitions: List[lf.ScopedDataType.FWT])

  val reservedNames: Set[String] =
    Set("id", "template", "namedArguments", "archive")

  def toNotReservedName(name: String): String =
    "userDefined" + name.capitalize

  //----------------------------------------------

  sealed trait CodeGenDeclKind
  case object Template extends CodeGenDeclKind
  case object Contract extends CodeGenDeclKind
  case object UserDefinedType extends CodeGenDeclKind
  case object EventDecoder extends CodeGenDeclKind

  val autoGenerationHeader: String =
    """|/*
       | * THIS FILE WAS AUTOGENERATED BY THE DIGITAL ASSET DAML SCALA CODE GENERATOR
       | * DO NOT EDIT BY HAND!
       | */""".stripMargin

  def toIdent(s: String): Ident = Ident(TermName(s))

  def toTypeDef(s: String): TypeDef = q"type ${TypeName(s.capitalize)}"

  def qualifiedNameToDirsAndName(qualifiedName: Ref.QualifiedName): (Array[String], String) = {
    val s = qualifiedName.module.segments.toSeq ++ qualifiedName.name.segments.toSeq
    (s.init.toArray, s.last)
  }

  def qualifiedNameToRefTree(ss: Array[String]): RefTree = {
    ss match {
      case Array() =>
        throw new RuntimeException("""|packageNameToRefTree: This function should never be called
           |with an empty list""".stripMargin)
      case Array(name) => toIdent(name)
      case Array(firstQual, rest @ _*) => {
        val base: RefTree = toIdent(firstQual)
        rest.foldLeft(base)((refTree: RefTree, nm: String) => {
          RefTree(refTree, TermName(nm))
        })
      }
    }
  }

  private[codegen] def genTypeTopLevelDeclNames(genType: IType): List[Ref.Identifier] =
    genType foldMapConsPrims {
      case TypeConName(nm) => List(nm)
      case _: PrimType => Nil
    }

  def packageNameToRefTree(packageName: String): RefTree = {
    val ss = packageName.split('.')
    qualifiedNameToRefTree(ss)
  }

  /** Does the type "simply delegate" (i.e. just pass along `typeVars`
    * verbatim) to another type constructor?  If so, yield that type
    * constructor as a string.
    */
  def simplyDelegates(typeVars: ImmArraySeq[Ref.Name]): IType => Option[Ref.Identifier] = {
    val ptv = typeVars.map(TypeVar(_): IType)

    {
      case TypeCon(tc, `ptv`) => Some(tc.identifier)
      case _ => None
    }
  }

  /** cf. scalaz.MonadPlus#separate */
  private[codegen] def partitionEithers[A, B, Coll, AS, BS](
      abs: TraversableLike[Either[A, B], Coll])(
      implicit AS: CanBuildFrom[Coll, A, AS],
      BS: CanBuildFrom[Coll, B, BS]): (AS, BS) =
    (abs collect { case Left(a) => a }, abs collect { case Right(a) => a })
}
