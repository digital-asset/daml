// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.daml.lf

import com.digitalasset.daml.lf.data.Ref
import com.digitalasset.daml.lf.data.Ref.PackageId
import com.digitalasset.daml.lf.language.Ast.{Package, PackageSignature}
import com.digitalasset.daml.lf.language.{PackageInterface, Util}
import com.digitalasset.daml.lf.speedy.{Compiler, SDefinition, SExpr}
import com.digitalasset.daml.lf.stablepackages.StablePackagesV2
import com.digitalasset.daml.lf.testing.parser.ParserParameters

/** Trait to abstract over a collection holding onto Daml-LF package definitions + the
  * compiled speedy expressions.
  */
private[lf] abstract class CompiledPackages(
    val compilerConfig: Compiler.Config
) {
  def signatures: collection.Map[PackageId, PackageSignature]
  def getDefinition(ref: SExpr.SDefinitionRef): Option[SDefinition]
  final def compiler: Compiler = new Compiler(pkgInterface, compilerConfig)
  final def pkgInterface = new PackageInterface(signatures)
  final def contains(pkgId: PackageId): Boolean = signatures.contains(pkgId)
}

@scala.annotation.nowarn("msg=match may not be exhaustive")
private[lf] object CompiledPackages {

  val (
    preloadedPackageSignatures: Map[PackageId, PackageSignature],
    preloadedDefs: Map[SExpr.SDefinitionRef, SDefinition],
  ) = {
    val preloadedPackages = StablePackagesV2.packagesMap
      .updated(internalPkgId, internalPkg)
    val signatures = Util.toSignatures(preloadedPackages)

    def defs = data.assertRight(
      Compiler.compilePackages(
        new PackageInterface(signatures),
        preloadedPackages,
        Compiler.Config.Dev,
      )
    )

    (signatures, defs)
  }

  val preloadedDeps: Map[PackageId, Set[PackageId]] = {
    val directDeps = preloadedPackageSignatures.transform { case (_, pkg) => pkg.directDeps }
    language.Graphs.transitiveClosure(directDeps)
  }

  lazy val internalPkgId = Ref.PackageId.assertFromString("-internal-");
  lazy val internalPkg = {
    import com.digitalasset.daml.lf.testing.parser.Implicits.SyntaxHelper
    implicit def parserParameters: ParserParameters[this.type] =
      ParserParameters(
        defaultPackageId = internalPkgId,
        languageVersion = language.LanguageVersion.v2_2,
      )
    p"""  metadata ( 'internal' : '1.0.0' )

module LF.Map {

  val size : forall (k: *) (v: *) . List <key: k, value: v> -> Int64 = /\ (k: *) (v: *).
    \(m: List <key: k, value: v>) ->
      case m of
        Nil -> 0
      | Cons h t -> ADD_INT64 1 (LF.Map:size @k @v t)
  ;

  val insert: forall (k: *) (v: *) . k -> v -> List <key: k, value: v> -> List <key: k, value: v> = /\ (k: *) (v: *) .
    \(key: k) (value: v) (m: List <key: k, value: v>) ->
      case m of
         Cons h t -> let keyH: k = (h).key in (
          case LESS @k key keyH of
            True -> Cons @<key: k, value: v> [<key = key, value = value>] m
          | False -> (
              case EQUAL @k key keyH of
                True  -> Cons @<key: k, value: v> [<key = key, value = value>] t
              | False -> Cons @<key: k, value: v> [h] (LF.Map:insert @k @v key value t)
          )
        )
      | Nil -> Cons @<key: k, value: v> [<key = key, value = value>] m
  ;

  val delete: forall (k: *) (v: *) . k -> List <key: k, value: v> -> List <key: k, value: v> = /\ (k: *) (v: *) .
    \(key: k) (m: List <key: k, value: v>) ->
      case m of
         Cons h t -> let keyH: k = (h).key in (
          case LESS @k key keyH of
            True -> m
          | False -> (
              case EQUAL @k key keyH of
                True  -> t
              | False -> LF.Map:delete @k @v key t
          )
        )
      | Nil -> m
  ;

  val lookup: forall (k: *) (v: *) . k -> List <key: k, value: v> -> Option v = /\ (k: *) (v: *) .
    \(key: k) (m: List <key: k, value: v>) ->
      case m of
         Cons h t -> let keyH: k = (h).key in (
          case LESS @k key keyH of
            True -> None @v
          | False -> (
              case EQUAL @k key keyH of
                True  -> Some @v (h).value
              | False -> LF.Map:lookup @k @v key t
          )
        )
      | Nil -> None @v
  ;

   val keys: forall (k: *) (v: *) . List <key: k, value: v> -> List k = /\ (k: *) (v: *) .
     \(m: List <key: k, value: v>) ->
       case m of
         Cons h t -> Cons @k [ (h).key ] (LF.Map:keys @k @v t)
       | Nil -> Nil @k
   ;

   val values: forall (k: *) (v: *) . List <key: k, value: v> -> List v = /\ (k: *) (v: *) .
     \(m: List <key: k, value: v>) ->
       case m of
         Cons h t -> Cons @v [ (h).value ] (LF.Map:values @k @v t)
       | Nil -> Nil @v
   ;

}
    """
  }

  val List(bMapInsert, bMapLookup, bMapDelete, bMapSize, bMapKeys, bMapValues) = {
    val modName = Ref.ModuleName.assertFromString("LF.Map")
    List("insert", "lookup", "delete", "size", "keys", "values").map { name =>
      SExpr.LfDefRef(
        Ref.DefinitionRef(
          internalPkgId,
          Ref.QualifiedName(modName, Ref.DottedName.assertFromString(name)),
        )
      )
    }
  }
}

/** Important: use the constructor only if you _know_ you have all the definitions! Otherwise
  * use the apply in the companion object, which will compile them for you.
  */
private[lf] final class PureCompiledPackages(
    override val signatures: Map[PackageId, PackageSignature],
    val definitions: Map[SExpr.SDefinitionRef, SDefinition],
    override val compilerConfig: Compiler.Config,
) extends CompiledPackages(compilerConfig) {
  override def getDefinition(ref: SExpr.SDefinitionRef): Option[SDefinition] = definitions.get(ref)
}

private[lf] object PureCompiledPackages {

  import CompiledPackages._

  /** Important: use this method only if you _know_ you have all the definitions! Otherwise
    * use the other apply, which will compile them for you.
    */
  def apply(
      packages: Map[PackageId, PackageSignature],
      definitions: Map[SExpr.SDefinitionRef, SDefinition],
      compilerConfig: Compiler.Config,
  ): PureCompiledPackages =
    new PureCompiledPackages(packages, definitions, compilerConfig)

  def build(
      packages: Map[PackageId, Package],
      compilerConfig: Compiler.Config,
  ): Either[String, PureCompiledPackages] = {
    val signatures = Util.toSignatures(packages) ++ preloadedPackageSignatures
    Compiler
      .compilePackages(
        pkgInterface = new PackageInterface(signatures),
        packages =
          packages.filterNot { case (pkgId, _) => preloadedPackageSignatures.isDefinedAt(pkgId) },
        compilerConfig = compilerConfig,
      )
      .map(defs => apply(signatures, defs ++ preloadedDefs, compilerConfig))
  }

  def assertBuild(
      packages: Map[PackageId, Package],
      compilerConfig: Compiler.Config,
  ): PureCompiledPackages =
    data.assertRight(build(packages, compilerConfig))

  def Empty(compilerConfig: Compiler.Config): PureCompiledPackages =
    PureCompiledPackages(preloadedPackageSignatures, preloadedDefs, compilerConfig)

}
