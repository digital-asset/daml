// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf

import com.daml.lf.data.Ref.PackageId
import com.daml.lf.language.Ast.{Package, PackageSignature}
import com.daml.lf.language.{LanguageVersion, Util}
import com.daml.lf.speedy.SExpr.SDefinitionRef
import com.daml.lf.speedy.{Compiler, SDefinition}

/** Trait to abstract over a collection holding onto DAML-LF package definitions + the
  * compiled speedy expressions.
  */
private[lf] abstract class CompiledPackages(
    compilerConfig: Compiler.Config,
) {
  def getSignature(pkgId: PackageId): Option[PackageSignature]
  def getDefinition(dref: SDefinitionRef): Option[SDefinition]

  def signatures: PartialFunction[PackageId, PackageSignature] = Function.unlift(this.getSignature)
  def packageIds: Set[PackageId]
  def definitions: PartialFunction[SDefinitionRef, SDefinition] =
    Function.unlift(this.getDefinition)

  def packageLanguageVersion: PartialFunction[PackageId, LanguageVersion] =
    signatures andThen (_.languageVersion)

  final def compiler: Compiler = new Compiler(signatures, compilerConfig)
}

/** Important: use the constructor only if you _know_ you have all the definitions! Otherwise
  * use the apply in the companion object, which will compile them for you.
  */
private[lf] final class PureCompiledPackages(
    signatures: Map[PackageId, PackageSignature],
    defns: Map[SDefinitionRef, SDefinition],
    compilerConfig: Compiler.Config,
) extends CompiledPackages(compilerConfig) {
  override def packageIds: Set[PackageId] = signatures.keySet
  override def getSignature(pkgId: PackageId): Option[PackageSignature] = signatures.get(pkgId)
  override def getDefinition(dref: SDefinitionRef): Option[SDefinition] = defns.get(dref)
}

private[lf] object PureCompiledPackages {

  /** Important: use this method only if you _know_ you have all the definitions! Otherwise
    * use the other apply, which will compile them for you.
    */
  def apply(
      signatures: Map[PackageId, PackageSignature],
      defns: Map[SDefinitionRef, SDefinition],
      compilerConfig: Compiler.Config,
  ): PureCompiledPackages =
    new PureCompiledPackages(signatures, defns, compilerConfig)

  def apply(
      packages: Map[PackageId, Package],
      compilerConfig: Compiler.Config = Compiler.Config.Default,
  ): Either[String, PureCompiledPackages] = {
    val signatures = Util.toSignatures(packages)
    Compiler
      .compilePackages(signatures, packages, compilerConfig)
      .map(apply(signatures, _, compilerConfig))
  }

}
