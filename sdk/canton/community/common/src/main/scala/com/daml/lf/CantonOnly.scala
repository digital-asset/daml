// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.daml.lf

import com.digitalasset.canton.protocol.{
  LfNode,
  LfNodeId,
  LfSerializationVersion,
  LfTransaction,
  LfVersionedTransaction,
}
import com.digitalasset.daml.lf.data.ImmArray
import com.digitalasset.daml.lf.data.Ref.PackageId
import com.digitalasset.daml.lf.language.Ast
import com.digitalasset.daml.lf.speedy.Compiler

/** As part of upstream Daml-LF refactoring, previously accessible capabilities have become
  * Canton-private. This enables Daml-LF to limit its API surface area while still allowing Canton
  * deeper visibility into transaction internals.
  */
// TODO(i3065): Get rid of lf.CantonOnly again
object CantonOnly {
  def lfVersionedTransaction(
      nodes: Map[LfNodeId, LfNode],
      roots: ImmArray[LfNodeId],
  ): LfVersionedTransaction =
    LfSerializationVersion.asVersionedTransaction(LfTransaction(nodes, roots))

  def tryBuildCompiledPackages(
      darMap: Map[PackageId, Ast.Package],
      enableLfDev: Boolean,
  ): PureCompiledPackages =
    PureCompiledPackages.assertBuild(
      darMap,
      if (enableLfDev) Compiler.Config.Dev
      else Compiler.Config.Default,
    )
}
