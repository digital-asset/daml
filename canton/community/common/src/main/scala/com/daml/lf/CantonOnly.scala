// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf

import com.daml.lf.data.ImmArray
import com.daml.lf.data.Ref.PackageId
import com.daml.lf.language.{Ast, LanguageMajorVersion}
import com.daml.lf.speedy.Compiler
import com.daml.lf.transaction.TransactionCoder.{DecodeNid, EncodeNid}
import com.daml.lf.transaction.{
  Node,
  NodeId,
  TransactionCoder,
  TransactionOuterClass,
  TransactionVersion,
}
import com.daml.lf.value.ValueCoder
import com.digitalasset.canton.protocol.{
  LfNode,
  LfNodeId,
  LfTransaction,
  LfTransactionVersion,
  LfVersionedTransaction,
}

/** As part of upstream Daml-LF refactoring, previously accessible capabilities have become Canton-private. This
  * enables Daml-LF to limit it's API surface area while still allowing Canton deeper visibility into transaction
  * internals.
  */
// TODO(i3065): Get rid of lf.CantonOnly again
object CantonOnly {
  def lfVersionedTransaction(
      nodes: Map[LfNodeId, LfNode],
      roots: ImmArray[LfNodeId],
  ): LfVersionedTransaction =
    LfTransactionVersion.asVersionedTransaction(LfTransaction(nodes, roots))

  def tryBuildCompiledPackages(
      darMap: Map[PackageId, Ast.Package],
      enableLfDev: Boolean,
  ): PureCompiledPackages = {
    PureCompiledPackages.assertBuild(
      darMap,
      if (enableLfDev) Compiler.Config.Dev(LanguageMajorVersion.V1)
      else Compiler.Config.Default(LanguageMajorVersion.V1),
    )
  }

  def encodeNode(
      encodeNid: EncodeNid,
      encodeCid: ValueCoder.EncodeCid,
      enclosingVersion: TransactionVersion,
      nodeId: NodeId,
      node: Node,
  ): Either[ValueCoder.EncodeError, TransactionOuterClass.Node] =
    TransactionCoder.encodeNode(encodeNid, encodeCid, enclosingVersion, nodeId, node)

  def decodeVersionedNode(
      decodeNid: DecodeNid,
      decodeCid: ValueCoder.DecodeCid,
      transactionVersion: TransactionVersion,
      protoNode: TransactionOuterClass.Node,
  ): Either[ValueCoder.DecodeError, (NodeId, Node)] =
    TransactionCoder.decodeVersionedNode(
      decodeNid,
      decodeCid,
      transactionVersion,
      protoNode,
    )
}
