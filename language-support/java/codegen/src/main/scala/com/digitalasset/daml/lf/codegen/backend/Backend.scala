// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf.codegen.backend

import _root_.java.nio.file.Path

import com.daml.lf.codegen.{NodeWithContext, InterfaceTree}
import com.daml.lf.data.Ref.PackageId
import com.daml.lf.iface.Interface

import scala.concurrent.{Future, ExecutionContext}

private[codegen] trait Backend {

  /** Transform an [[Interface]] such that it works for a given backend, e.g. by escaping
    * the reserved keywords.
    */
  def preprocess(
      interfaces: Seq[Interface],
      outputDirectory: Path,
      decoderPkgAndClass: Option[(String, String)] = None,
      packagePrefixes: Map[PackageId, String],
  )(implicit
      ec: ExecutionContext
  ): Future[Seq[InterfaceTree]]

  def process(
      nodeWithContext: NodeWithContext,
      packagePrefixes: Map[PackageId, String],
      outputDirectory: Path,
  )(implicit ec: ExecutionContext): Future[Unit]
}
