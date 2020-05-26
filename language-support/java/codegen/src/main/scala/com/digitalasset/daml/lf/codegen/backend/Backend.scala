// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf.codegen.backend

import com.daml.lf.codegen.conf.Conf
import com.daml.lf.codegen.{InterfaceTrees, NodeWithContext}
import com.daml.lf.data.Ref.PackageId
import com.daml.lf.iface.Interface

import scala.concurrent.{ExecutionContext, Future}

private[codegen] trait Backend {

  /**
    * Transform a [[InterfaceTrees]] such that it works for a given backend, e.g. by escaping
    * the reserved keywords.
    */
  def preprocess(interfaces: Seq[Interface], conf: Conf, packagePrefixes: Map[PackageId, String])(
      implicit ec: ExecutionContext): Future[InterfaceTrees]

  def process(
      nodeWithContext: NodeWithContext,
      conf: Conf,
      packagePrefixes: Map[PackageId, String])(implicit ec: ExecutionContext): Future[Unit]
}
