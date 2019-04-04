// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.daml.lf.codegen.backend

import com.digitalasset.daml.lf.codegen.conf.Conf
import com.digitalasset.daml.lf.codegen.{InterfaceTrees, NodeWithContext}
import com.digitalasset.daml.lf.iface.PackageId
import com.digitalasset.daml.lf.iface.reader.Interface

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
