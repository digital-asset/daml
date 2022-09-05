// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf.codegen.dependencygraph

import com.daml.lf.typesig.{DefTemplate, DefInterface, DefDataType, Record}

sealed abstract class NodeType extends Product with Serializable

private[dependencygraph] object NodeType {

  sealed abstract class Root extends NodeType

  object Root {

    final case class Template(record: Record.FWT, defTemplate: DefTemplate.FWT) extends Root

    final case class Interface(defInterface: DefInterface.FWT) extends Root

  }

  final case class Internal(defDataType: DefDataType.FWT) extends NodeType

}
