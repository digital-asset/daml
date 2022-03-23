// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf.codegen.dependencygraph

import com.daml.lf.iface.DefDataType

// wraps type declarations and templates so that they can be used together
sealed abstract class TypeDeclOrTemplateWrapper[+TmplI] extends Product with Serializable

final case class TypeDeclWrapper(typeDecl: DefDataType.FWT)
    extends TypeDeclOrTemplateWrapper[Nothing]

final case class TemplateWrapper[+TmplI](template: TmplI) extends TypeDeclOrTemplateWrapper[TmplI]
