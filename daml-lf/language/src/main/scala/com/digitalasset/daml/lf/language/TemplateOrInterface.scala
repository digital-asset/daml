// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf
package language

// glorified Either to handle template or interface cases
sealed abstract class TemplateOrInterface[+T, +I]
object TemplateOrInterface {
  final case class Template[+T](value: T) extends TemplateOrInterface[T, Nothing]
  final case class Interface[+I](value: I) extends TemplateOrInterface[Nothing, I]
}
