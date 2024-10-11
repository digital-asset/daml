// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf
package data

// glorified Either to handle template or interface cases
sealed abstract class TemplateOrInterface[+T, +I] extends Product with Serializable

object TemplateOrInterface {
  final case class Template[+T](value: T) extends TemplateOrInterface[T, Nothing]
  final case class Interface[+I](value: I) extends TemplateOrInterface[Nothing, I]

  implicit final class MergeOps[T](private val x: TemplateOrInterface[T, T]) extends AnyVal {
    def merge: T = x match {
      case Template(value) => value
      case Interface(value) => value
    }
  }

}
