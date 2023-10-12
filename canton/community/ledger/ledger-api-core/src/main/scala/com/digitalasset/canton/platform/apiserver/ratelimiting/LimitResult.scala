// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.platform.apiserver.ratelimiting

import com.daml.error.DamlError

import scala.annotation.tailrec

sealed trait LimitResult {
  final def map(f: Unit => Unit): LimitResult = {
    f(()); this
  }

  def flatMap(f: Unit => LimitResult): LimitResult
}

object LimitResult {

  type FullMethodName = String
  type IsStream = Boolean
  type LimitResultCheck = (FullMethodName, IsStream) => LimitResult

  implicit class LimitResultCheckOps(checks: List[LimitResultCheck]) {
    def traverse(fullMethodName: String, isStream: Boolean): LimitResult = {
      @tailrec
      def loop(todo: List[LimitResultCheck], result: LimitResult): LimitResult = {
        todo match {
          case Nil => result
          case _ if result != UnderLimit => result
          case head :: tail => loop(tail, head(fullMethodName, isStream))
        }
      }
      loop(checks, UnderLimit)
    }
  }

  case object UnderLimit extends LimitResult {
    override def flatMap(f: Unit => LimitResult): LimitResult = f(())
  }

  final case class OverLimit(error: DamlError) extends LimitResult {
    override def flatMap(f: Unit => LimitResult): LimitResult = this
  }

}
