// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.daml.lf
package speedy
package explore

import com.digitalasset.daml.lf.archive.UniversalArchiveDecoder
import com.digitalasset.daml.lf.data.Ref.{DefinitionRef, Identifier, QualifiedName}
import com.digitalasset.daml.lf.speedy.SExpr._
import com.digitalasset.daml.lf.speedy.SResult._
import com.digitalasset.daml.lf.speedy.SValue._
import com.digitalasset.daml.lf.speedy.Speedy._
import com.daml.logging.LoggingContext

import java.io.File

object LoadDarFunction extends App {

  private[this] implicit def logContext: LoggingContext = LoggingContext.ForTesting

  def load(darFile: File, base: String, funcName: String): (Long => Long) = {

    val packages = UniversalArchiveDecoder.assertReadFile(darFile)

    val compilerConfig =
      Compiler.Config
        .Default(packages.main._2.languageVersion.major)
        .copy(stacktracing = Compiler.NoStackTrace)

    val compiledPackages: CompiledPackages =
      PureCompiledPackages.assertBuild(packages.all.toMap, compilerConfig)

    def function(argValue: Long): Long = {
      val expr: SExpr = {
        val ref: DefinitionRef =
          Identifier(packages.main._1, QualifiedName.assertFromString(s"${base}:${funcName}"))
        val func = SEVal(LfDefRef(ref))
        val arg = SInt64(argValue)
        SEApp(func, Array(arg))
      }
      val machine = Machine.fromPureSExpr(compiledPackages, expr)

      machine.run() match {
        case SResultFinal(SInt64(result)) => result
        case res => throw new RuntimeException(s"Unexpected result from machine $res")
      }
    }

    function
  }

}
