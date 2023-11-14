// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf
package speedy

import com.daml.lf.language.LanguageMajorVersion
import com.daml.lf.testing.parser._
import com.daml.logging.LoggingContext
import org.openjdk.jmh.annotations._

@State(Scope.Benchmark)
class StructProjBench {

  import com.daml.lf.testing.parser.Implicits.SyntaxHelper

  private[this] implicit def logContext: LoggingContext = LoggingContext.ForTesting

  // log2 of the number of iterations
  // Above 2^12=4096 the validation/compilation seems to be a bit long
  @Param(Array("12"))
  var m: Int = _
  private def M = 1 << m

  // log2 of the structural record size.
  // We test up to 2^7=128 as 100 seems to be a reasonable limit for the number
  // of fields of record typically used by Daml.
  @Param(Array("0", "1", "2", "3", "4", "5", "6", "7"))
  var n: Int = _
  private def N = 1 << n

  @Param(Array("1", "2"))
  var majorLfVersion: String = _
  private def MAJOR_LF_VERSION = LanguageMajorVersion
    .fromString(majorLfVersion)
    .getOrElse(
      throw new IllegalArgumentException(s"cannot parse major LF version: $majorLfVersion")
    )

  implicit def parserParameters: ParserParameters[this.type] =
    ParserParameters.defaultFor(MAJOR_LF_VERSION)
  private[this] def defaultPackageId = parserParameters.defaultPackageId

  private[this] def pkg = {
    p"""
       module Mod {

        synonym Struct = < ${(0 until N).map(i => s"x$i : Int64").mkString(",")} > ;

        val struct: |Mod:Struct| = < ${(0 until N).map(i => s"x$i = $i").mkString(",")} > ;

        val bench: |Mod:Struct| -> Int64 = \(s: |Mod:Struct|) ->
          ${(0 until M).map(i => s"let y$i: Int64 = (s).x${i % N} in").mkString(" ")}
          y${M - 1};
       }
       """
  }

  private[this] var compiledPackages: PureCompiledPackages = _
  private[this] var sexpr: SExpr.SExpr = _

  @Setup(Level.Trial)
  def init(): Unit = {
    assert(m >= n)
    println(s"LF = $MAJOR_LF_VERSION, M = $M, N = $N")
    val config = Compiler.Config
      .Dev(MAJOR_LF_VERSION)
      .copy(packageValidation = Compiler.NoPackageValidation)
    compiledPackages = PureCompiledPackages.assertBuild(Map(defaultPackageId -> pkg), config)
    sexpr = compiledPackages.compiler.unsafeCompile(e"Mod:bench Mod:struct")
    val value = bench()
    val expected = SValue.SInt64((N - 1).toLong)
    assert(value == expected, s"$value != $expected")
  }

  @Benchmark
  def bench(): SValue = {
    val machine = Speedy.Machine.fromPureSExpr(compiledPackages, sexpr)
    machine.run() match {
      case SResult.SResultFinal(v) =>
        v
      case otherwise =>
        throw new UnknownError(otherwise.toString)
    }
  }
}
