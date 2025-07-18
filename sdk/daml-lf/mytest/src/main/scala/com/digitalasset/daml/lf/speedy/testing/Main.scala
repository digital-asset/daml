// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.daml.lf
package speedy
package testing

import com.digitalasset.daml.lf.data._
import com.digitalasset.daml.lf.data.Ref._
import com.digitalasset.daml.lf.language.Ast._
import com.digitalasset.daml.lf.archive.{DarReader, ArchiveReader, ArchiveDecoder}
import com.digitalasset.daml.lf.speedy.Pretty._
import com.digitalasset.daml.lf.speedy.SResult._
import com.digitalasset.daml.lf.speedy.SExpr.LfDefRef
import com.digitalasset.daml.lf.validation.Validation
//import com.digitalasset.daml.lf.testing.parser
import com.digitalasset.daml.lf.language.{
  LanguageMajorVersion,
  PackageInterface,
  LanguageVersion => LV,
}
import com.daml.logging.LoggingContext

import java.io.{File, PrintWriter, StringWriter}
import java.nio.file.{Path, Paths}
import java.io.PrintStream
import org.jline.builtins.Completers
import org.jline.reader.{History, LineReader, LineReaderBuilder}
import org.jline.reader.impl.completer.{AggregateCompleter, ArgumentCompleter, StringsCompleter}
import org.jline.reader.impl.history.DefaultHistory

import scala.collection.immutable.ListMap
import scala.concurrent.duration._
import scala.jdk.CollectionConverters._

import com.digitalasset.daml.lf.archive._

object Main extends App {
  // idempotent; force stdout to output in UTF-8 -- in theory it should pick it up from
  // the locale, but in practice it often seems to _not_ do that.
  val out = new PrintStream(System.out, true, "UTF-8")
  System.setOut(out)

  // val proto = ArchiveReader.fromFile(new File("/home/dylanthinnes/.cache/bazel/_bazel_dylanthinnes/75f97a43b4974eaa4f4aa2df69b7ec80/execroot/com_github_digital_asset_daml/bazel-out/k8-opt/bin/daml-script/test/script-test-v2.dev.dar")).toOption.get
  val idkwhat = DarReader.assertReadArchiveFromFile(
    new File("/home/roger.bosman/Documents/playground/ScriptTest2/.daml/dist/ScriptTest2-0.0.1.dar")
  )

  // val ArchivePayload.Lf2(pkgId, proto, minor) = idkwhat.main

  println(idkwhat.main)

  val ast = Decode.decodeArchivePayload(idkwhat.main)
  // println(proto)
  // println(ast)
}
