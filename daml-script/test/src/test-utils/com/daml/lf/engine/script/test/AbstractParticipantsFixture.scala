// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf.engine.script.test

import java.io.File

import com.daml.ledger.api.testing.utils.AkkaBeforeAndAfterAll
import com.daml.lf.archive.{Dar, DarDecoder}
import com.daml.lf.data.ImmArray
import com.daml.lf.data.Ref.{Identifier, Name, PackageId, QualifiedName}
import com.daml.lf.engine.script.ledgerinteraction.{ScriptLedgerClient, ScriptTimeMode}
import com.daml.lf.engine.script.{Participants, Runner}
import com.daml.lf.typesig.EnvironmentSignature
import com.daml.lf.typesig.reader.SignatureReader
import com.daml.lf.language.Ast.Package
import com.daml.lf.language.StablePackage
import com.daml.lf.speedy.{ArrayList, SValue}
import com.daml.lf.speedy.SValue.SRecord
import org.scalatest.Suite
import scalaz.\/-
import scalaz.syntax.traverse._
import spray.json.JsValue

import scala.concurrent.{ExecutionContext, Future}

// Fixture for a set of participants used in Daml Script tests
trait AbstractScriptTest extends AkkaBeforeAndAfterAll {
  self: Suite =>
  protected def timeMode: ScriptTimeMode

  protected def readDar(file: File): (Dar[(PackageId, Package)], EnvironmentSignature) = {
    val dar = DarDecoder.assertReadArchiveFromFile(file)
    val ifaceDar = dar.map(pkg => SignatureReader.readPackageSignature(() => \/-(pkg))._2)
    val envIface = EnvironmentSignature.fromPackageSignatures(ifaceDar)
    (dar, envIface)
  }

  protected def run(
      clients: Participants[ScriptLedgerClient],
      name: QualifiedName,
      inputValue: Option[JsValue] = None,
      dar: Dar[(PackageId, Package)],
  )(implicit ec: ExecutionContext): Future[SValue] = {
    val scriptId = Identifier(dar.main._1, name)
    Runner.run(dar, scriptId, inputValue, clients, timeMode)
  }

  def tuple(a: SValue, b: SValue) =
    SRecord(
      id = StablePackage.DA.Types.assertIdentifier("Tuple2"),
      fields = ImmArray(Name.assertFromString("_1"), Name.assertFromString("_2")),
      values = ArrayList(a, b),
    )
}
