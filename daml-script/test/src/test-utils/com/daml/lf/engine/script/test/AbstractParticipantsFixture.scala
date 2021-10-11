// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf.engine.script.test

import java.io.File
import java.util

import com.daml.ledger.api.testing.utils.AkkaBeforeAndAfterAll
import com.daml.lf.archive.{Dar, DarDecoder}
import com.daml.lf.data.ImmArray
import com.daml.lf.data.Ref.{Identifier, Name, PackageId, QualifiedName}
import com.daml.lf.engine.script.ledgerinteraction.{ScriptLedgerClient, ScriptTimeMode}
import com.daml.lf.engine.script.{Participants, Runner}
import com.daml.lf.iface.EnvironmentInterface
import com.daml.lf.iface.reader.InterfaceReader
import com.daml.lf.language.Ast.Package
import com.daml.lf.speedy.SValue
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

  protected def readDar(file: File): (Dar[(PackageId, Package)], EnvironmentInterface) = {
    val dar = DarDecoder.assertReadArchiveFromFile(file)
    val ifaceDar = dar.map(pkg => InterfaceReader.readInterface(() => \/-(pkg))._2)
    val envIface = EnvironmentInterface.fromReaderInterfaces(ifaceDar)
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

  def tuple(a: SValue, b: SValue) = {
    val vals = new util.ArrayList[SValue](2);
    vals.add(a)
    vals.add(b)
    SRecord(
      id = Identifier(
        PackageId.assertFromString(
          "40f452260bef3f29dede136108fc08a88d5a5250310281067087da6f0baddff7"
        ),
        QualifiedName.assertFromString("DA.Types:Tuple2"),
      ),
      fields = ImmArray(Name.assertFromString("_1"), Name.assertFromString("_2")),
      values = vals,
    )
  }
}
