// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package user

import com.digitalasset.daml.lf.{Choice, ConsumingChoice, Contract, Template, TemplateId}
import com.digitalasset.daml.lf.ledger.api.{
  LfIdentifier,
  LfValue,
  LfValueConverterFrom,
  LfValueConverterTo,
  LfValueInt,
  LfValueParty,
  LfValueRecord,
}
import com.digitalasset.daml.lf.ledger

class SimpleTemplate(val owner: LfValueParty, val count: LfValueInt) extends Template {

  // FIXME: can annotation processing be used to auto-generate this?
  override val arg = LfValueRecord("owner" -> owner, "count" -> count)

  private implicit val companion: SimpleTemplate.type = SimpleTemplate

  override val signatories = {
    Seq(owner)
  }

  def SimpleTemplate_increment(
      n: LfValueInt
  ): Choice[Contract[SimpleTemplate, SimpleTemplate.type]] =
    new ConsumingChoice[Contract[SimpleTemplate, SimpleTemplate.type]] {

      override def controllers = {
        Seq(owner)
      }

      override def exercise() = {
        import LfValueConverterTo._
        import LfValueConverterFrom._

        ledger.api.logInfo(
          s"called Scala SimpleTemplate_increment(${n.toValue[Int]}) with count = ${count.toValue[Int]}"
        )

        new SimpleTemplate(owner, (count.toValue[Int] + n.toValue[Int]).toLfValue)
          .create[SimpleTemplate, SimpleTemplate.type]()
      }
    }
}

// FIXME: can annotation processing be used to auto-generate this?
object SimpleTemplate extends TemplateId[SimpleTemplate] {
  override val templateId: LfIdentifier =
    LfIdentifier(moduleName = "user", name = "SimpleTemplate#apply")

  override def apply(arg: LfValue): SimpleTemplate = arg match {
    case LfValueRecord(fields) =>
      new SimpleTemplate(
        fields.getOrElse("owner", fields("_0")).asInstanceOf[LfValueParty],
        fields.getOrElse("count", fields("_1")).asInstanceOf[LfValueInt],
      )

    case _ =>
      sys.error(s"invalid record argument: $arg")
  }
}
