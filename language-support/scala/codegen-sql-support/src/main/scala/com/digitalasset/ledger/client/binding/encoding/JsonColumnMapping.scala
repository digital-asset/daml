// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.ledger.client.binding.encoding

import scala.language.existentials
import scala.reflect.ClassTag
import spray.json.JsValue
import SlickTypeEncoding.SupportedProfile

import slick.ast.{BaseTypedType, FieldSymbol, ScalaBaseType, ScalaType}
import slick.jdbc.{H2Profile, JdbcType, PostgresProfile, SQLiteProfile}

abstract class JsonColumnMapping[-Profile <: SupportedProfile] {
  def column(profile: Profile): profile.BaseColumnType[JsValue]
}

object JsonColumnMapping extends JsonColumnMappingLowPriority {
  import slick.jdbc._

  implicit val `h2 instance`: JsonColumnMapping[H2Profile] with Optimized = stringMapping

  implicit val `postgres instance`: JsonColumnMapping[PostgresProfile] with Optimized =
    new JCMInstance[PostgresProfile] {
      override def column(profile: PostgresProfile): profile.BaseColumnType[JsValue] =
        postgresJsonColumnType(profile)
    }

  private[this] lazy val pgObjectCall = {
    val k = Class forName "org.postgresql.util.PGobject"
    (k, k.getMethod("setType", classOf[String]), k.getMethod("setValue", classOf[String]))
  }

  private[this] def postgresJsonColumnType(
      profile: PostgresProfile): profile.BaseColumnType[JsValue] =
    new JdbcType[JsValue] with BaseTypedType[JsValue] {
      import java.sql.{PreparedStatement, ResultSet}

      // reflection is fun, said no one ever
      private[this] def pgObject(v: JsValue): Any = {
        val (pgc, st, sv) = pgObjectCall
        val pgo: Any = pgc.newInstance()
        st.invoke(pgo, sqlTypeName(None))
        sv.invoke(pgo, v.compactPrint)
        pgo
      }

      override def sqlType: Int = java.sql.Types.OTHER
      override def sqlTypeName(size: Option[FieldSymbol]): String = "json"
      override def setValue(v: JsValue, p: PreparedStatement, idx: Int): Unit =
        p.setObject(idx, pgObject(v))
      override def setNull(p: PreparedStatement, idx: Int): Unit = p.setNull(idx, sqlType)
      override def getValue(r: ResultSet, idx: Int): JsValue = {
        import spray.json._
        r.getString(idx).parseJson
      }
      // XXX I have no idea if this is right, but it will always be false
      // for proper LF schemata, anyway
      override def wasNull(r: ResultSet, idx: Int): Boolean = r.getObject(idx) eq null
      override def updateValue(v: JsValue, r: ResultSet, idx: Int): Unit =
        r.updateObject(idx, pgObject(v))
      override def valueToSQLLiteral(value: JsValue): String =
        s"(${profile.api.stringColumnType.valueToSQLLiteral(value.compactPrint)}::json)"
      override def hasLiteralForm: Boolean = true
      override def scalaType: ScalaType[JsValue] = ScalaBaseType[JsValue]
      override def classTag: ClassTag[_] = implicitly[ClassTag[JsValue]]
    }

  implicit val `sqlite instance`: JsonColumnMapping[SQLiteProfile] = stringMapping

  abstract sealed class JCMInstance[-Profile <: SupportedProfile]
      extends JsonColumnMapping[Profile]
      with Optimized

  sealed trait Optimized { this: JsonColumnMapping[_] =>
  }
}

sealed abstract class JsonColumnMappingLowPriority { this: JsonColumnMapping.type =>
  def runtimeMapping: JsonColumnMapping[SupportedProfile] = new JCMInstance[SupportedProfile] {
    override def column(profile: SupportedProfile): profile.BaseColumnType[JsValue] =
      profile match {
        case p: PostgresProfile => `postgres instance`.column(p)
        case p: H2Profile => `h2 instance`.column(p)
        case p: SQLiteProfile => `sqlite instance`.column(p)
        case _ => stringMapping.column(profile)
      }
  }

  protected final def stringMapping[Profile <: SupportedProfile]
    : JsonColumnMapping[Profile] with Optimized =
    new JCMInstance[Profile] {
      override def column(profile: Profile): profile.BaseColumnType[JsValue] = {
        import spray.json._
        import profile.api.{MappedColumnType, stringColumnType}
        MappedColumnType.base[JsValue, String](_.compactPrint, _.parseJson)
      }
    }
}
