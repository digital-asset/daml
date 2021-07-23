package com.daml.lf

import com.daml.lf.data.Ref._
import com.daml.lf.language._
import com.daml.lf.archive.{Dar, DarReader, Decode}
import scalaz.syntax.traverse._

import java.io.File

object IdSize {

  def main(args: Array[String]) = {
    if (args.size != 1) {
      sys.error("Expected exactly one argument")
    }
    val darFile = new File(args(0))
    val dar: Dar[(PackageId, Ast.Package)] = DarReader.assertReadArchiveFromFile(darFile).map(Decode.assertDecodeArchivePayload(_))

    var longestModuleName: String = ""
    var longestTypeSynonym: String = ""
    var longestTypeCon: String = ""
    var longestValue: String = ""
    var longestRecordField: String = ""
    var longestVariantConstructor: String = ""
    var longestEnumConstructor: String = ""
    dar.all.foreach { case (_, pkg) =>
      pkg.modules.foreach { case (moduleName, module) =>
        if (moduleName.toString.length > longestModuleName.length) {
          longestModuleName = moduleName.toString
        }
        module.definitions.foreach { case (name, definition) =>
          definition match {
            case _ : Ast.DTypeSyn =>
              if (name.toString.length > longestTypeSynonym.length) {
                longestTypeSynonym = name.toString
              }
            case Ast.DDataType(_, _, cons) =>
              if (name.toString.length > longestTypeCon.length) {
                longestTypeCon = name.toString
              }
              cons match {
                case Ast.DataRecord(fields) =>
                  fields.foreach { case (name, _) =>
                    if (name.toString.length > longestRecordField.length) {
                      longestRecordField = name.toString
                    }
                  }
                case Ast.DataVariant(variants) =>
                  variants.foreach { case (name, _) =>
                    if (name.toString.length > longestVariantConstructor.length) {
                      longestVariantConstructor = name.toString
                    }
                  }
                case Ast.DataEnum(constrs) =>
                  constrs.foreach { case name =>
                    if (name.toString.length > longestEnumConstructor.length) {
                      longestEnumConstructor = name.toString
                    }
                  }

              }
            case _ : Ast.GenDValue[_] =>
              if (name.toString.length > longestValue.length) {
                longestValue = name.toString
              }
          }
        }
      }
    }
    println(s"Module name: ${longestModuleName.length} $longestModuleName")
    println(s"Type synonym: ${longestTypeSynonym.length} $longestTypeSynonym")
    println(s"Type constructor: ${longestTypeCon.length} $longestTypeCon")
    println(s"Value: ${longestValue.length} $longestValue")
    println(s"RecordField: ${longestRecordField.length} $longestRecordField")
    println(s"VariantConstructor: ${longestVariantConstructor.length} $longestVariantConstructor")
    println(s"EnumConstructor: ${longestEnumConstructor.length} $longestEnumConstructor")
  }

}
