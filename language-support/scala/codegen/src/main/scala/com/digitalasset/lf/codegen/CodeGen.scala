// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf.codegen

import java.io._

import com.daml.lf.codegen.types.Namespace
import com.daml.lf.archive.{ArchivePayload, Dar, UniversalArchiveReader}
import com.daml.lf.data.Ref
import com.daml.lf.typesig
import typesig.{Type => _, reader => _, _}
import typesig.reader.{Errors, SignatureReader}
import Errors.ErrorLoc
import com.daml.lf.codegen.dependencygraph._
import com.daml.lf.codegen.exception.PackageInterfaceException
import lf.{LFUtil, ScopedDataType}
import com.daml.lf.data.Ref._
import com.typesafe.scalalogging.Logger
import scalaz.{Enum => _, _}
import scalaz.std.tuple._
import scalaz.std.set._
import scalaz.std.string._
import scalaz.std.vector._
import scalaz.syntax.std.option._
import scalaz.syntax.bind._
import scalaz.syntax.traverse1._

object CodeGen {

  private val logger: Logger = Logger(getClass)

  val universe: scala.reflect.runtime.universe.type = scala.reflect.runtime.universe
  import universe._

  import LFUtil.{FilePlan, WriteParams}

  /*
   * Given a Daml package (in DAR or DALF format), a package name and an output
   * directory, this function writes a bunch of generated .scala files
   * to 'outputDir' that mirror the namespace of the Daml package.
   *
   * This function throws exception when an unexpected error happens. Unexpected errors are:
   * - input file not found or not readable
   * - package interface extraction failed
   */
  @throws[FileNotFoundException](cause = "input file not found")
  @throws[SecurityException](cause = "input file not readable")
  @throws[PackageInterfaceException](
    cause = "either decoding a package from a file or extracting" +
      " the package interface failed"
  )
  def generateCode(
      files: List[File],
      packageName: String,
      outputDir: File,
      roots: Seq[String] = Seq(),
  ): Unit =
    files match {
      case Nil =>
        throw PackageInterfaceException("Expected at least one DAR or DALF input file.")
      case f :: fs =>
        generateCodeSafe(NonEmptyList(f, fs: _*), packageName, outputDir, roots)
          .fold(es => throw PackageInterfaceException(formatErrors(es)), identity)
    }

  private def formatErrors(es: NonEmptyList[String]): String =
    es.toList.mkString("\n")

  def generateCodeSafe(
      files: NonEmptyList[File],
      packageName: String,
      outputDir: File,
      roots: Seq[String],
  ): ValidationNel[String, Unit] =
    decodeInterfaces(files).map { interfaces: NonEmptyList[EnvironmentSignature] =>
      val combined = interfaces.suml1.resolveRetroImplements
      val interface = combined.copy(
        typeDecls = Util.filterTemplatesBy(roots.map(_.r))(combined.typeDecls)
      )
      packageInterfaceToScalaCode(LFUtil(packageName, interface, outputDir))
    }

  private def decodeInterfaces(
      files: NonEmptyList[File]
  ): ValidationNel[String, NonEmptyList[EnvironmentSignature]] = {
    files.traverse(f => decodeInterface(parseFile(_))(f).validationNel)
  }

  private def parseFile(f: File): String \/ Dar[ArchivePayload] =
    UniversalArchiveReader.readFile(f) match {
      case Right(p) => \/.right(p)
      case Left(e) =>
        logger.error("Scala Codegen error", e)
        \/.left(e.getLocalizedMessage)
    }

  private def decodeInterface(parse: File => String \/ Dar[ArchivePayload])(
      file: File
  ): String \/ EnvironmentSignature =
    parse(file).flatMap(decodeInterface)

  private def decodeInterface(dar: Dar[ArchivePayload]): String \/ EnvironmentSignature = {
    import scalaz.syntax.traverse._
    dar.traverse(decodeInterface).map(combineInterfaces)
  }

  private def decodeInterface(p: ArchivePayload): String \/ PackageSignature =
    \/.attempt {
      logger.info(s"decoding archive with Package ID: ${p.pkgId}")
      val (errors, out) = PackageSignature.read(p)
      (if (!errors.empty) {
         -\/(formatDecodeErrors(p.pkgId, errors))
       } else \/-(out)): String \/ PackageSignature
    }(_.getLocalizedMessage).join

  private def formatDecodeErrors(
      packageId: PackageId,
      errors: Errors[ErrorLoc, SignatureReader.InvalidDataTypeDefinition],
  ): String =
    (Cord(s"Errors decoding LF archive (Package ID: $packageId):\n") ++
      SignatureReader.Error.treeReport(errors)).toString

  private def combineInterfaces(dar: Dar[PackageSignature]): EnvironmentSignature =
    EnvironmentSignature.fromPackageSignatures(dar)

  private def templateCount(types: Iterable[(_, PackageSignature.TypeDecl)]): Int =
    types.count {
      case (_, _: PackageSignature.TypeDecl.Template) => true
      case _ => false
    }

  private def packageInterfaceToScalaCode(util: LFUtil): Unit = {
    val typeDeclarationsToGenerate =
      DependencyGraph.transitiveClosure(
        serializableTypes = util.iface.typeDecls,
        interfaces = util.iface.interfaces,
      )

    // Each record/variant has Scala code generated for it individually, unless their names are related
    writeTemplatesAndTypes(util)(WriteParams(typeDeclarationsToGenerate))

    val totalTemplates = templateCount(util.iface.typeDecls)
    val generated = templateCount(typeDeclarationsToGenerate.serializableTypes) // TODO(#13349)
    val notGenerated = totalTemplates - generated

    val errorMessages = typeDeclarationsToGenerate.errors.map(_.msg).mkString("\n")

    logger.info(
      s"""Scala Codegen result:
          |Number of generated templates: $generated
          |Number of not generated templates: $notGenerated
          |Details: $errorMessages""".stripMargin
    )
  }

  private[codegen] def produceTemplateAndTypeFilesLF(
      wp: WriteParams,
      util: lf.LFUtil,
  ): IterableOnce[FilePlan] = {
    import wp._

    // New prep steps for LF codegen
    // 1. collect records, search variants and splat/filter
    val (unassociatedRecords, splattedVariants, enums) = splatVariants(definitions)

    // TODO(#13349) include interfaces as well
    // 2. put templates/types into single Namespace.fromHierarchy
    val treeified: Namespace[String, Option[lf.HierarchicalOutput.TemplateOrDatatype]] =
      Namespace.fromHierarchy {
        def widenDDT[R, V](iddt: Iterable[ScopedDataType.DT[R, V]]) = iddt
        import lf.HierarchicalOutput.{PotentialFile => SrcV}
        val ntdPotentials =
          (widenDDT(unassociatedRecords ++ enums) ++ splattedVariants)
            .map(sdt => (sdt.name, SrcV.NormalDt(sdt): SrcV))
        val tmplPotentials = templateIds.transform((_, v) => SrcV.Tpl(v): SrcV)
        val interfacePotentials = interfaces.transform((_, v) => SrcV.Interface(v): SrcV)

        (ntdPotentials ++ tmplPotentials ++ interfacePotentials) map {
          case (ddtIdent @ Identifier(_, qualName), body) =>
            (qualName.module.segments.toList ++ qualName.name.segments.toList, (ddtIdent, body))
        }
      }

    // fold up the tree to discover the hierarchy's roots, each of which produces a file
    val (treeErrors, topFiles) = lf.HierarchicalOutput.discoverFiles(treeified, util)
    val filePlans = topFiles.map { case (fil, trees) =>
      \/-((None, fil, trees)): FilePlan
    } ++ treeErrors
      .map(e => -\/(e): FilePlan)

    // Finally we generate the "event decoder" and "package ID source"
    val specials =
      Seq(lf.EventDecoderGen.generate(util, templateIds.keySet), lf.PackageIDsGen.generate(util))

    val specialPlans = specials map { case (fp, t) => \/-((None, fp, t)): FilePlan }

    filePlans ++ specialPlans
  }

  private[this] def splitNTDs[RT, VT](definitions: Vector[ScopedDataType.DT[RT, VT]]): (
      Vector[ScopedDataType[Record[RT]]],
      Vector[ScopedDataType[Variant[VT]]],
      Vector[ScopedDataType[Enum]],
  ) = {

    val (recordAndVariants, enums) = definitions.partitionMap {
      case sdt @ ScopedDataType(_, _, ddt) =>
        ddt match {
          case r: Record[RT] =>
            Left(Left(sdt copy (dataType = r)))
          case v: Variant[VT] =>
            Left(Right(sdt copy (dataType = v)))
          case e: Enum =>
            Right(sdt copy (dataType = e))
        }
    }

    val (records, variants) = recordAndVariants.partitionMap(identity)

    (records, variants, enums)
  }

  /** Replace every VT that refers to some apparently-nominalized record
    * type in the argument list with the fields of that record, and drop
    * those records that arose from nominalization.
    *
    * The nature of each variant data constructor ("field") can be
    * figured by examining the _2: left means splatted, right means
    * unchanged.
    */
  private[this] def splatVariants[RT <: typesig.Type, VT <: typesig.Type](
      definitions: Vector[ScopedDataType.DT[RT, VT]]
  ): (
      Vector[ScopedDataType[Record[RT]]],
      Vector[ScopedDataType[Variant[List[(Ref.Name, RT)] \/ VT]]],
      Vector[ScopedDataType[Enum]],
  ) = {
    type VariantField = List[(Ref.Name, RT)] \/ VT

    val (records, variants, enums) = splitNTDs(definitions)

    val recordMap: Map[(ScopedDataType.Name, List[Ref.Name]), ScopedDataType[Record[RT]]] =
      records.view.map { case ddt @ ScopedDataType(name, vars, _) =>
        (name -> vars.toList) -> ddt
      }.toMap

    val noDeletion = Set.empty[(Identifier, List[Ref.Name])]
    val (deletedRecords, newVariants) =
      variants.traverse {
        case ScopedDataType(ident @ Identifier(packageId, qualName), vTypeVars, Variant(fields)) =>
          val typeVarDelegate = LFUtil simplyDelegates vTypeVars
          val (deleted, sdt) = fields.traverse { case (vn, vt) =>
            val syntheticRecord = Identifier(
              packageId,
              qualName copy (name =
                DottedName.assertFromSegments(qualName.name.segments.slowSnoc(vn).toSeq)),
            )
            val key = (syntheticRecord, vTypeVars.toList)
            typeVarDelegate(vt)
              .filter((_: Identifier) == syntheticRecord)
              .flatMap(_ => recordMap get key)
              .cata(
                nr => (Set(key), (vn, -\/(nr.dataType.fields.toList): VariantField)),
                (noDeletion, (vn, \/-(vt): VariantField)),
              )
          }
          (deleted, ScopedDataType(ident, vTypeVars, Variant(sdt)))
      }

    ((recordMap -- deletedRecords).valuesIterator.toVector, newVariants, enums)
  }

  private[this] def writeTemplatesAndTypes(
      util: LFUtil
  )(wp: WriteParams): Unit = {
    util.templateAndTypeFiles(wp).iterator.foreach {
      case -\/(msg) => logger.debug(msg)
      case \/-((msg, filePath, trees)) =>
        msg foreach (m => logger.debug(m))
        writeCode(filePath, trees)
    }
  }

  private def writeCode(filePath: File, trees: Iterable[Tree]): Unit =
    if (trees.nonEmpty) {
      filePath.getParentFile.mkdirs()
      val writer = new PrintWriter(filePath)
      try {
        writer.println(LFUtil.autoGenerationHeader)
        trees.foreach(tree => writer.println(showCode(tree)))
      } finally {
        writer.close()
      }
    } else {
      logger.warn(s"WARNING: nothing to generate, empty trees passed, file: $filePath")
    }
}
