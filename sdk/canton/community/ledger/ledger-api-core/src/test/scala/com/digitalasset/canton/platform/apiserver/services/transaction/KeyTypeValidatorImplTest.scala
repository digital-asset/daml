// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.platform.apiserver.services.transaction

import com.daml.daml_lf_dev.DamlLf
import com.daml.lf.archive
import com.daml.lf.archive.Dar
import com.daml.lf.crypto.Hash.KeyPackageName
import com.daml.lf.data.Ref.{PackageId, QualifiedName}
import com.daml.lf.data.{ImmArray, Ref}
import com.daml.lf.engine.Engine
import com.daml.lf.transaction.GlobalKey
import com.daml.lf.typesig.reader.SignatureReader
import com.daml.lf.value.Value
import com.daml.nonempty.NonEmpty
import com.digitalasset.canton.ledger.api.domain.{LedgerOffset, PackageEntry}
import com.digitalasset.canton.ledger.participant.state.index.v2.{
  IndexPackagesService,
  PackageDetails,
}
import com.digitalasset.canton.logging.LoggingContextWithTrace
import com.digitalasset.canton.metrics.Metrics
import com.digitalasset.canton.platform.localstore.{PackageMetadataSnapshot, PackageMetadataStore}
import com.digitalasset.canton.platform.store.packagemeta.PackageMetadata
import com.digitalasset.canton.platform.store.packagemeta.PackageMetadata.{
  LocalPackagePreference,
  PackageResolution,
}
import com.digitalasset.canton.{BaseTest, HasExecutorService}
import org.apache.pekko.NotUsed
import org.apache.pekko.stream.scaladsl.Source
import org.scalatest.wordspec.AsyncWordSpecLike

import java.io.File
import scala.concurrent.{ExecutionContext, Future}

class KeyTypeValidatorImplTest extends AsyncWordSpecLike with BaseTest with HasExecutorService {

  implicit val ec: ExecutionContext = executorService

  val engine: Engine = new Engine()

  lazy private val dar: Dar[DamlLf.Archive] =
    archive.DarParser.assertReadArchiveFromFile(new File(CantonExamplesPath))

  lazy private val mainPackage = SignatureReader.readPackageSignature(dar.main)._2

  lazy private val packages = dar.all.map { a =>
    val (_, sig) = SignatureReader.readPackageSignature(a)
    sig.packageId -> a
  }.toMap

  val packagesService: IndexPackagesService = new IndexPackagesService {
    override def listLfPackages()(implicit
        loggingContext: LoggingContextWithTrace
    ): Future[Map[PackageId, PackageDetails]] = Future.successful(Map.empty)
    override def getLfArchive(packageId: PackageId)(implicit
        loggingContext: LoggingContextWithTrace
    ): Future[Option[DamlLf.Archive]] = Future.successful(packages.get(packageId))
    override def packageEntries(startExclusive: Option[LedgerOffset.Absolute])(implicit
        loggingContext: LoggingContextWithTrace
    ): Source[PackageEntry, NotUsed] = Source.empty
  }

  val packageMetadataStore: PackageMetadataStore = new PackageMetadataStore {
    override def getSnapshot: PackageMetadataSnapshot = new PackageMetadataSnapshot(
      PackageMetadata(packageNameMap =
        mainPackage.metadata
          .map { m =>
            m.name -> PackageResolution(
              preference = LocalPackagePreference(m.version, mainPackage.packageId),
              allPackageIdsForName = NonEmpty(Set, mainPackage.packageId),
            )
          }
          .toList
          .toMap
      )
    )
  }

  private val qualifiedName = QualifiedName.assertFromString("ContractKeys:Keyed");

  private val packageName = mainPackage.metadata.value.name
  private val packageRefName: Ref.PackageRef = Ref.PackageRef.Name(packageName)
  private val packageRefId: Ref.PackageRef = Ref.PackageRef.Id(mainPackage.packageId)

  // Type is tuple to field names are _ prefixed
  private val f1 = (
    Some(Ref.Name.assertFromString("_1")),
    Value.ValueParty(Ref.Party.assertFromString("partyA")),
  ) // sig
  private val f2 = (Some(Ref.Name.assertFromString("_2")), Value.ValueInt64(7)) // k

  private val wellTyped = Value.ValueRecord(None, ImmArray.from(Seq(f1, f2)))
  private val unTyped = Value.ValueRecord(None, ImmArray.from(Seq(f2, f1)))
  private val invalidType = Value.ValueRecord(None, ImmArray.from(Seq(f1)))
  private val expectedV = Value.ValueRecord(None, ImmArray.from(Seq(f1, f2).map(t => (None, t._2))))

  private val expected = GlobalKey.assertBuild(
    Ref.Identifier(mainPackage.packageId, qualifiedName),
    expectedV,
    KeyPackageName.assertBuild(Some(packageName), mainPackage.languageVersion),
  )

  private val underTest =
    new KeyTypeValidatorImpl(engine, packagesService, packageMetadataStore, Metrics.ForTesting)

  private implicit val lc: LoggingContextWithTrace = LoggingContextWithTrace.ForTesting

  "KeyValidator" should {

    "verify type correctness with well typed key" in {
      val ref = Ref.TypeConRef(packageRefId, qualifiedName)
      underTest.apply(ref, wellTyped, lc).map(_.value shouldBe expected)
    }
    "verify type correctness with package by id" in {
      val ref = Ref.TypeConRef(packageRefId, qualifiedName)
      underTest.apply(ref, unTyped, lc).map(_.value shouldBe expected)
    }
    "verify type correctness with package by name" in {
      val ref = Ref.TypeConRef(packageRefName, qualifiedName)
      underTest.apply(ref, unTyped, lc).map(_.value shouldBe expected)
    }
    "fail if package id cannot be resolved" in {
      val ref =
        Ref.TypeConRef(Ref.PackageRef.Id(Ref.PackageId.assertFromString("badPkgId")), qualifiedName)
      underTest
        .apply(ref, unTyped, lc)
        .map(_.swap.value should include.regex("MissingPackage.*badPkgId"))
    }
    "fail if package name cannot be resolved" in {
      val ref = Ref.TypeConRef(
        Ref.PackageRef.Name(Ref.PackageName.assertFromString("badPkgName")),
        qualifiedName,
      )
      underTest
        .apply(ref, unTyped, lc)
        .map(_.swap.value should include.regex("Could not resolve package.*badPkgName"))
    }
    "fail on unknown typename" in {
      val ref = Ref.TypeConRef(packageRefName, QualifiedName.assertFromString("badMod:badType"))
      underTest
        .apply(ref, unTyped, lc)
        .map(_.swap.value should include.regex("NotFound.*badMod:badType"))
    }
    "fail on invalid type construction" in {
      val ref = Ref.TypeConRef(packageRefName, qualifiedName)
      underTest
        .apply(ref, invalidType, lc)
        .map(
          _.swap.value should include.regex(
            "Expecting 2 field for record .*:DA.Types:Tuple2, but got 1"
          )
        )
    }
  }

}
