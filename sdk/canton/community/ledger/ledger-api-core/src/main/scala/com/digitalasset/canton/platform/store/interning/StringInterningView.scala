// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.platform.store.interning

import com.digitalasset.daml.lf.data.Ref.PackageVersion
import com.digitalasset.canton.concurrent.DirectExecutionContext
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.platform.{Identifier, PackageName, Party}
import com.digitalasset.canton.topology.DomainId

import scala.concurrent.{Future, blocking}

class DomainStringIterators(
    val parties: Iterator[String],
    val templateIds: Iterator[String],
    val domainIds: Iterator[String],
    val packageNames: Iterator[String],
    val packageVersions: Iterator[String],
)

trait InternizingStringInterningView {

  /** Internize strings of different domains. The new entries are returend as prefixed entries for persistent storage.
    *
    * @param domainStringIterators iterators of the new entires
    * @return If some of the entries were not part of the view: they will be added, and these will be returned as a interned-id and raw, prefixed string pairs.
    *
    * @note This method is thread-safe.
    */
  def internize(domainStringIterators: DomainStringIterators): Iterable[(Int, String)]
}

trait UpdatingStringInterningView {

  /** Update the StringInterningView from persistence
    *
    * @param lastStringInterningId this is the "version" of the persistent view, which from the StringInterningView can see if it is behind
    * @return a completion Future:
    *
    * * if the view is behind, it will load the missing entries from persistence, and update the view state.
    *
    * * if the view is ahead, it will remove all entries with ids greater than the `lastStringInterningId`
    *
    * @note This method is NOT thread-safe and should not be called concurrently with itself or [[InternizingStringInterningView.internize]].
    */
  def update(lastStringInterningId: Int)(
      loadPrefixedEntries: LoadStringInterningEntries
  ): Future[Unit]
}

/** Encapsulate the dependency to load a range of string-interning-entries from persistence
  */
trait LoadStringInterningEntries {
  def apply(
      fromExclusive: Int,
      toInclusive: Int,
  ): Future[Iterable[(Int, String)]]
}

/** This uses the prefixed raw representation internally similar to the persistence layer.
  * Concurrent view usage is optimized for reading:
  * - The single, volatile reference enables non-synchronized access from all threads, accessing persistent-immutable datastructure
  * - On the writing side it synchronizes (this usage is anyway expected) and maintains the immutable internal datastructure
  */
class StringInterningView(override protected val loggerFactory: NamedLoggerFactory)
    extends StringInterning
    with InternizingStringInterningView
    with UpdatingStringInterningView
    with NamedLogging {

  private val directEc = DirectExecutionContext(noTracingLogger)

  @SuppressWarnings(Array("org.wartremover.warts.Var"))
  @volatile private var raw: RawStringInterning = RawStringInterning.from(Nil)

  private def rawAccessor: StringInterningAccessor[String] = new StringInterningAccessor[String] {
    override def internalize(t: String): Int = raw.map(t)
    override def tryInternalize(t: String): Option[Int] = raw.map.get(t)
    override def externalize(id: Int): String = raw.idMap(id)
    override def tryExternalize(id: Int): Option[String] = raw.idMap.get(id)
  }

  private val TemplatePrefix = "t|"
  private val PartyPrefix = "p|"
  private val DomainIdPrefix = "d|"
  private val PackageNamePrefix = "n|"
  private val PackageVersionPrefix = "v|"

  override val templateId: StringInterningDomain[Identifier] =
    StringInterningDomain.prefixing(
      prefix = TemplatePrefix,
      prefixedAccessor = rawAccessor,
      to = Identifier.assertFromString,
      from = _.toString,
    )

  override val party: StringInterningDomain[Party] =
    StringInterningDomain.prefixing(
      prefix = PartyPrefix,
      prefixedAccessor = rawAccessor,
      to = Party.assertFromString,
      from = _.toString,
    )

  override val domainId: StringInterningDomain[DomainId] =
    StringInterningDomain.prefixing(
      prefix = DomainIdPrefix,
      prefixedAccessor = rawAccessor,
      to = DomainId.tryFromString,
      from = _.toProtoPrimitive,
    )

  override val packageName: StringInterningDomain[PackageName] =
    StringInterningDomain.prefixing(
      prefix = PackageNamePrefix,
      prefixedAccessor = rawAccessor,
      to = PackageName.assertFromString,
      from = _.toString,
    )

  override val packageVersion: StringInterningDomain[PackageVersion] =
    StringInterningDomain.prefixing(
      prefix = PackageVersionPrefix,
      prefixedAccessor = rawAccessor,
      to = PackageVersion.assertFromString,
      from = _.toString(),
    )

  override def internize(domainStringIterators: DomainStringIterators): Iterable[(Int, String)] =
    blocking(synchronized {
      val allPrefixedStrings =
        domainStringIterators.parties.map(PartyPrefix + _) ++
          domainStringIterators.templateIds.map(TemplatePrefix + _) ++
          domainStringIterators.domainIds.map(DomainIdPrefix + _) ++
          domainStringIterators.packageNames.map(PackageNamePrefix + _) ++
          domainStringIterators.packageVersions.map(PackageVersionPrefix + _)

      val newEntries = RawStringInterning.newEntries(
        strings = allPrefixedStrings,
        rawStringInterning = raw,
      )
      updateView(newEntries)
      newEntries
    })

  override def update(lastStringInterningId: Int)(
      loadStringInterningEntries: LoadStringInterningEntries
  ): Future[Unit] =
    if (lastStringInterningId <= raw.lastId) {
      raw = RawStringInterning.resetTo(lastStringInterningId, raw)
      Future.unit
    } else {
      loadStringInterningEntries(raw.lastId, lastStringInterningId)
        .map(updateView)(directEc)
    }

  private def updateView(newEntries: Iterable[(Int, String)]): Unit = blocking(synchronized {
    if (newEntries.nonEmpty) {
      raw = RawStringInterning.from(
        entries = newEntries,
        rawStringInterning = raw,
      )
    }
  })
}
