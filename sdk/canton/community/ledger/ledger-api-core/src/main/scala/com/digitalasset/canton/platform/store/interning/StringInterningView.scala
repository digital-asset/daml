// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.platform.store.interning

import com.digitalasset.canton.concurrent.DirectExecutionContext
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.platform.Party
import com.digitalasset.canton.topology.SynchronizerId
import com.digitalasset.canton.util.Mutex
import com.digitalasset.daml.lf.data.Ref.{
  ChoiceName,
  Identifier,
  NameTypeConRef,
  PackageId,
  ParticipantId,
  UserId,
}

import scala.concurrent.Future

class DomainStringIterators(
    val parties: Iterator[Party],
    val templateIds: Iterator[NameTypeConRef],
    val synchronizerIds: Iterator[SynchronizerId],
    val packageIds: Iterator[PackageId],
    val userIds: Iterator[UserId],
    val participantIds: Iterator[ParticipantId],
    val choiceNames: Iterator[ChoiceName],
    val interfaceIds: Iterator[Identifier],
)

trait InternizingStringInterningView {

  /** Internize strings of different domains. The new entries are returend as prefixed entries for
    * persistent storage.
    *
    * @param domainStringIterators
    *   iterators of the new entires
    * @return
    *   If some of the entries were not part of the view: they will be added, and these will be
    *   returned as a interned-id and raw, prefixed string pairs.
    *
    * @note
    *   This method is thread-safe. This method should be called from Indexer, which maintains
    *   consistency between StringInterning view and persistence.
    */
  private[platform] def internize(
      domainStringIterators: DomainStringIterators
  ): Iterable[(Int, String)]
}

trait UpdatingStringInterningView {

  /** Update the StringInterningView from persistence
    *
    * @param lastStringInterningId
    *   this is the "version" of the persistent view, which from the StringInterningView can see if
    *   it is behind
    * @return
    *   a completion Future:
    *
    * * if the view is behind, it will load the missing entries from persistence, and update the
    * view state.
    *
    * * if the view is ahead, it will remove all entries with ids greater than the
    * `lastStringInterningId`
    *
    * @note
    *   This method is NOT thread-safe and should not be called concurrently with itself or
    *   InternizingStringInterningView.internize.
    */
  def update(lastStringInterningId: Option[Int])(
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
  *   - The single, volatile reference enables non-synchronized access from all threads, accessing
  *     persistent-immutable datastructure
  *   - On the writing side it synchronizes (this usage is anyway expected) and maintains the
  *     immutable internal datastructure
  */
class StringInterningView(override protected val loggerFactory: NamedLoggerFactory)
    extends StringInterning
    with InternizingStringInterningView
    with UpdatingStringInterningView
    with NamedLogging {

  private val directEc = DirectExecutionContext(noTracingLogger)
  private val lock = new Mutex()

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
  private val SynchronizerIdPrefix = "d|"
  private val PackageIdPrefix = "i|"
  private val UserIdPrefix = "u|"
  private val ParticipantIdPrefix = "n|"
  private val ChoicePrefix = "c|"
  private val InterfacePrefix = "f|"

  override val templateId: StringInterningDomain[NameTypeConRef] =
    StringInterningDomain.prefixing(
      prefix = TemplatePrefix,
      prefixedAccessor = rawAccessor,
      to = NameTypeConRef.assertFromString,
      from = _.toString,
    )

  override val party: StringInterningDomain[Party] =
    StringInterningDomain.prefixing(
      prefix = PartyPrefix,
      prefixedAccessor = rawAccessor,
      to = Party.assertFromString,
      from = identity,
    )

  override val synchronizerId: StringInterningDomain[SynchronizerId] =
    StringInterningDomain.prefixing(
      prefix = SynchronizerIdPrefix,
      prefixedAccessor = rawAccessor,
      to = SynchronizerId.tryFromString,
      from = _.toProtoPrimitive,
    )

  override val packageId: StringInterningDomain[PackageId] =
    StringInterningDomain.prefixing(
      prefix = PackageIdPrefix,
      prefixedAccessor = rawAccessor,
      to = PackageId.assertFromString,
      from = identity,
    )

  override val userId: StringInterningDomain[UserId] =
    StringInterningDomain.prefixing(
      prefix = UserIdPrefix,
      prefixedAccessor = rawAccessor,
      to = UserId.assertFromString,
      from = identity,
    )

  override def participantId: StringInterningDomain[ParticipantId] =
    StringInterningDomain.prefixing(
      prefix = ParticipantIdPrefix,
      prefixedAccessor = rawAccessor,
      to = ParticipantId.assertFromString,
      from = identity,
    )

  override val choiceName: StringInterningDomain[ChoiceName] =
    StringInterningDomain.prefixing(
      prefix = ChoicePrefix,
      prefixedAccessor = rawAccessor,
      to = ChoiceName.assertFromString,
      from = identity,
    )

  override val interfaceId: StringInterningDomain[Identifier] =
    StringInterningDomain.prefixing(
      prefix = InterfacePrefix,
      prefixedAccessor = rawAccessor,
      to = Identifier.assertFromString,
      from = _.toString,
    )

  override private[platform] def internize(
      domainStringIterators: DomainStringIterators
  ): Iterable[(Int, String)] =
    (lock.exclusive {
      val allPrefixedStrings =
        domainStringIterators.parties.map(PartyPrefix + _) ++
          domainStringIterators.templateIds.map(TemplatePrefix + _) ++
          domainStringIterators.synchronizerIds
            .map(_.toProtoPrimitive)
            .map(SynchronizerIdPrefix + _) ++
          domainStringIterators.packageIds.map(PackageIdPrefix + _) ++
          domainStringIterators.userIds.map(UserIdPrefix + _) ++
          domainStringIterators.participantIds.map(ParticipantIdPrefix + _) ++
          domainStringIterators.choiceNames.map(ChoicePrefix + _) ++
          domainStringIterators.interfaceIds.map(InterfacePrefix + _)

      val newEntries = RawStringInterning.newEntries(
        strings = allPrefixedStrings,
        rawStringInterning = raw,
      )
      updateView(newEntries)
      newEntries
    })

  override def update(lastStringInterningId: Option[Int])(
      loadStringInterningEntries: LoadStringInterningEntries
  ): Future[Unit] =
    if (lastStringInterningId.getOrElse(0) <= raw.lastId) {
      raw = RawStringInterning.resetTo(lastStringInterningId.getOrElse(0), raw)
      Future.unit
    } else {
      loadStringInterningEntries(raw.lastId, lastStringInterningId.getOrElse(0))
        .map(updateView)(directEc)
    }

  private def updateView(newEntries: Iterable[(Int, String)]): Unit = (lock.exclusive {
    if (newEntries.nonEmpty) {
      raw = RawStringInterning.from(
        entries = newEntries,
        rawStringInterning = raw,
      )
    }
  })
}
