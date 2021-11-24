// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.participant.state.kvutils.committer.transaction

import java.time
import java.time.Duration

import com.codahale.metrics.MetricRegistry
import com.daml.ledger.configuration.Configuration
import com.daml.ledger.participant.state.kvutils.Conversions.{
  buildDuration,
  buildTimestamp,
  parseTimestamp,
}
import com.daml.ledger.participant.state.kvutils.TestHelpers._
import com.daml.ledger.participant.state.kvutils.committer.{CommitContext, StepContinue, StepStop}
import com.daml.ledger.participant.state.kvutils.store.events.{
  DamlConfigurationEntry,
  DamlSubmitterInfo,
  DamlTransactionRejectionEntry,
}
import com.daml.ledger.participant.state.kvutils.store.{
  DamlCommandDedupValue,
  DamlStateValue,
  PreExecutionDeduplicationBounds,
}
import com.daml.ledger.participant.state.kvutils.{Conversions, Err}
import com.daml.lf.data.Time.Timestamp
import com.daml.logging.LoggingContext
import com.daml.metrics.Metrics
import com.google.protobuf
import org.mockito.MockitoSugar
import org.scalatest.OptionValues
import org.scalatest.matchers.should.Matchers
import org.scalatest.prop.TableDrivenPropertyChecks._
import org.scalatest.wordspec.AnyWordSpec

import scala.annotation.nowarn

@nowarn("msg=deprecated")
class CommandDeduplicationSpec
    extends AnyWordSpec
    with Matchers
    with MockitoSugar
    with OptionValues {
  import CommandDeduplicationSpec._

  private implicit val loggingContext: LoggingContext = LoggingContext.ForTesting

  private val metrics = new Metrics(new MetricRegistry)
  private val rejections = new Rejections(metrics)
  private val deduplicateCommandStep = CommandDeduplication.deduplicateCommandStep(rejections)
  private val setDeduplicationEntryStep =
    CommandDeduplication.setDeduplicationEntryStep()
  private val timestamp: Timestamp = Timestamp.now()

  "deduplicateCommand" should {
    Map(
      "pre-execution" -> ((dedupValueBuilder: Timestamp => Option[DamlStateValue]) => {
        val dedupValue = dedupValueBuilder(timestamp)
        val commitContext = createCommitContext(None, Map(aDedupKey -> dedupValue))
        commitContext.minimumRecordTime = Some(timestamp)
        commitContext.maximumRecordTime = Some(Timestamp.Epoch)
        timestamp -> commitContext
      }),
      "normal-execution" -> ((dedupValueBuilder: Timestamp => Option[DamlStateValue]) => {
        val dedupValue = dedupValueBuilder(timestamp)
        val commitContext = createCommitContext(Some(timestamp), Map(aDedupKey -> dedupValue))
        timestamp -> commitContext
      }),
    ).foreach { case (key, contextBuilder) =>
      key should {
        "continue if no deduplication entry could be found" in {
          val (_, context) = contextBuilder(_ => None)

          deduplicationStepContinues(context)
        }

        "continue if deduplication entry has no value set" in {
          val (_, context) = contextBuilder(_ => Some(newDedupValue(identity)))

          deduplicationStepContinues(context)
        }

        "using deduplicate until" should {

          "continue if record time is after deduplication time in case a deduplication entry is found" in {
            val (_, context) = contextBuilder(timestamp =>
              Some(
                newDedupValue(
                  _.setDeduplicatedUntil(buildTimestamp(timestamp.subtract(Duration.ofSeconds(1))))
                )
              )
            )

            deduplicationStepContinues(context)
          }

          "produce rejection log entry in case record time is on or before deduplication time" in {
            for (
              durationToAdd <- Iterable(
                Duration.ZERO,
                Duration.ofSeconds(1),
              )
            ) {
              val (_, context) = contextBuilder(timestamp =>
                Some(
                  newDedupValue(
                    _.setDeduplicatedUntil(buildTimestamp(timestamp.add(durationToAdd)))
                  )
                )
              )
              deduplicateStepHasTransactionRejectionEntry(context)
            }
          }
        }

        "using record time" should {
          forAll(
            Table(
              "identifier" -> "time setter",
              "record time" -> ((timestamp: Timestamp) =>
                (builder: DamlCommandDedupValue.Builder) =>
                  builder.setRecordTime(buildTimestamp(timestamp))
              ),
              "record time bounds" -> ((timestamp: Timestamp) =>
                (builder: DamlCommandDedupValue.Builder) =>
                  builder.setRecordTimeBounds(
                    buildPreExecutionDeduplicationBounds(
                      timestamp.subtract(deduplicationDuration),
                      timestamp,
                    )
                  )
              ),
            )
          ) { case (identifier, timeSetter) =>
            identifier should {
              "continue if record time is after deduplication time in case a deduplication entry is found" in {
                val (_, context) = contextBuilder(timestamp =>
                  Some(
                    newDedupValue(
                      timeSetter(timestamp.subtract(deduplicationDuration.plusMillis(1)))
                    )
                  )
                )

                deduplicationStepContinues(context)
              }

              "produce rejection log entry in case transaction timestamp is on or before deduplication time" in {
                for (
                  durationToSubstractFromDeduplicationDuration <- Iterable(
                    Duration.ZERO,
                    Duration.ofSeconds(1),
                  )
                ) {
                  val (_, context) = contextBuilder(timestamp =>
                    Some(
                      newDedupValue(
                        timeSetter(
                          timestamp.subtract(
                            deduplicationDuration.minus(
                              durationToSubstractFromDeduplicationDuration
                            )
                          )
                        )
                      )
                    )
                  )
                  deduplicateStepHasTransactionRejectionEntry(context)
                }
              }

              "include the submission id in the rejection" in {
                val submissionId = "submissionId"
                val (_, context) = contextBuilder(timestamp =>
                  Some(
                    newDedupValue(builder =>
                      timeSetter(timestamp)(builder.setSubmissionId(submissionId))
                    )
                  )
                )
                val rejection = deduplicateStepHasTransactionRejectionEntry(context)
                rejection.getDuplicateCommand.getSubmissionId shouldBe submissionId
              }

            }
          }
        }
      }
    }

    "using pre-execution" should {

      "produce rejection log entry when there's an overlap between previous transaction max-record-time and current transaction min-record-time" in {
        val dedupValue = newDedupValue(
          _.setRecordTimeBounds(buildPreExecutionDeduplicationBounds(timestamp, timestamp))
        )
        val commitContext = createCommitContext(None, Map(aDedupKey -> Some(dedupValue)))
        commitContext.minimumRecordTime = Some(timestamp.subtract(Duration.ofMillis(1)))
        commitContext.maximumRecordTime = Some(Timestamp.Epoch)

        deduplicateStepHasTransactionRejectionEntry(commitContext)
      }

      "set the out of time bounds log entry during rejections" in {
        val dedupValue = newDedupValue(
          _.setRecordTimeBounds(buildPreExecutionDeduplicationBounds(timestamp, timestamp))
        )
        val commitContext = createCommitContext(None, Map(aDedupKey -> Some(dedupValue)))
        commitContext.minimumRecordTime = Some(timestamp)
        commitContext.maximumRecordTime = Some(timestamp)

        deduplicateStepHasTransactionRejectionEntry(commitContext)
        commitContext.outOfTimeBoundsLogEntry shouldBe 'defined
      }

      "return the command deduplication duration as deduplication duration when this exceeds the max delta between record times" in {
        val dedupValue = newDedupValue(
          _.setRecordTimeBounds(buildPreExecutionDeduplicationBounds(timestamp, timestamp))
        )
        val commitContext = createCommitContext(None, Map(aDedupKey -> Some(dedupValue)))
        commitContext.minimumRecordTime = Some(timestamp)
        commitContext.maximumRecordTime = Some(timestamp.add(Duration.ofSeconds(1)))

        val rejectionEntry = deduplicateStepHasTransactionRejectionEntry(commitContext)
        rejectionEntry.getSubmitterInfo.getDeduplicationDuration shouldBe buildDuration(
          deduplicationDuration
        )
      }

      "the deduplication duration is the delta between records when this exceeds the deduplication duration sent in the command" in {
        val dedupValue = newDedupValue(
          _.setRecordTimeBounds(
            buildPreExecutionDeduplicationBounds(timestamp, timestamp.add(Duration.ofSeconds(1)))
          )
        )
        val commitContext = createCommitContext(None, Map(aDedupKey -> Some(dedupValue)))
        val deltaBetweenRecords = deduplicationDuration.plusMillis(1)
        commitContext.minimumRecordTime = Some(timestamp)
        commitContext.maximumRecordTime = Some(timestamp.add(deltaBetweenRecords))

        val rejectionEntry = deduplicateStepHasTransactionRejectionEntry(commitContext)
        rejectionEntry.getSubmitterInfo.getDeduplicationDuration shouldBe buildDuration(
          deltaBetweenRecords
        )
      }

    }
  }

  "setting dedup context" should {
    val deduplicateUntil = protobuf.Timestamp.newBuilder().setSeconds(30).build()
    val submissionTime = protobuf.Timestamp.newBuilder().setSeconds(60).build()
    val deduplicationDuration = time.Duration.ofSeconds(3)

    "set the time bounds for deduplication based on the committer context values" in {
      val (context, transactionEntrySummary) =
        buildContextAndTransaction(
          submissionTime,
          _.setDeduplicationDuration(Conversions.buildDuration(deduplicationDuration)),
        )
      val minimumRecordTime = timestamp
      val maximumRecordTime = minimumRecordTime.subtract(Duration.ofSeconds(1))
      context.maximumRecordTime = Some(maximumRecordTime)
      context.minimumRecordTime = Some(minimumRecordTime)
      setDeduplicationEntryStep(context, transactionEntrySummary)
      val setTimeBounds = deduplicateValueStoredInContext(context, transactionEntrySummary)
        .map(
          _.getRecordTimeBounds
        )
        .value
      setTimeBounds shouldBe PreExecutionDeduplicationBounds
        .newBuilder()
        .setMinRecordTime(buildTimestamp(minimumRecordTime))
        .setMaxRecordTime(buildTimestamp(maximumRecordTime))
        .build()
    }

    "set the record time in the committer context values" in {
      val recordTime = timestamp
      val (context, transactionEntrySummary) =
        buildContextAndTransaction(
          submissionTime,
          _.setDeduplicationDuration(Conversions.buildDuration(deduplicationDuration)),
          Some(recordTime),
        )
      context.maximumRecordTime = Some(recordTime)
      setDeduplicationEntryStep(context, transactionEntrySummary)
      parseTimestamp(
        deduplicateValueStoredInContext(context, transactionEntrySummary)
          .map(
            _.getRecordTime
          )
          .value
      ) shouldBe recordTime
    }

    "set the submission id in the dedup value" in {
      val submissionId = "submissionId"
      val (context, transactionEntrySummary) =
        buildContextAndTransaction(
          submissionTime,
          _.setDeduplicationDuration(Conversions.buildDuration(deduplicationDuration))
            .setSubmissionId(submissionId),
          Some(timestamp),
        )
      setDeduplicationEntryStep(context, transactionEntrySummary)
      deduplicateValueStoredInContext(context, transactionEntrySummary)
        .map(
          _.getSubmissionId
        )
        .value shouldBe submissionId
    }

    "throw an error for missing record time bounds" in {
      val (context, transactionEntrySummary) =
        buildContextAndTransaction(
          submissionTime,
          _.setDeduplicationDuration(Conversions.buildDuration(deduplicationDuration)),
        )
      context.minimumRecordTime = Some(timestamp)
      a[Err.InternalError] shouldBe thrownBy(
        setDeduplicationEntryStep(context, transactionEntrySummary)
      )
      context.maximumRecordTime = Some(timestamp)
      context.minimumRecordTime = None
      a[Err.InternalError] shouldBe thrownBy(
        setDeduplicationEntryStep(context, transactionEntrySummary)
      )
    }

    "throw an error for unsupported deduplication periods" in {
      forAll(
        Table[DamlSubmitterInfo.Builder => DamlSubmitterInfo.Builder](
          "deduplication setter",
          _.clearDeduplicationPeriod(),
          _.setDeduplicationOffset("offset"),
          _.setDeduplicateUntil(deduplicateUntil),
        )
      ) { deduplicationSetter =>
        {
          val (context, transactionEntrySummary) =
            buildContextAndTransaction(submissionTime, deduplicationSetter)
          a[Err.InvalidSubmission] shouldBe thrownBy(
            setDeduplicationEntryStep(context, transactionEntrySummary)
          )
        }
      }
    }
  }

  private def deduplicateStepHasTransactionRejectionEntry(
      context: CommitContext
  ): DamlTransactionRejectionEntry = {
    val actual = deduplicateCommandStep(context, aTransactionEntrySummary)
    actual match {
      case StepContinue(_) => fail()
      case StepStop(actualLogEntry) =>
        actualLogEntry.hasTransactionRejectionEntry shouldBe true
        actualLogEntry.getTransactionRejectionEntry
    }
  }

  private def deduplicationStepContinues(context: CommitContext) = {
    val actual = deduplicateCommandStep(context, aTransactionEntrySummary)

    actual match {
      case StepContinue(_) => succeed
      case StepStop(_) => fail()
    }
  }
}

object CommandDeduplicationSpec {

  private val deduplicationDuration = Duration.ofSeconds(3)
  private val aDamlTransactionEntry = {
    val transaction = createEmptyTransactionEntry(List("aSubmitter"))
    transaction.toBuilder
      .setSubmitterInfo(
        transaction.getSubmitterInfo.toBuilder
          .setDeduplicationDuration(buildDuration(deduplicationDuration))
      )
      .build()
  }
  private val aTransactionEntrySummary = DamlTransactionEntrySummary(aDamlTransactionEntry)
  private val aDedupKey = Conversions
    .commandDedupKey(aTransactionEntrySummary.submitterInfo)
  private val aDamlConfigurationStateValue = DamlStateValue.newBuilder
    .setConfigurationEntry(
      DamlConfigurationEntry.newBuilder
        .setConfiguration(Configuration.encode(theDefaultConfig))
    )
    .build

  private def buildPreExecutionDeduplicationBounds(
      minRecordTime: Timestamp,
      maxRecordTime: Timestamp,
  ) = PreExecutionDeduplicationBounds
    .newBuilder()
    .setMaxRecordTime(buildTimestamp(maxRecordTime))
    .setMinRecordTime(buildTimestamp(minRecordTime))

  private def buildContextAndTransaction(
      submissionTime: protobuf.Timestamp,
      submitterInfoAugmenter: DamlSubmitterInfo.Builder => DamlSubmitterInfo.Builder,
      recordTime: Option[Timestamp] = None,
  ) = {
    val context = createCommitContext(recordTime)
    context.set(Conversions.configurationStateKey, aDamlConfigurationStateValue)
    val transactionEntrySummary = DamlTransactionEntrySummary(
      aDamlTransactionEntry.toBuilder
        .setSubmitterInfo(
          submitterInfoAugmenter(
            aDamlTransactionEntry.getSubmitterInfo.toBuilder
          )
        )
        .setSubmissionTime(submissionTime)
        .build()
    )
    context -> transactionEntrySummary
  }

  private def deduplicateValueStoredInContext(
      context: CommitContext,
      transactionEntrySummary: DamlTransactionEntrySummary,
  ) = context
    .get(Conversions.commandDedupKey(transactionEntrySummary.submitterInfo))
    .map(
      _.getCommandDedup
    )

  private def newDedupValue(
      valueBuilder: DamlCommandDedupValue.Builder => DamlCommandDedupValue.Builder
  ): DamlStateValue =
    DamlStateValue.newBuilder
      .setCommandDedup(
        valueBuilder(DamlCommandDedupValue.newBuilder)
      )
      .build
}
