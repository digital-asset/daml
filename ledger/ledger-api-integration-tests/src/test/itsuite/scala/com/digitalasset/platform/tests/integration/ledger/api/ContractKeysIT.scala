package com.digitalasset.platform.tests.integration.ledger.api

import java.util.UUID

import com.digitalasset.daml.lf.language.{
  LanguageVersion,
  LanguageMajorVersion => LMajV,
  LanguageMinorVersion => LMinV
}
import com.digitalasset.ledger.api.testing.utils.{
  AkkaBeforeAndAfterAll,
  SuiteResourceManagementAroundEach
}
import com.digitalasset.ledger.api.v1.value.{Optional, Record, RecordField, Value}
import com.digitalasset.platform.apitesting.LedgerContextExtensions._
import com.digitalasset.platform.apitesting.TestParties._
import com.digitalasset.platform.apitesting.{MultiLedgerFixture, TestIdsGenerator, TestTemplateIds}
import com.digitalasset.platform.participant.util.ValueConversions._
import com.digitalasset.daml.lf.transaction.VersionTimeline.Implicits._
import com.google.rpc.code.Code
import org.scalatest.concurrent.{AsyncTimeLimitedTests, ScalaFutures}
import org.scalatest.{AsyncWordSpec, Matchers, OptionValues}

class ContractKeysIT
    extends AsyncWordSpec
    with AkkaBeforeAndAfterAll
    with MultiLedgerFixture
    with SuiteResourceManagementAroundEach
    with ScalaFutures
    with AsyncTimeLimitedTests
    with Matchers
    with OptionValues {

  protected val testTemplateIds = new TestTemplateIds(config)
  protected val templateIds = testTemplateIds.templateIds
  protected val testIdsGenerator = new TestIdsGenerator(config)

  private lazy val dummyTemplates =
    List(templateIds.dummy, templateIds.dummyFactory, templateIds.dummyWithParam)
  private val operator = Grace
  private val receiver = Heidi
  private val giver = Alice
  private val owner = Bob
  private val delegate = Charlie
  private val observers = List(Eve, Frank)

  private val emptyRecordValue = Value(Value.Sum.Record(Record()))

  /** This should be the same version as the dar version. */
  protected val languageVersion: LanguageVersion = LanguageVersion(LMajV.V1, LMinV.Dev)

  override protected def config: Config = Config.default

  "Working with contract keys" should {

    "permit fetching a divulged contract" in allFixtures { ctx =>
      def pf(label: String, party: String) =
        RecordField(label, Some(Value(Value.Sum.Party(party))))

      // TODO currently we run multiple suites with the same sandbox, therefore we must generate
      // unique keys. This is not so great though, it'd be better to have a clean environment.
      val key = s"${UUID.randomUUID.toString}-key"
      val odArgs = Seq(
        pf("owner", owner),
        pf("delegate", delegate)
      )
      val delegatedCreate = ctx.testingHelpers.simpleCreate(
        testIdsGenerator.testCommandId("SDVl3"),
        owner,
        templateIds.delegated,
        Record(
          Some(templateIds.delegated),
          Seq(pf("owner", owner), RecordField(value = Some(Value(Value.Sum.Text(key))))))
      )
      val delegationCreate = ctx.testingHelpers.simpleCreate(
        testIdsGenerator.testCommandId("SDVl4"),
        owner,
        templateIds.delegation,
        Record(Some(templateIds.delegation), odArgs))
      val showIdCreate = ctx.testingHelpers.simpleCreate(
        testIdsGenerator.testCommandId("SDVl5"),
        owner,
        templateIds.showDelegated,
        Record(Some(templateIds.showDelegated), odArgs))
      for {
        delegatedEv <- delegatedCreate
        delegationEv <- delegationCreate
        showIdEv <- showIdCreate
        fetchArg = Record(
          None,
          Seq(RecordField("", Some(Value(Value.Sum.ContractId(delegatedEv.contractId))))))
        lookupArg = (expected: Option[String]) =>
          Record(
            None,
            Seq(
              pf("", owner),
              RecordField(value = Some(Value(Value.Sum.Text(key)))),
              RecordField(value = expected match {
                case None => Value(Value.Sum.Optional(Optional(None)))
                case Some(cid) => Value(Value.Sum.Optional(Optional(Some(cid.asContractId))))
              })
            )
        )
        _ <- ctx.testingHelpers.simpleExercise(
          testIdsGenerator.testCommandId("SDVl6"),
          submitter = owner,
          template = templateIds.showDelegated,
          contractId = showIdEv.contractId,
          choice = "ShowIt",
          arg = Value(Value.Sum.Record(fetchArg)),
        )
        _ <- ctx.testingHelpers.simpleExercise(
          testIdsGenerator.testCommandId("SDVl7"),
          submitter = delegate,
          template = templateIds.delegation,
          contractId = delegationEv.contractId,
          choice = "FetchDelegated",
          arg = Value(Value.Sum.Record(fetchArg)),
        )
        _ <- if (languageVersion precedes LanguageVersion.checkSubmitterInMaintainers) {
          ctx.testingHelpers.simpleExercise(
            testIdsGenerator.testCommandId("SDVl8"),
            submitter = delegate,
            template = templateIds.delegation,
            contractId = delegationEv.contractId,
            choice = "FetchByKeyDelegated",
            arg = Value(Value.Sum.Record(lookupArg(Some(delegatedEv.contractId)))),
          )
        } else {
          ctx.testingHelpers.failingExercise(
            testIdsGenerator.testCommandId("SDVl8"),
            submitter = delegate,
            template = templateIds.delegation,
            contractId = delegationEv.contractId,
            choice = "FetchByKeyDelegated",
            arg = Value(Value.Sum.Record(lookupArg(Some(delegatedEv.contractId)))),
            Code.INVALID_ARGUMENT,
            "Expected the submitter 'Charlie' to be in maintainers 'Bob'",
          )
        }
        _ <- if (languageVersion precedes LanguageVersion.checkSubmitterInMaintainers) {
          ctx.testingHelpers.simpleExercise(
            testIdsGenerator.testCommandId("SDVl9"),
            submitter = delegate,
            template = templateIds.delegation,
            contractId = delegationEv.contractId,
            choice = "LookupByKeyDelegated",
            arg = Value(Value.Sum.Record(lookupArg(Some(delegatedEv.contractId)))),
          )
        } else {
          ctx.testingHelpers.failingExercise(
            testIdsGenerator.testCommandId("SDVl9"),
            submitter = delegate,
            template = templateIds.delegation,
            contractId = delegationEv.contractId,
            choice = "LookupByKeyDelegated",
            arg = Value(Value.Sum.Record(lookupArg(Some(delegatedEv.contractId)))),
            Code.INVALID_ARGUMENT,
            "Expected the submitter 'Charlie' to be in maintainers 'Bob'",
          )
        }
      } yield (succeed)
    }

    "reject fetching an undisclosed contract" in allFixtures { ctx =>
      def pf(label: String, party: String) =
        RecordField(label, Some(Value(Value.Sum.Party(party))))

      // TODO currently we run multiple suites with the same sandbox, therefore we must generate
      // unique keys. This is not so great though, it'd be better to have a clean environment.
      val key = s"${UUID.randomUUID.toString}-key"
      val delegatedCreate = ctx.testingHelpers.simpleCreate(
        testIdsGenerator.testCommandId("TDVl3"),
        owner,
        templateIds.delegated,
        Record(
          Some(templateIds.delegated),
          Seq(pf("owner", owner), RecordField(value = Some(Value(Value.Sum.Text(key))))))
      )
      val delegationCreate = ctx.testingHelpers.simpleCreate(
        testIdsGenerator.testCommandId("TDVl4"),
        owner,
        templateIds.delegation,
        Record(Some(templateIds.delegation), Seq(pf("owner", owner), pf("delegate", delegate)))
      )
      for {
        delegatedEv <- delegatedCreate
        delegationEv <- delegationCreate
        fetchArg = Record(
          None,
          Seq(RecordField("", Some(Value(Value.Sum.ContractId(delegatedEv.contractId))))))
        lookupArg = (expected: Option[String]) =>
          Record(
            None,
            Seq(
              pf("", owner),
              RecordField(value = Some(Value(Value.Sum.Text(key)))),
              RecordField(value = expected match {
                case None => Value(Value.Sum.Optional(Optional(None)))
                case Some(cid) => Value(Value.Sum.Optional(Optional(Some(cid.asContractId))))
              }),
            )
        )
        fetchResult <- ctx.testingHelpers.failingExercise(
          testIdsGenerator.testCommandId("TDVl5"),
          submitter = delegate,
          template = templateIds.delegation,
          contractId = delegationEv.contractId,
          choice = "FetchDelegated",
          arg = Value(Value.Sum.Record(fetchArg)),
          Code.INVALID_ARGUMENT,
          pattern = "dependency error: couldn't find contract"
        )
        // this fetch still fails even if we do not check that the submitter
        // is in the lookup maintainer, since we have the visibility check
        // implement as part of #753.
        _ <- if (languageVersion precedes LanguageVersion.checkSubmitterInMaintainers) {
          ctx.testingHelpers.failingExercise(
            testIdsGenerator.testCommandId("TDVl6"),
            submitter = delegate,
            template = templateIds.delegation,
            contractId = delegationEv.contractId,
            choice = "FetchByKeyDelegated",
            arg = Value(Value.Sum.Record(lookupArg(None))),
            Code.INVALID_ARGUMENT,
            "couldn't find key",
          )
        } else {
          ctx.testingHelpers.failingExercise(
            testIdsGenerator.testCommandId("TDVl6"),
            submitter = delegate,
            template = templateIds.delegation,
            contractId = delegationEv.contractId,
            choice = "FetchByKeyDelegated",
            arg = Value(Value.Sum.Record(lookupArg(None))),
            Code.INVALID_ARGUMENT,
            "Expected the submitter 'Charlie' to be in maintainers 'Bob'",
          )
        }
        _ <- if (languageVersion precedes LanguageVersion.checkSubmitterInMaintainers) {
          ctx.testingHelpers.simpleExercise(
            testIdsGenerator.testCommandId("TDVl7"),
            submitter = delegate,
            template = templateIds.delegation,
            contractId = delegationEv.contractId,
            choice = "LookupByKeyDelegated",
            arg = Value(Value.Sum.Record(lookupArg(None))),
          )
        } else {
          ctx.testingHelpers.failingExercise(
            testIdsGenerator.testCommandId("TDVl7"),
            submitter = delegate,
            template = templateIds.delegation,
            contractId = delegationEv.contractId,
            choice = "LookupByKeyDelegated",
            arg = Value(Value.Sum.Record(lookupArg(None))),
            Code.INVALID_ARGUMENT,
            "Expected the submitter 'Charlie' to be in maintainers 'Bob'",
          )
        }
      } yield (succeed)
    }

    //      // this is basically a port of
    //      // `daml-lf/tests/scenario/daml-1.3/contract-keys/Test.daml`.
    "process contract keys" in allFixtures { ctx =>
      // TODO currently we run multiple suites with the same sandbox, therefore we must generate
      // unique keys. This is not so great though, it'd be better to have a clean environment.
      val keyPrefix = UUID.randomUUID.toString

      def textKeyRecord(p: String, k: String, disclosedTo: List[String]): Record =
        Record(
          fields = List(
            RecordField(value = p.asParty),
            RecordField(value = s"$keyPrefix-$k".asText),
            RecordField(value = disclosedTo.map(_.asParty).asList)))

      val key = "some-key"

      def textKeyKey(p: String, k: String): Value =
        Value(
          Value.Sum.Record(Record(fields =
            List(RecordField(value = p.asParty), RecordField(value = s"$keyPrefix-$k".asText)))))

      for {
        cid1 <- ctx.testingHelpers.simpleCreate(
          testIdsGenerator.testCommandId("CK-test-cid1"),
          Alice,
          templateIds.textKey,
          textKeyRecord(Alice, key, List(Bob))
        )
        // duplicate keys are not ok
        _ <- ctx.testingHelpers.failingCreate(
          testIdsGenerator.testCommandId("CK-test-duplicate-key"),
          Alice,
          templateIds.textKey,
          textKeyRecord(Alice, key, List(Bob)),
          Code.INVALID_ARGUMENT,
          "DuplicateKey"
        )
        // create handles to perform lookups / fetches
        aliceTKO <- ctx.testingHelpers.simpleCreate(
          testIdsGenerator.testCommandId("CK-test-aliceTKO"),
          Alice,
          templateIds.textKeyOperations,
          Record(fields = List(RecordField(value = Alice.asParty))))
        bobTKO <- ctx.testingHelpers.simpleCreate(
          testIdsGenerator.testCommandId("CK-test-bobTKO"),
          Bob,
          templateIds.textKeyOperations,
          Record(fields = List(RecordField(value = Bob.asParty)))
        )

        // unauthorized lookups are not OK
        // both existing lookups...
        lookupNone = Value(Value.Sum.Optional(Optional(None)))
        lookupSome = (cid: String) => Value(Value.Sum.Optional(Optional(Some(cid.asContractId))))
        _ <- ctx.testingHelpers.failingExercise(
          testIdsGenerator.testCommandId("CK-test-bob-unauthorized-1"),
          Bob,
          templateIds.textKeyOperations,
          bobTKO.contractId,
          "TKOLookup",
          Value(
            Value.Sum.Record(
              Record(fields = List(
                RecordField(value = textKeyKey(Alice, key)),
                RecordField(value = lookupSome(cid1.contractId)))))),
          Code.INVALID_ARGUMENT,
          if (languageVersion precedes LanguageVersion.checkSubmitterInMaintainers) {
            "requires authorizers"
          } else {
            "Expected the submitter 'Bob' to be in maintainers 'Alice'"
          }
        )
        // ..and non-existing ones
        _ <- ctx.testingHelpers.failingExercise(
          testIdsGenerator.testCommandId("CK-test-bob-unauthorized-2"),
          Bob,
          templateIds.textKeyOperations,
          bobTKO.contractId,
          "TKOLookup",
          Value(
            Value.Sum.Record(
              Record(fields = List(
                RecordField(value = textKeyKey(Alice, "bogus-key")),
                RecordField(value = lookupNone))))),
          Code.INVALID_ARGUMENT,
          if (languageVersion precedes LanguageVersion.checkSubmitterInMaintainers) {
            "requires authorizers"
          } else {
            "Expected the submitter 'Bob' to be in maintainers 'Alice'"
          }
        )
        // successful, authorized lookup
        _ <- ctx.testingHelpers.simpleExercise(
          testIdsGenerator.testCommandId("CK-test-alice-lookup-found"),
          Alice,
          templateIds.textKeyOperations,
          aliceTKO.contractId,
          "TKOLookup",
          Value(
            Value.Sum.Record(
              Record(fields = List(
                RecordField(value = textKeyKey(Alice, key)),
                RecordField(value = lookupSome(cid1.contractId))))))
        )
        // successful fetch
        _ <- ctx.testingHelpers.simpleExercise(
          testIdsGenerator.testCommandId("CK-test-alice-fetch-found"),
          Alice,
          templateIds.textKeyOperations,
          aliceTKO.contractId,
          "TKOFetch",
          Value(
            Value.Sum.Record(
              Record(fields = List(
                RecordField(value = textKeyKey(Alice, key)),
                RecordField(value = cid1.contractId.asContractId)))))
        )
        // failing, authorized lookup
        _ <- ctx.testingHelpers.simpleExercise(
          testIdsGenerator.testCommandId("CK-test-alice-lookup-not-found"),
          Alice,
          templateIds.textKeyOperations,
          aliceTKO.contractId,
          "TKOLookup",
          Value(
            Value.Sum.Record(
              Record(fields = List(
                RecordField(value = textKeyKey(Alice, "bogus-key")),
                RecordField(value = lookupNone)))))
        )
        // failing fetch
        _ <- ctx.testingHelpers.failingExercise(
          testIdsGenerator.testCommandId("CK-test-alice-fetch-not-found"),
          Alice,
          templateIds.textKeyOperations,
          aliceTKO.contractId,
          "TKOFetch",
          Value(
            Value.Sum.Record(
              Record(fields = List(
                RecordField(value = textKeyKey(Alice, "bogus-key")),
                RecordField(value = cid1.contractId.asContractId))))),
          Code.INVALID_ARGUMENT,
          "couldn't find key"
        )
        // now we exercise the contract, thus archiving it, and then verify
        // that we cannot look it up anymore
        _ <- ctx.testingHelpers.simpleExercise(
          testIdsGenerator.testCommandId("CK-test-alice-consume-cid1"),
          Alice,
          templateIds.textKey,
          cid1.contractId,
          "TextKeyChoice",
          emptyRecordValue)
        _ <- ctx.testingHelpers.simpleExercise(
          testIdsGenerator.testCommandId("CK-test-alice-lookup-after-consume"),
          Alice,
          templateIds.textKeyOperations,
          aliceTKO.contractId,
          "TKOLookup",
          Value(
            Value.Sum.Record(Record(fields =
              List(RecordField(value = textKeyKey(Alice, key)), RecordField(value = lookupNone)))))
        )
        cid2 <- ctx.testingHelpers.simpleCreate(
          testIdsGenerator.testCommandId("CK-test-cid2"),
          Alice,
          templateIds.textKey,
          textKeyRecord(Alice, "test-key-2", List(Bob))
        )
        _ <- ctx.testingHelpers.simpleExercise(
          testIdsGenerator.testCommandId("CK-test-alice-consume-and-lookup"),
          Alice,
          templateIds.textKeyOperations,
          aliceTKO.contractId,
          "TKOConsumeAndLookup",
          Value(
            Value.Sum.Record(
              Record(fields = List(
                RecordField(value = cid2.contractId.asContractId),
                RecordField(value = textKeyKey(Alice, "test-key-2"))))))
        )
        // failing create when a maintainer is not a signatory
        _ <- ctx.testingHelpers.failingCreate(
          testIdsGenerator.testCommandId("CK-test-alice-create-maintainer-not-signatory"),
          Alice,
          templateIds.maintainerNotSignatory,
          Record(
            fields = List(RecordField(value = Alice.asParty), RecordField(value = Bob.asParty))),
          Code.INVALID_ARGUMENT,
          "are not a subset of the signatories"
        )
      } yield {
        succeed
      }
    }
  }

}
