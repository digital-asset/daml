-- Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

{-# LANGUAGE FlexibleInstances #-}

module DA.Test.DamlcUpgrades (main) where

{- HLINT ignore "locateRunfiles/package_app" -}

import Control.Monad.Extra
import DA.Bazel.Runfiles
import qualified DA.Daml.LF.Ast.Version as LF
import Data.Foldable
import System.Directory.Extra
import System.FilePath
import System.IO.Extra
import DA.Test.Process
import Test.Tasty
import Test.Tasty.HUnit
import SdkVersion (SdkVersioned, sdkVersion, withSdkVersions)
import Text.Regex.TDFA
import qualified Data.Text as T
import Data.Maybe (maybeToList, fromMaybe)

main :: IO ()
main = withSdkVersions $ do
    damlc <- locateRunfiles (mainWorkspace </> "compiler" </> "damlc" </> exe "damlc")
    defaultMain $ tests damlc

tests :: SdkVersioned => FilePath -> TestTree
tests damlc =
    testGroup
        "Upgrade"
        (
            [ mkTest
                "CannotUpgradeView"
                (FailWithError ".*Tried to implement a view of type (‘|\915\199\255)IView(’|\915\199\214) on interface (‘|\915\199\255)V1.I(’|\915\199\214), but the definition of interface (‘|\915\199\255)V1.I(’|\915\199\214) requires a view of type (‘|\915\199\255)V1.IView(’|\915\199\214)")
                testOptions
                  { sharedDep = DependOnV1
                  , warnBadInterfaceInstances = True
                  }
            ] ++
            concat [
                [ mkTest
                      "ValidUpgrade"
                      Succeed
                      testOptions { setUpgradeField = setUpgradeField }
                , mkTest
                      "MissingModule"
                      (FailWithError "\ESC\\[0;91merror type checking <none>:\n  Module Other appears in package that is being upgraded, but does not appear in this package.")
                      testOptions { setUpgradeField = setUpgradeField }
                , mkTest
                      "MissingTemplate"
                      (FailWithError "\ESC\\[0;91merror type checking <none>:\n  Template U appears in package that is being upgraded, but does not appear in this package.")
                      testOptions { setUpgradeField = setUpgradeField }
                , mkTest
                      "MissingDataCon"
                      (FailWithError "\ESC\\[0;91merror type checking <none>:\n  Data type U appears in package that is being upgraded, but does not appear in this package.")
                      testOptions { setUpgradeField = setUpgradeField }
                , mkTest
                      "MissingChoice"
                      (FailWithError "\ESC\\[0;91merror type checking template Main.T :\n  Choice C2 appears in package that is being upgraded, but does not appear in this package.")
                      testOptions { setUpgradeField = setUpgradeField }
                , mkTest
                      "TemplateChangedKeyType"
                      (FailWithError "\ESC\\[0;91merror type checking template Main.T key:\n  The upgraded template T cannot change its key type.")
                      testOptions { setUpgradeField = setUpgradeField }
                , mkTest
                      "TemplateChangedKeyType2"
                      Succeed
                      testOptions { setUpgradeField = setUpgradeField }
                , mkTest
                      "RecordFieldsNewNonOptional"
                      (FailWithError "\ESC\\[0;91merror type checking data type Main.Struct:\n  The upgraded data type Struct has added new fields, but the following new fields are not Optional:\n    Field 'field2' with type Text")
                      testOptions { setUpgradeField = setUpgradeField }
                , mkTest
                      "FailsWithSynonymReturnTypeChange"
                      (FailWithError "\ESC\\[0;91merror type checking template Main.T choice C:\n  The upgraded choice C cannot change its return type.")
                      testOptions { setUpgradeField = setUpgradeField }
                ]
            | setUpgradeField <- [True, False]
            ] ++
            [ testUpgradeCheck
                  "ValidUpgrade"
                  Succeed
            , testUpgradeCheck
                  "MissingModule"
                  (FailWithError "error type checking <none>:\n  Module Other appears in package that is being upgraded, but does not appear in this package.")
            , testUpgradeCheck
                  "MissingTemplate"
                  (FailWithError "error type checking <none>:\n  Template U appears in package that is being upgraded, but does not appear in this package.")
            , testUpgradeCheck
                  "MissingDataCon"
                  (FailWithError "error type checking <none>:\n  Data type U appears in package that is being upgraded, but does not appear in this package.")
            , testUpgradeCheck
                  "MissingChoice"
                  (FailWithError "error type checking template Main.T :\n  Choice C2 appears in package that is being upgraded, but does not appear in this package.")
            , testUpgradeCheck
                  "TemplateAddedChoice"
                  Succeed
            , testUpgradeCheck
                  "TemplateChangedKeyType"
                  (FailWithError "error type checking template Main.T key:\n  The upgraded template T cannot change its key type.")
            , testUpgradeCheck
                  "TemplateChangedKeyType2"
                  Succeed
            , testUpgradeCheck
                  "RecordFieldsNewNonOptional"
                  (FailWithError "error type checking data type Main.Struct:\n  The upgraded data type Struct has added new fields, but the following new fields are not Optional:\n    Field 'field2' with type Text")
            , testUpgradeCheck
                  "FailsWithSynonymReturnTypeChange"
                  (FailWithError "error type checking template Main.T choice C:\n  The upgraded choice C cannot change its return type.")
            , testUpgradeCheck
                  "WarnsWhenTemplateChangesSignatories"
                  (SucceedWithWarning "warning while type checking template Main.A signatories:\n  The upgraded template A has changed the definition of its signatories..*Expression is structurally different")
            , testUpgradeCheck
                  "WarnsWhenTemplateChangesAgreement"
                  (SucceedWithWarning "warning while type checking template Main.A agreement:\n  The upgraded template A has changed the definition of agreement..*Expression is structurally different")
            , testUpgradeCheck
                  "WarnsWhenTemplateChangesObservers"
                  (SucceedWithWarning "warning while type checking template Main.A observers:\n  The upgraded template A has changed the definition of its observers..*Expression is structurally different")
            , testUpgradeCheck
                  "SucceedsWhenATopLevelEnumChanges"
                  Succeed
            , testUpgradeCheck
                  "WarnsWhenTemplateChangesEnsure"
                  (SucceedWithWarning "warning while type checking template Main.A precondition:\n  The upgraded template A has changed the definition of its precondition..*Expression is structurally different")
            , testUpgradeCheck
                  "WarnsWhenTemplateChangesKeyExpression"
                  (SucceedWithWarning "warning while type checking template Main.A key:\n  The upgraded template A has changed the expression for computing its key..*Expression is structurally different")
            , testUpgradeCheck
                  "WarnsWhenTemplateChangesKeyMaintainers"
                  (SucceedWithWarning "warning while type checking template Main.A key:\n  The upgraded template A has changed the maintainers for its key.")
            , testUpgradeCheck
                  "FailsWhenTemplateChangesKeyTypeSuperficially"
                  (FailWithError "error type checking template Main.A key:\n  The upgraded template A cannot change its key type.")
            , testUpgradeCheck
                  "FailsWhenTemplateRemovesKeyType"
                  (FailWithError "error type checking template Main.A key:\n  The upgraded template A cannot remove its key.")
            , testUpgradeCheck
                  "FailsWhenTemplateAddsKeyType"
                  (FailWithError "error type checking template Main.A key:\n  The upgraded template A cannot add a key where it didn't have one previously.")
            , testUpgradeCheck
                  "FailsWhenNewFieldIsAddedToTemplateWithoutOptionalType"
                  (FailWithError "error type checking template Main.A :\n  The upgraded template A has added new fields, but the following new fields are not Optional:\n    Field 'new' with type Int64")
            , testUpgradeCheck
                  "FailsWhenOldFieldIsDeletedFromTemplate"
                  (FailWithError "error type checking template Main.A :\n  The upgraded template A is missing some of its original fields.")
            , testUpgradeCheck
                  "FailsWhenExistingFieldInTemplateIsChanged"
                  (FailWithError "error type checking template Main.A :\n  The upgraded template A has changed the types of some of its original fields.")
            , testUpgradeCheck
                  "SucceedsWhenNewFieldWithOptionalTypeIsAddedToTemplate"
                  Succeed
            , testUpgradeCheck
                  "FailsWhenNewFieldIsAddedToTemplateChoiceWithoutOptionalType"
                  (FailWithError "error type checking template Main.A choice C:\n  The upgraded input type of choice C on template A has added new fields, but the following new fields are not Optional:\n    Field 'new' with type Int64")
            , testUpgradeCheck
                  "FailsWhenOldFieldIsDeletedFromTemplateChoice"
                  (FailWithError "error type checking template Main.A choice C:\n  The upgraded input type of choice C on template A is missing some of its original fields.")
            , testUpgradeCheck
                  "FailsWhenExistingFieldInTemplateChoiceIsChanged"
                  (FailWithError "error type checking template Main.A choice C:\n  The upgraded input type of choice C on template A has changed the types of some of its original fields.")
            , testUpgradeCheck
                  "WarnsWhenControllersOfTemplateChoiceAreChanged"
                  (SucceedWithWarning "warning while type checking template Main.A choice C:\n  The upgraded choice C has changed the definition of controllers.")
            , testUpgradeCheck
                  "WarnsWhenObserversOfTemplateChoiceAreChanged"
                  (SucceedWithWarning "warning while type checking template Main.A choice C:\n  The upgraded choice C has changed the definition of observers.")
            , testUpgradeCheck
                  "FailsWhenTemplateChoiceChangesItsReturnType"
                  (FailWithError "error type checking template Main.A choice C:\n  The upgraded choice C cannot change its return type.")
            , testUpgradeCheck
                  "SucceedsWhenTemplateChoiceReturnsATemplateWhichHasChanged"
                  Succeed
            , testUpgradeCheck
                  "SucceedsWhenTemplateChoiceInputArgumentTemplateHasChanged"
                  Succeed
            , testUpgradeCheck
                  "SucceedsWhenTemplateChoiceInputArgumentEnumHasChanged"
                  Succeed
            , testUpgradeCheck
                  "SucceedsWhenTemplateChoiceInputArgumentStructHasChanged"
                  Succeed
            , testUpgradeCheck
                  "SucceedsWhenTemplateChoiceInputArgumentVariantHasChanged"
                  Succeed
            , testUpgradeCheck
                  "SucceedsWhenNewFieldWithOptionalTypeIsAddedToTemplateChoice"
                  Succeed
            , testUpgradeCheck
                  "FailsWhenATopLevelRecordAddsANonOptionalField"
                  (FailWithError "error type checking data type Main.A:\n  The upgraded data type A has added new fields, but the following new fields are not Optional:\n    Field 'y' with type Text")
            , testUpgradeCheck
                  "SucceedsWhenATopLevelRecordAddsAnOptionalFieldAtTheEnd"
                  Succeed
            , testUpgradeCheck
                  "FailsWhenATopLevelRecordAddsAnOptionalFieldBeforeTheEnd"
                  (FailWithError "error type checking data type Main.A:\n  The upgraded data type A has added new fields, but the following fields need to be moved to the end: 'y'. All new fields in upgrades must be added to the end of the definition.")
            , testUpgradeCheck
                  "SucceedsWhenATopLevelVariantAddsAConstructor"
                  Succeed
            , testUpgradeCheck
                  "FailsWhenATopLevelVariantRemovesAConstructor"
                  (FailWithError "error type checking <none>:\n  Data type A.Z appears in package that is being upgraded, but does not appear in this package.")
            , testUpgradeCheck
                  "FailWhenATopLevelVariantChangesChangesTheOrderOfItsConstructors"
                  (FailWithError "error type checking data type Main.A:\n  The upgraded data type A has changed the order of its constructors - any new constructor must be added at the end of the variant.")
            , testUpgradeCheck
                  "FailsWhenATopLevelVariantAddsAFieldToAConstructorsType"
                  (FailWithError "error type checking data type Main.A:\n  The upgraded constructor Y from variant A has added new fields, but the following new fields are not Optional:\n    Field 'y2' with type Int64")
            , testUpgradeCheck
                  "SucceedsWhenATopLevelVariantAddsAnOptionalFieldToAConstructorsType"
                  Succeed
            , testUpgradeCheck
                  "SucceedWhenATopLevelEnumAddsAField"
                  Succeed
            , testUpgradeCheck
                  "FailsWhenAnEnumDropsAConstructor"
                  (FailWithError "error type checking data type Main.MyEnum:\n  The upgraded data type MyEnum is missing some of its original constructors: MyEnumCon3")
            , testUpgradeCheck
                  "FailWhenATopLevelEnumChangesChangesTheOrderOfItsConstructors"
                  (FailWithError "error type checking data type Main.A:\n  The upgraded data type A has changed the order of its constructors - any new enum constructor must be added at the end of the enum.")
            , testUpgradeCheck
                  "FailsWithSynonymReturnTypeChangeInSeparatePackage"
                  (FailWithError "error type checking template Main.T choice C:\n  The upgraded choice C cannot change its return type.")
            , testUpgradeCheck
                  "SucceedsWhenUpgradingADependency"
                  Succeed
            , testUpgradeCheck
                  "FailsOnlyInModuleNotInReexports"
                  (FailWithError "error type checking data type Other.A:\n  The upgraded data type A has added new fields, but the following new fields are not Optional:\n    Field 'field2' with type Text")
            , testUpgradeCheck
                  "FailsWhenDatatypeChangesVariety"
                  (FailWithError "error type checking data type Main.RecordToEnum:\n  The upgraded data type RecordToEnum has changed from a record to a enum.")
            , testUpgradeCheck
                  "SucceedsWhenATopLevelTypeSynonymChanges"
                  Succeed
            , testUpgradeCheck
                  "SucceedsWhenTwoDeeplyNestedTypeSynonymsResolveToTheSameDatatypes"
                  Succeed
            , testUpgradeCheck
                  "FailsWhenTwoDeeplyNestedTypeSynonymsResolveToDifferentDatatypes"
                  (FailWithError "error type checking template Main.A :\n  The upgraded template A has changed the types of some of its original fields")
            , testUpgradeCheck
                  "SucceedsWhenAnInterfaceIsOnlyDefinedInTheInitialPackage"
                  Succeed
            , testUpgradeCheck
                  "FailsWhenAnInterfaceIsDefinedInAnUpgradingPackageWhenItWasAlreadyInThePriorPackage"
                  (FailWithError "error type checking interface Main.I :\n  Tried to upgrade interface I, but interfaces cannot be upgraded. They should be removed whenever a package is being upgraded.")
            , testUpgradeCheck
                  "FailsWhenAnInstanceIsDropped"
                  (FailWithError "error type checking template Main.T :\n  Implementation of interface I by template T appears in package that is being upgraded, but does not appear in this package.")
            , testUpgradeCheck
                  "FailsWhenAnInstanceIsAddedSeparateDep"
                  (FailWithError "error type checking template Main.T :\n  Implementation of interface I by template T appears in this package, but does not appear in package that is being upgraded.")
            , testUpgradeCheck
                  "FailsWhenAnInstanceIsAddedUpgradedPackage"
                  (FailWithError "error type checking template Main.T :\n  Implementation of interface I by template T appears in this package, but does not appear in package that is being upgraded.")
            , testUpgradeCheck
                  "FailsWhenAnInstanceIsReplacedWithADifferentInstanceOfAnIdenticallyNamedInterface"
                  (FailWithError "error type checking template Main.T :\n  Implementation of interface I by template T appears in package that is being upgraded, but does not appear in this package.")
            , testUpgradeCheck
                  "SucceedsWhenAnInstanceIsAddedToNewTemplateSeparateDep"
                  Succeed
            , testUpgradeCheck
                  "SucceedsWhenAnInstanceIsAddedToNewTemplateUpgradedPackage"
                  Succeed
            , testUpgradeCheck
                  "FailsWhenDepsDowngradeVersionsWhileUsingDatatypes"
                  (FailWithError "error type checking data type Main.Main:\n  The upgraded data type Main has changed the types of some of its original fields.")
            , testUpgradeCheck
                  "SucceedsWhenDepsDowngradeVersionsWithoutUsingDatatypes"
                  Succeed
            , testUpgradeCheck
                  "FailsWhenDependencyIsNotAValidUpgrade"
                  (FailWithError "error while validating that dependency upgrades-example-FailsWhenDependencyIsNotAValidUpgrade-dep version 2.0.0 is a valid upgrade of version 1.0.0\n  error type checking data type Dep.Dep:\n    The upgraded data type Dep has added new fields, but the following new fields are not Optional:\n      Field 'nonOptionalField' with type Text")
            , testUpgradeCheck
                  "SucceedsWhenUpgradingLFVersionWithoutExpressionWarning"
                  (SucceedWithoutWarning "warning while type checking data type Main.T:\n  The upgraded template T has changed the definition of its signatories.")
            , testUpgradeCheck
                  "WarnsWhenExpressionChangesUtilityToSchemaPackage"
                  (SucceedWithWarning ".*the previous package was a utility package and the current one is not.")
            , testUpgradeCheck
                  "WarnsWhenExpressionDowngradesVersion"
                  (SucceedWithWarning ".*Both packages support upgrades, but the previous package had a higher version than the current one.")
            , testUpgradeCheck
                  "FailWhenParamCountChanges"
                  (FailWithError "error type checking data type Main.MyStruct:\n  The upgraded data type MyStruct has changed the number of type variables it has.")
            , testUpgradeCheck
                  "SucceedWhenParamNameChanges"
                  Succeed
            , testUpgradeCheck
                  "SucceedWhenPhantomParamBecomesUsed"
                  Succeed
            , testUpgradeCheck
                  "SucceedsWhenNonSerializableTypesAreIncompatible"
                  Succeed
            , testUpgradeCheck
                  "SucceedsWhenAddingNonOptionalFieldsToUnserializableTypes"
                  Succeed
            , testUpgradeCheck
                  "SucceedsWhenChangingConstructorOfUnserializableType"
                  Succeed
            , testUpgradeCheck
                  "SucceedsWhenDeletingUnserializableType"
                  Succeed
            , testUpgradeCheck
                  "FailsWhenMakingTypeUnserializable"
                  (FailWithError "error type checking data type Main.MyData:\n  The upgraded data type MyData was serializable and is now unserializable. Datatypes cannot change their serializability via upgrades.")
            -- TODO https://github.com/digital-asset/daml/issues/19980
            -- Currently there is no good way to test BindingMismatch errors
            -- in upgrades, because LF turns lambdas into their own
            -- top-level definitions, which are not expanded by the
            -- expression equality checker. We're disabling the following
            -- test in the meantime.
            --, test
            --      "WarnsWhenExpressionChangesBindingOrder"
            --      (SucceedWithWarning ".*refer to different bindings in the environment")
            --      version1_dev
            --      (SeparateDeps False)
            --      False
            --      True
            ] ++
            concat [
                [ mkTest
                      (prefix <> "WhenAnInterfaceAndATemplateAreDefinedInTheSamePackage")
                      (expectation "type checking <none>:\n  This package defines both interfaces and templates.")
                      testOptions
                        { mbLocation = Just "WarnsWhenAnInterfaceAndATemplateAreDefinedInTheSamePackage"
                        , warnBadInterfaceInstances = warnBadInterfaceInstances
                        , doTypecheck = doTypecheck
                        }
                , mkTest
                      (prefix <> "WhenAnInterfaceIsUsedInThePackageThatItsDefinedIn")
                      (expectation "type checking template Main.T interface instance Main.I for Main.T:\n  The interface I was defined in this package") -- TODO complete error
                      testOptions
                        { mbLocation = Just "WarnsWhenAnInterfaceIsUsedInThePackageThatItsDefinedIn"
                        , warnBadInterfaceInstances = warnBadInterfaceInstances
                        , doTypecheck = doTypecheck
                        }
                ]
            | warnBadInterfaceInstances <- [True, False]
            , let prefix = if warnBadInterfaceInstances then "Warns" else "Fail"
            , let expectation msg =
                      if warnBadInterfaceInstances
                         then SucceedWithWarning ("\ESC\\[0;93mwarning while " <> msg)
                         else FailWithError ("\ESC\\[0;91merror " <> msg)
            , doTypecheck <- [True, False]
            ] ++
            [ mkTest
                  "FailsWhenUpgradedFieldPackagesAreNotUpgradable"
                  (FailWithError "\ESC\\[0;91merror type checking data type ProjectMain.T:\n  The upgraded data type T has changed the types of some of its original fields.")
                  testOptions
                    -- Note that dependencies are in different order
                    { additionalDarsV1 = ["upgrades-SucceedsWhenATopLevelRecordAddsAnOptionalFieldAtTheEnd-v2.dar"]
                    , additionalDarsV2 = ["upgrades-SucceedsWhenATopLevelRecordAddsAnOptionalFieldAtTheEnd-v1.dar"]
                    }
            , mkTest
                  "FailsWhenUpgradedFieldFromDifferentPackageName"
                  (FailWithError "\ESC\\[0;91merror type checking data type Main.A:\n  The upgraded data type A has changed the types of some of its original fields.")
                  testOptions
                    { additionalDarsV1 = ["upgrades-FailsWhenUpgradedFieldFromDifferentPackageName-dep-name1.dar"]
                    , additionalDarsV2 = ["upgrades-FailsWhenUpgradedFieldFromDifferentPackageName-dep-name2.dar"]
                    }
            , mkTest
                  "WarnsWhenExpressionChangesPackageId"
                  (SucceedWithWarning "\ESC\\[0;93mwarning while type checking template Main.T signatories:\n  The upgraded template T has changed the definition of its signatories..*Name came from package .* and now comes from differently-named package .*")
                  testOptions
                    { additionalDarsV1 = ["upgrades-WarnsWhenExpressionChangesPackageId-dep-name1.dar"]
                    , additionalDarsV2 = ["upgrades-WarnsWhenExpressionChangesPackageId-dep-name2.dar"]
                    }
            , testMetadata
                  "FailsWhenUpgradesPackageHasDifferentPackageName"
                  (FailWithError $ 
                    "\ESC\\[0;91mMain package must have the same package name as upgraded package.\n"
                      <> "Main package \\(v0.0.2\\) name: my-package2\n"
                      <> "Upgraded package \\(v0.0.1\\) name: my-package"
                  )
                  "my-package"
                  "0.0.1"
                  LF.version1_dev
                  "my-package2"
                  "0.0.2"
                  LF.version1_dev
            , testMetadata
                  "FailsWhenUpgradesPackageHasEqualVersion"
                  (FailWithError "\ESC\\[0;91mMain package \\(v0.0.1\\) cannot have the same package version as Upgraded package \\(v0.0.1\\)")
                  "my-package"
                  "0.0.1"
                  LF.version1_dev
                  "my-package"
                  "0.0.1"
                  LF.version1_dev
            , testMetadata
                  "FailsWhenUpgradesPackageHasHigherVersion"
                  (FailWithError "\ESC\\[0;91mUpgraded package \\(v0.0.2\\) cannot have a higher package version than Main package \\(v0.0.1\\)")
                  "my-package"
                  "0.0.2"
                  LF.version1_dev
                  "my-package"
                  "0.0.1"
                  LF.version1_dev
            , testMetadata
                  "FailsWhenUpgradesPackageDoesNotSupportUpgrades"
                  (FailWithError "\ESC\\[0;91mUpgraded package \\(v0.0.1\\) LF Version \\(1.15\\) does not support Smart Contract Upgrades")
                  "my-package"
                  "0.0.1"
                  LF.version1_15
                  "my-package"
                  "0.0.2"
                  LF.version1_dev
            , testMetadata
                  "FailsWhenMainPackageDoesNotSupportUpgrades"
                  (FailWithError "\ESC\\[0;91mMain package \\(v0.0.2\\) LF Version \\(1.15\\) does not support Smart Contract Upgrades")
                  "my-package"
                  "0.0.1"
                  LF.version1_dev
                  "my-package"
                  "0.0.2"
                  LF.version1_15
            , testMetadata
                  "FailsWhenUpgradesPackageHasHigherLFVersion"
                  (FailWithError "\ESC\\[0;91mMain package \\(v0.0.2\\) LF Version \\(1.17\\) cannot be lower than the Upgraded package \\(v0.0.1\\) LF Version \\(1.dev\\)")
                  "my-package"
                  "0.0.1"
                  LF.version1_dev
                  "my-package"
                  "0.0.2"
                  LF.version1_17
            , testMetadata
                  "SucceedsWhenUpgradesPackageHasLowerLFVersion"
                  Succeed
                  "my-package"
                  "0.0.1"
                  LF.version1_17
                  "my-package"
                  "0.0.2"
                  LF.version1_dev
            , testUpgradeCheck
                  "SucceedsWhenAnExceptionIsOnlyDefinedInTheInitialPackage"
                  Succeed
            , testUpgradeCheck
                  "FailsWhenAnExceptionIsDefinedInAnUpgradingPackageWhenItWasAlreadyInThePriorPackage"
                  (FailWithError "error type checking exception Main.E:\n  Tried to upgrade exception E, but exceptions cannot be upgraded. They should be removed in any upgrading package.")
            ]
       )
  where
    testUpgradeCheck
        :: String
        -> Expectation
        -> TestTree
    testUpgradeCheck name expectation =
        testCase (name <> " (upgrade-check)") $ do
            let testAdditionaDarRunfile version = locateRunfiles (mainWorkspace </> "test-common" </> ("upgrades-" <> name <> "-" <> version <> ".dar"))
            v1Dar <- testAdditionaDarRunfile "v1"
            v2Dar <- testAdditionaDarRunfile "v2"
            let expectedDiagFile = Nothing
            let regexPrefix = maybe "" (\filePat -> "File:.*" <> T.pack filePat <> ".+") expectedDiagFile
            case expectation of
              Succeed ->
                  callProcessSilent damlc ["upgrade-check", v1Dar, v2Dar]
              SucceedWithoutWarning regex -> do
                  stderr <- callProcessForSuccessfulStderr damlc ["upgrade-check", v1Dar, v2Dar]
                  let regexWithSeverity = "Severity: DsWarning\nMessage: \n" <> regex
                  let compiledRegex :: Regex
                      compiledRegex = makeRegexOpts defaultCompOpt { multiline = False } defaultExecOpt regexWithSeverity
                  when (matchTest compiledRegex stderr) $
                    assertFailure ("`daml build` succeeded, but should not give a warning matching '" <> show regexWithSeverity <> "':\n" <> show stderr)
              FailWithError regex -> do
                  stderr <- callProcessForStderr damlc ["upgrade-check", v1Dar, v2Dar]
                  let regexWithSeverity = regexPrefix <> "Severity: DsError\nMessage: \n" <> regex
                  let compiledRegex :: Regex
                      compiledRegex = makeRegexOpts defaultCompOpt { multiline = False } defaultExecOpt regexWithSeverity
                  unless (matchTest compiledRegex stderr) $
                      assertFailure ("`daml build` failed as expected, but did not give an error matching '" <> show regexWithSeverity <> "':\n" <> show stderr)
              SucceedWithWarning regex -> do
                  stderr <- callProcessForSuccessfulStderr damlc ["upgrade-check", v1Dar, v2Dar]
                  let regexWithSeverity = regexPrefix <> "Severity: DsWarning\nMessage: \n" <> regex
                  let compiledRegex :: Regex
                      compiledRegex = makeRegexOpts defaultCompOpt { multiline = False } defaultExecOpt regexWithSeverity
                  case matchCount compiledRegex stderr of
                    0 -> assertFailure ("`daml build` succeeded, but did not give a warning matching '" <> show regexWithSeverity <> "':\n" <> show stderr)
                    1 -> pure ()
                    _ -> assertFailure ("`daml build` succeeded, but gave a warning matching '" <> show regexWithSeverity <> "' more than once:\n" <> show stderr)
            pure ()

    mkTest :: String -> Expectation -> TestOptions -> TestTree
    mkTest name expectation TestOptions{..} =
        let location = fromMaybe name mbLocation
            upgradeFieldTrailer = if not setUpgradeField then " (no upgrades field)" else ""
            doTypecheckTrailer = if not doTypecheck then " (disable typechecking)" else ""
        in
        testCase (name <> " (damlc build)" <> upgradeFieldTrailer <> doTypecheckTrailer) $ do
        withTempDir $ \dir -> do
            let newDir = dir </> "newVersion"
            let oldDir = dir </> "oldVersion"
            let newDar = newDir </> "out.dar"
            let oldDar = oldDir </> "old.dar"

            let testRunfile path = locateRunfiles (mainWorkspace </> "test-common/src/main/daml/upgrades" </> path)
            let testAdditionaDarRunfile darName = locateRunfiles (mainWorkspace </> "test-common" </> darName)

            v1FilePaths <- listDirectory =<< testRunfile (location </> "v1")
            let oldVersion = flip map v1FilePaths $ \path ->
                  ( "daml" </> path
                  , readFile =<< testRunfile (location </> "v1" </> path)
                  )
            v2FilePaths <- listDirectory =<< testRunfile (location </> "v2")
            let newVersion = flip map v2FilePaths $ \path ->
                  ( "daml" </> path
                  , readFile =<< testRunfile (location </> "v2" </> path)
                  )

            let ((oldDepLfVersion, newDepLfVersion), (oldLfVersion, newLfVersion)) = versionPairs lfVersion
            (depV1Dar, depV2Dar) <- case sharedDep of
              SharedDep -> do
                depFilePaths <- listDirectory =<< testRunfile (location </> "dep")
                let sharedDepFiles = flip map depFilePaths $ \path ->
                      ( "daml" </> path
                      , readFile =<< testRunfile (location </> "dep" </> path)
                      )

                let oldSharedDir = dir </> "oldShared"
                let oldSharedDar = oldSharedDir </> "out.dar"
                writeFiles oldSharedDir (projectFile "0.0.1" oldDepLfVersion ("upgrades-example-" <> location <> "-dep") Nothing Nothing [] : sharedDepFiles)
                callProcessSilent damlc ["build", "--project-root", oldSharedDir, "-o", oldSharedDar]

                let newSharedDir = dir </> "newShared"
                let newSharedDar = newSharedDir </> "out.dar"
                writeFiles newSharedDir (projectFile "0.0.1" newDepLfVersion ("upgrades-example-" <> location <> "-dep") Nothing Nothing [] : sharedDepFiles)
                callProcessSilent damlc ["build", "--project-root", newSharedDir, "-o", newSharedDar]

                pure (Just oldSharedDar, Just newSharedDar)
              SeparateDeps { shouldSwap } -> do
                depV1FilePaths <- listDirectory =<< testRunfile (location </> "dep-v1")
                let depV1Files = flip map depV1FilePaths $ \path ->
                      ( "daml" </> path
                      , readFile =<< testRunfile (location </> "dep-v1" </> path)
                      )
                let depV1Dir = dir </> "shared-v1"
                let depV1Dar = depV1Dir </> "out.dar"
                writeFiles depV1Dir (projectFile "0.0.1" (if shouldSwap then newDepLfVersion else oldDepLfVersion) ("upgrades-example-" <> location <> "-dep") Nothing Nothing [] : depV1Files)
                callProcessSilent damlc ["build", "--project-root", depV1Dir, "-o", depV1Dar]

                depV2FilePaths <- listDirectory =<< testRunfile (location </> "dep-v2")
                let depV2Files = flip map depV2FilePaths $ \path ->
                      ( "daml" </> path
                      , readFile =<< testRunfile (location </> "dep-v2" </> path)
                      )
                let depV2Dir = dir </> "shared-v2"
                let depV2Dar = depV2Dir </> "out.dar"
                writeFiles depV2Dir (projectFile "0.0.2" (if shouldSwap then oldDepLfVersion else newDepLfVersion) ("upgrades-example-" <> location <> "-dep") Nothing Nothing [] : depV2Files)
                callProcessSilent damlc ["build", "--project-root", depV2Dir, "-o", depV2Dar]

                if shouldSwap
                   then pure (Just depV2Dar, Just depV1Dar)
                   else pure (Just depV1Dar, Just depV2Dar)
              DependOnV1 ->
                pure (Nothing, Just oldDar)
              _ ->
                pure (Nothing, Nothing)

            v1AdditionalDarsRunFiles <- traverse testAdditionaDarRunfile additionalDarsV1
            writeFiles oldDir (projectFile "0.0.1" oldLfVersion ("upgrades-example-" <> location) Nothing depV1Dar v1AdditionalDarsRunFiles : oldVersion)
            callProcessSilent damlc ["build", "--project-root", oldDir, "-o", oldDar]

            v2AdditionalDarsRunFiles <- traverse testAdditionaDarRunfile additionalDarsV2
            writeFiles newDir (projectFile "0.0.2" newLfVersion ("upgrades-example-" <> location) (if setUpgradeField then Just oldDar else Nothing) depV2Dar v2AdditionalDarsRunFiles : newVersion)

            handleExpectation expectation newDir newDar (doTypecheck && setUpgradeField) Nothing
      where
        projectFile version lfVersion name upgradedFile mbDep darDeps =
          makeProjectFile name version lfVersion upgradedFile (maybeToList mbDep ++ darDeps) doTypecheck warnBadInterfaceInstances

    handleExpectation :: Expectation -> FilePath -> FilePath -> Bool -> Maybe FilePath -> IO ()
    handleExpectation expectation dir dar shouldRunChecks expectedDiagFile =
      case expectation of
        Succeed ->
            callProcessSilent damlc ["build", "--project-root", dir, "-o", dar]
        SucceedWithoutWarning regex -> do
            stderr <- callProcessForSuccessfulStderr damlc ["build", "--project-root", dir, "-o", dar]
            let regexWithSeverity = "Severity: DsWarning\nMessage: \n" <> regex
            let compiledRegex :: Regex
                compiledRegex = makeRegexOpts defaultCompOpt { multiline = False } defaultExecOpt regexWithSeverity
            when (matchTest compiledRegex stderr) $
              if shouldRunChecks
                then assertFailure ("`daml build` succeeded, but should not give a warning matching '" <> show regexWithSeverity <> "':\n" <> show stderr)
                else assertFailure ("`daml build` succeeded, did not `upgrade:` field set, should not give a warning matching '" <> show regexWithSeverity <> "':\n" <> show stderr)
        FailWithError _ | not shouldRunChecks ->
            callProcessSilent damlc ["build", "--project-root", dir, "-o", dar]
        FailWithError regex -> do
            stderr <- callProcessForStderr damlc ["build", "--project-root", dir, "-o", dar]
            let regexWithSeverity = regexPrefix <> "Severity: DsError\nMessage: \n" <> regex
            let compiledRegex :: Regex
                compiledRegex = makeRegexOpts defaultCompOpt { multiline = False } defaultExecOpt regexWithSeverity
            unless (matchTest compiledRegex stderr) $
                assertFailure ("`daml build` failed as expected, but did not give an error matching '" <> show regexWithSeverity <> "':\n" <> show stderr)
        SucceedWithWarning regex -> do
            stderr <- callProcessForSuccessfulStderr damlc ["build", "--project-root", dir, "-o", dar]
            let regexWithSeverity = regexPrefix <> "Severity: DsWarning\nMessage: \n" <> regex
            let compiledRegex :: Regex
                compiledRegex = makeRegexOpts defaultCompOpt { multiline = False } defaultExecOpt regexWithSeverity
            if shouldRunChecks
                then case matchCount compiledRegex stderr of
                      0 -> assertFailure ("`daml build` succeeded, but did not give a warning matching '" <> show regexWithSeverity <> "':\n" <> show stderr)
                      1 -> pure ()
                      _ -> assertFailure ("`daml build` succeeded, but gave a warning matching '" <> show regexWithSeverity <> "' more than once:\n" <> show stderr)
                else when (matchTest compiledRegex stderr) $
                      assertFailure ("`daml build` succeeded, did not have `upgrade:` field set, should NOT give a warning matching '" <> show regexWithSeverity <> "':\n" <> show stderr)
      where
        -- Note ".*" after "File:", as CI prefixes the path with `private/`, which we need to match
        regexPrefix = maybe "" (\filePat -> "File:.*" <> T.pack filePat <> ".+") expectedDiagFile

    testMetadata :: String -> Expectation -> String -> String -> LF.Version -> String -> String -> LF.Version -> TestTree
    testMetadata name expectation v1Name v1Version v1LfVersion v2Name v2Version v2LfVersion =
        testCase (name <> " (damlc build)") $ do
        withTempDir $ \dir -> do
            let newDir = dir </> "newVersion"
            let oldDir = dir </> "oldVersion"
            let newDar = newDir </> "out.dar"
            let oldDar = oldDir </> "old.dar"
            writeFiles oldDir [makeProjectFile v1Name v1Version v1LfVersion Nothing [] True False, ("daml/Main.daml", pure "module Main where")]
            callProcessSilent damlc ["build", "--project-root", oldDir, "-o", oldDar]
            writeFiles newDir [makeProjectFile v2Name v2Version v2LfVersion (Just oldDar) [] True False, ("daml/Main.daml", pure "module Main where")]
            -- Metadata errors are reported on the daml.yaml file
            handleExpectation expectation newDir newDar True (Just $ toUnixPath $ newDir </> "daml.yaml")

    toUnixPath :: FilePath -> FilePath
    toUnixPath = fmap $ \case
      '\\' -> '/'
      c -> c

    makeProjectFile :: String -> String -> LF.Version -> Maybe FilePath -> [FilePath] -> Bool -> Bool -> (FilePath, IO String)
    makeProjectFile name version lfVersion upgradedFile deps doTypecheck warnBadInterfaceInstances =
        ( "daml.yaml"
        , pure $ unlines $
          [ "sdk-version: " <> sdkVersion
          , "name: " <> name
          , "source: daml"
          , "version: " <> version
          , "dependencies:"
          , "  - daml-prim"
          , "  - daml-stdlib"
          , "build-options:"
          , "  - --target=" <> LF.renderVersion lfVersion
          ]
            ++ ["  - --typecheck-upgrades=no" | not doTypecheck]
            ++ ["  - -Wupgrade-interfaces" | warnBadInterfaceInstances ]
            ++ ["upgrades: '" <> path <> "'" | Just path <- pure upgradedFile]
            ++ renderDataDeps deps
        )
      where
        renderDataDeps :: [String] -> [String]
        renderDataDeps [] = []
        renderDataDeps paths =
          ["data-dependencies:"] ++ [" -  '" <> path <> "'" | path <- paths]

    writeFiles :: FilePath -> [(FilePath, IO String)] -> IO ()
    writeFiles dir fs =
        for_ fs $ \(file, ioContent) -> do
            content <- ioContent
            createDirectoryIfMissing True (takeDirectory $ dir </> file)
            writeFileUTF8 (dir </> file) content

data TestOptions = TestOptions
  { mbLocation :: Maybe String
  , lfVersion :: VersionPair
  , sharedDep :: Dependency
  , warnBadInterfaceInstances :: Bool
  , setUpgradeField :: Bool
  , doTypecheck :: Bool
  , additionalDarsV1 :: [String]
  , additionalDarsV2 :: [String]
  }

testOptions :: TestOptions
testOptions =
  TestOptions
    { mbLocation = Nothing
    , lfVersion = versionPairs versionDefault
    , sharedDep = NoDependencies
    , warnBadInterfaceInstances = False
    , setUpgradeField = True
    , doTypecheck = True
    , additionalDarsV1 = []
    , additionalDarsV2 = []
    }

versionDefault :: LF.Version
versionDefault = LF.version1_dev

data Expectation
  = Succeed
  | FailWithError T.Text
  | SucceedWithWarning T.Text
  | SucceedWithoutWarning T.Text
  deriving (Show, Eq, Ord)

data Dependency
  = NoDependencies
  | DependOnV1
  | SharedDep
  | SeparateDeps { shouldSwap :: Bool }

type VersionPair = ((LF.Version, LF.Version), (LF.Version, LF.Version))

class IsVersionPair a where
  versionPairs :: a -> VersionPair

instance IsVersionPair LF.Version where
  versionPairs v = ((v, v), (v, v))

instance IsVersionPair (LF.Version, LF.Version) where
  versionPairs vs = (vs, vs)

instance IsVersionPair VersionPair where
  versionPairs vs = vs
