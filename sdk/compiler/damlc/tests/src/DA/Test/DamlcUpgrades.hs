-- Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

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
import DA.Daml.LF.Ast.Version
import Text.Regex.TDFA
import qualified Data.Text as T
import Data.Maybe (maybeToList)

main :: IO ()
main = withSdkVersions $ do
    damlc <- locateRunfiles (mainWorkspace </> "compiler" </> "damlc" </> exe "damlc")
    defaultMain $ tests damlc

tests :: SdkVersioned => FilePath -> TestTree
tests damlc =
    testGroup
        "Upgrade"
        (
            [ test
                "CannotUpgradeView"
                (FailWithError ".*Tried to implement a view of type (‘|\915\199\255)IView(’|\915\199\214) on interface (‘|\915\199\255)V1.I(’|\915\199\214), but the definition of interface (‘|\915\199\255)V1.I(’|\915\199\214) requires a view of type (‘|\915\199\255)V1.IView(’|\915\199\214)")
                versionDefault
                DependOnV1
                True
                True
            ] ++
            concat [
                [ test
                      "WarnsWhenTemplateChangesSignatories"
                      (SucceedWithWarning "\ESC\\[0;93mwarning while type checking template Main.A signatories:\n  The upgraded template A has changed the definition of its signatories.")
                      versionDefault
                      NoDependencies
                      False
                      setUpgradeField
                , test
                      "WarnsWhenTemplateChangesAgreement"
                      (SucceedWithWarning "\ESC\\[0;93mwarning while type checking template Main.A agreement:\n  The upgraded template A has changed the definition of agreement.")
                      versionDefault
                      NoDependencies
                      False
                      setUpgradeField
                , test
                      "WarnsWhenTemplateChangesObservers"
                      (SucceedWithWarning "\ESC\\[0;93mwarning while type checking template Main.A observers:\n  The upgraded template A has changed the definition of its observers.")
                      versionDefault
                      NoDependencies
                      False
                      setUpgradeField
                , test
                      "SucceedsWhenATopLevelEnumChanges"
                      Succeed
                      versionDefault
                      NoDependencies
                      False
                      setUpgradeField
                , test
                      "WarnsWhenTemplateChangesEnsure"
                      (SucceedWithWarning "\ESC\\[0;93mwarning while type checking template Main.A precondition:\n  The upgraded template A has changed the definition of its precondition.")
                      versionDefault
                      NoDependencies
                      False
                      setUpgradeField
                , test
                      "WarnsWhenTemplateChangesKeyExpression"
                      (SucceedWithWarning "\ESC\\[0;93mwarning while type checking template Main.A key:\n  The upgraded template A has changed the expression for computing its key.")
                      versionDefault
                      NoDependencies
                      False
                      setUpgradeField
                , test
                      "WarnsWhenTemplateChangesKeyMaintainers"
                      (SucceedWithWarning "\ESC\\[0;93mwarning while type checking template Main.A key:\n  The upgraded template A has changed the maintainers for its key.")
                      versionDefault
                      NoDependencies
                      False
                      setUpgradeField
                , test
                      "FailsWhenTemplateChangesKeyTypeSuperficially"
                      (FailWithError "\ESC\\[0;91merror type checking template Main.A key:\n  The upgraded template A cannot change its key type.")
                      versionDefault
                      NoDependencies
                      False
                      setUpgradeField
                , test
                      "FailsWhenTemplateRemovesKeyType"
                      (FailWithError "\ESC\\[0;91merror type checking template Main.A key:\n  The upgraded template A cannot remove its key.")
                      versionDefault
                      NoDependencies
                      False
                      setUpgradeField
                , test
                      "FailsWhenTemplateAddsKeyType"
                      (FailWithError "\ESC\\[0;91merror type checking template Main.A key:\n  The upgraded template A cannot add a key where it didn't have one previously.")
                      versionDefault
                      NoDependencies
                      False
                      setUpgradeField
                , test
                      "FailsWhenNewFieldIsAddedToTemplateWithoutOptionalType"
                      (FailWithError "\ESC\\[0;91merror type checking template Main.A :\n  The upgraded template A has added new fields, but those fields are not Optional.")
                      versionDefault
                      NoDependencies
                      False
                      setUpgradeField
                , test
                      "FailsWhenOldFieldIsDeletedFromTemplate"
                      (FailWithError "\ESC\\[0;91merror type checking template Main.A :\n  The upgraded template A is missing some of its original fields.")
                      versionDefault
                      NoDependencies
                      False
                      setUpgradeField
                , test
                      "FailsWhenExistingFieldInTemplateIsChanged"
                      (FailWithError "\ESC\\[0;91merror type checking template Main.A :\n  The upgraded template A has changed the types of some of its original fields.")
                      versionDefault
                      NoDependencies
                      False
                      setUpgradeField
                , test
                      "SucceedsWhenNewFieldWithOptionalTypeIsAddedToTemplate"
                      Succeed
                      versionDefault
                      NoDependencies
                      False
                      setUpgradeField
                , test
                      "FailsWhenNewFieldIsAddedToTemplateChoiceWithoutOptionalType"
                      (FailWithError "\ESC\\[0;91merror type checking template Main.A choice C:\n  The upgraded input type of choice C on template A has added new fields, but those fields are not Optional.")
                      versionDefault
                      NoDependencies
                      False
                      setUpgradeField
                , test
                      "FailsWhenOldFieldIsDeletedFromTemplateChoice"
                      (FailWithError "\ESC\\[0;91merror type checking template Main.A choice C:\n  The upgraded input type of choice C on template A is missing some of its original fields.")
                      versionDefault
                      NoDependencies
                      False
                      setUpgradeField
                , test
                      "FailsWhenExistingFieldInTemplateChoiceIsChanged"
                      (FailWithError "\ESC\\[0;91merror type checking template Main.A choice C:\n  The upgraded input type of choice C on template A has changed the types of some of its original fields.")
                      versionDefault
                      NoDependencies
                      False
                      setUpgradeField
                , test
                      "WarnsWhenControllersOfTemplateChoiceAreChanged"
                      (SucceedWithWarning "\ESC\\[0;93mwarning while type checking template Main.A choice C:\n  The upgraded choice C has changed the definition of controllers.")
                      versionDefault
                      NoDependencies
                      False
                      setUpgradeField
                , test
                      "WarnsWhenObserversOfTemplateChoiceAreChanged"
                      (SucceedWithWarning "\ESC\\[0;93mwarning while type checking template Main.A choice C:\n  The upgraded choice C has changed the definition of observers.")
                      versionDefault
                      NoDependencies
                      False
                      setUpgradeField
                , test
                      "FailsWhenTemplateChoiceChangesItsReturnType"
                      (FailWithError "\ESC\\[0;91merror type checking template Main.A choice C:\n  The upgraded choice C cannot change its return type.")
                      versionDefault
                      NoDependencies
                      False
                      setUpgradeField
                , test
                      "SucceedsWhenTemplateChoiceReturnsATemplateWhichHasChanged"
                      Succeed
                      versionDefault
                      NoDependencies
                      False
                      setUpgradeField
                , test
                      "SucceedsWhenTemplateChoiceInputArgumentHasChanged"
                      Succeed
                      versionDefault
                      NoDependencies
                      False
                      setUpgradeField
                , test
                      "SucceedsWhenNewFieldWithOptionalTypeIsAddedToTemplateChoice"
                      Succeed
                      versionDefault
                      NoDependencies
                      False
                      setUpgradeField
                , test
                      "FailsWhenATopLevelRecordAddsANonOptionalField"
                      (FailWithError "\ESC\\[0;91merror type checking data type Main.A:\n  The upgraded data type A has added new fields, but those fields are not Optional.")
                      versionDefault
                      NoDependencies
                      False
                      setUpgradeField
                , test
                      "SucceedsWhenATopLevelRecordAddsAnOptionalFieldAtTheEnd"
                      Succeed
                      versionDefault
                      NoDependencies
                      False
                      setUpgradeField
                , test
                      "FailsWhenATopLevelRecordAddsAnOptionalFieldBeforeTheEnd"
                      (FailWithError "\ESC\\[0;91merror type checking data type Main.A:\n  The upgraded data type A has changed the order of its fields - any new fields must be added at the end of the record.")
                      versionDefault
                      NoDependencies
                      False
                      setUpgradeField
                , test
                      "SucceedsWhenATopLevelVariantAddsAVariant"
                      Succeed
                      versionDefault
                      NoDependencies
                      False
                      setUpgradeField
                , test
                      "FailsWhenATopLevelVariantRemovesAVariant"
                      (FailWithError "\ESC\\[0;91merror type checking <none>:\n  Data type A.Z appears in package that is being upgraded, but does not appear in this package.")
                      versionDefault
                      NoDependencies
                      False
                      setUpgradeField
                , test
                      "FailWhenATopLevelVariantChangesChangesTheOrderOfItsVariants"
                      (FailWithError "\ESC\\[0;91merror type checking data type Main.A:\n  The upgraded data type A has changed the order of its variants - any new variant must be added at the end of the variant.")
                      versionDefault
                      NoDependencies
                      False
                      setUpgradeField
                , test
                      "FailsWhenATopLevelVariantAddsAFieldToAVariantsType"
                      (FailWithError "\ESC\\[0;91merror type checking data type Main.A:\n  The upgraded variant constructor Y from variant A has added a field.")
                      versionDefault
                      NoDependencies
                      False
                      setUpgradeField
                , test
                      "SucceedsWhenATopLevelVariantAddsAnOptionalFieldToAVariantsType"
                      Succeed
                      versionDefault
                      NoDependencies
                      False
                      setUpgradeField
                , test
                      "SucceedWhenATopLevelEnumAddsAField"
                      Succeed
                      versionDefault
                      NoDependencies
                      False
                      setUpgradeField
                , test
                      "FailWhenATopLevelEnumChangesChangesTheOrderOfItsVariants"
                      (FailWithError "\ESC\\[0;91merror type checking data type Main.A:\n  The upgraded data type A has changed the order of its variants - any new variant must be added at the end of the enum.")
                      versionDefault
                      NoDependencies
                      False
                      setUpgradeField
                , test
                      "SucceedsWhenATopLevelTypeSynonymChanges"
                      Succeed
                      versionDefault
                      NoDependencies
                      False
                      setUpgradeField
                , test
                      "SucceedsWhenTwoDeeplyNestedTypeSynonymsResolveToTheSameDatatypes"
                      Succeed
                      versionDefault
                      NoDependencies
                      False
                      setUpgradeField
                , test
                      "FailsWhenTwoDeeplyNestedTypeSynonymsResolveToDifferentDatatypes"
                      (FailWithError "\ESC\\[0;91merror type checking template Main.A :\n  The upgraded template A has changed the types of some of its original fields.")
                      versionDefault
                      NoDependencies
                      False
                      setUpgradeField
                , test
                      "SucceedsWhenAnInterfaceIsOnlyDefinedInTheInitialPackage"
                      Succeed
                      versionDefault
                      NoDependencies
                      False
                      setUpgradeField
                , test
                      "FailsWhenAnInterfaceIsDefinedInAnUpgradingPackageWhenItWasAlreadyInThePriorPackage"
                      (FailWithError "\ESC\\[0;91merror type checking interface Main.I :\n  Tried to upgrade interface I, but interfaces cannot be upgraded. They should be removed in any upgrading package.")
                      versionDefault
                      NoDependencies
                      False
                      setUpgradeField
                , test
                      "FailsWhenAnInstanceIsDropped"
                      (FailWithError "\ESC\\[0;91merror type checking template Main.T :\n  Implementation of interface I by template T appears in package that is being upgraded, but does not appear in this package.")
                      versionDefault
                      SeparateDep
                      False
                      setUpgradeField
                , test
                      "FailsWhenAnInstanceIsAddedSeparateDep"
                      (FailWithError "\ESC\\[0;91merror type checking template Main.T :\n  Implementation of interface I by template T appears in this package, but does not appear in package that is being upgraded.")
                      versionDefault
                      SeparateDep
                      False
                      setUpgradeField
                , test
                      "FailsWhenAnInstanceIsAddedUpgradedPackage"
                      (FailWithError "\ESC\\[0;91merror type checking template Main.T :\n  Implementation of interface I by template T appears in this package, but does not appear in package that is being upgraded.")
                      versionDefault
                      DependOnV1
                      True
                      setUpgradeField
                , test
                      "SucceedsWhenAnInstanceIsAddedToNewTemplateSeparateDep"
                      Succeed
                      versionDefault
                      SeparateDep
                      False
                      setUpgradeField
                , test
                      "SucceedsWhenAnInstanceIsAddedToNewTemplateUpgradedPackage"
                      Succeed
                      versionDefault
                      DependOnV1
                      True
                      setUpgradeField
                , test
                      "ValidUpgrade"
                      Succeed
                      versionDefault
                      NoDependencies
                      False
                      setUpgradeField
                , test
                      "MissingModule"
                      (FailWithError "\ESC\\[0;91merror type checking <none>:\n  Module Other appears in package that is being upgraded, but does not appear in this package.")
                      versionDefault
                      NoDependencies
                      False
                      setUpgradeField
                , test
                      "MissingTemplate"
                      (FailWithError "\ESC\\[0;91merror type checking <none>:\n  Template U appears in package that is being upgraded, but does not appear in this package.")
                      versionDefault
                      NoDependencies
                      False
                      setUpgradeField
                , test
                      "MissingDataCon"
                      (FailWithError "\ESC\\[0;91merror type checking <none>:\n  Data type U appears in package that is being upgraded, but does not appear in this package.")
                      versionDefault
                      NoDependencies
                      False
                      setUpgradeField
                , test
                      "MissingChoice"
                      (FailWithError "\ESC\\[0;91merror type checking template Main.T :\n  Choice C2 appears in package that is being upgraded, but does not appear in this package.")
                      versionDefault
                      NoDependencies
                      False
                      setUpgradeField
                , test
                      "TemplateChangedKeyType"
                      (FailWithError "\ESC\\[0;91merror type checking template Main.T key:\n  The upgraded template T cannot change its key type.")
                      versionDefault
                      NoDependencies
                      False
                      setUpgradeField
                , test
                      "TemplateChangedKeyType2"
                      Succeed
                      versionDefault
                      NoDependencies
                      False
                      setUpgradeField
                , test
                      "RecordFieldsNewNonOptional"
                      (FailWithError "\ESC\\[0;91merror type checking data type Main.Struct:\n  The upgraded data type Struct has added new fields, but those fields are not Optional.")
                      versionDefault
                      NoDependencies
                      False
                      setUpgradeField
                , test
                      "FailsWithSynonymReturnTypeChange"
                      (FailWithError "\ESC\\[0;91merror type checking template Main.T choice C:\n  The upgraded choice C cannot change its return type.")
                      versionDefault
                      NoDependencies
                      False
                      setUpgradeField
                , test
                      "FailsWithSynonymReturnTypeChangeInSeparatePackage"
                      (FailWithError "\ESC\\[0;91merror type checking template Main.T choice C:\n  The upgraded choice C cannot change its return type.")
                      versionDefault
                      (SeparateDeps False)
                      False
                      setUpgradeField
                , test
                      "SucceedsWhenUpgradingADependency"
                      Succeed
                      versionDefault
                      (SeparateDeps False)
                      False
                      setUpgradeField
                , test
                      "FailsOnlyInModuleNotInReexports"
                      (FailWithError "\ESC\\[0;91merror type checking data type Other.A:\n  The upgraded data type A has added new fields, but those fields are not Optional.")
                      versionDefault
                      NoDependencies
                      False
                      setUpgradeField
                , test
                      "FailsWhenDatatypeChangesVariety"
                      (FailWithError "\ESC\\[0;91merror type checking data type Main.RecordToEnum:\n  The upgraded data type RecordToEnum has changed from a record to a enum.")
                      versionDefault
                      NoDependencies
                      False
                      setUpgradeField
                , test
                      "FailsWhenDepsDowngradeVersionsWhileUsingDatatypes"
                      (FailWithError "\ESC\\[0;91merror type checking data type Main.Main:\n  The upgraded data type Main has changed the types of some of its original fields.")
                      versionDefault
                      (SeparateDeps True)
                      False
                      setUpgradeField
                , test
                      "SucceedsWhenDepsDowngradeVersionsWithoutUsingDatatypes"
                      Succeed
                      versionDefault
                      (SeparateDeps True)
                      False
                      setUpgradeField
                , test
                      "FailsWhenDependencyIsNotAValidUpgrade"
                      (FailWithError "\ESC\\[0;91merror while validating that dependency upgrades-example-FailsWhenDependencyIsNotAValidUpgrade-dep version 0.0.2 is a valid upgrade of version 0.0.1\n  error type checking data type Dep.Dep:\n    The upgraded data type Dep has added new fields, but those fields are not Optional.")
                      versionDefault
                      (SeparateDeps False)
                      False
                      setUpgradeField
                , testWithAdditionalDars
                      "FailsWhenUpgradedFieldPackagesAreNotUpgradable"
                      (FailWithError "\ESC\\[0;91merror type checking data type ProjectMain.T:\n  The upgraded data type T has changed the types of some of its original fields.")
                      versionDefault
                      NoDependencies
                      False
                      setUpgradeField
                      ["upgrades-SucceedsWhenATopLevelRecordAddsAnOptionalFieldAtTheEnd-v2.dar"] -- Note that dependencies are in different order
                      ["upgrades-SucceedsWhenATopLevelRecordAddsAnOptionalFieldAtTheEnd-v1.dar"]
                , testWithAdditionalDars
                      "FailsWhenUpgradedFieldFromDifferentPackageName"
                      (FailWithError "\ESC\\[0;91merror type checking data type Main.A:\n  The upgraded data type A has changed the types of some of its original fields.")
                      versionDefault
                      NoDependencies
                      False
                      setUpgradeField
                      ["upgrades-FailsWhenUpgradedFieldFromDifferentPackageName-dep-name1.dar"]
                      ["upgrades-FailsWhenUpgradedFieldFromDifferentPackageName-dep-name2.dar"]
                ]
            | setUpgradeField <- [True, False]
            ] ++
            concat [
                [ testGeneral
                      (prefix <> "WhenAnInterfaceAndATemplateAreDefinedInTheSamePackage")
                      "WarnsWhenAnInterfaceAndATemplateAreDefinedInTheSamePackage"
                      (expectation "type checking module Main:\n  This package defines both interfaces and templates.")
                      versionDefault
                      NoDependencies
                      warnBadInterfaceInstances
                      True
                      doTypecheck
                      []
                      []
                , testGeneral
                      (prefix <> "WhenAnInterfaceIsUsedInThePackageThatItsDefinedIn")
                      "WarnsWhenAnInterfaceIsUsedInThePackageThatItsDefinedIn"
                      (expectation "type checking interface Main.I :\n  The interface I was defined in this package and implemented in this package by the following templates:")
                      versionDefault
                      NoDependencies
                      warnBadInterfaceInstances
                      True
                      doTypecheck
                      []
                      []
                ]
            | warnBadInterfaceInstances <- [True, False]
            , let prefix = if warnBadInterfaceInstances then "Warns" else "Fail"
            , let expectation msg =
                      if warnBadInterfaceInstances
                         then SucceedWithWarning ("\ESC\\[0;93mwarning while " <> msg)
                         else FailWithError ("\ESC\\[0;91merror " <> msg)
            , doTypecheck <- [True, False]
            ]
       )
  where
    -- TODO: https://github.com/digital-asset/daml/issues/19862
    versionDefault :: LF.Version
    versionDefault = version1_dev

    test
        :: String
        -> Expectation
        -> LF.Version
        -> Dependency
        -> Bool
        -> Bool
        -> TestTree
    test name expectation lfVersion sharedDep warnBadInterfaceInstances setUpgradeField =
            testGeneral name name expectation lfVersion sharedDep warnBadInterfaceInstances setUpgradeField True [] []

    testWithAdditionalDars
        :: String
        -> Expectation
        -> LF.Version
        -> Dependency
        -> Bool
        -> Bool
        -> [String] -> [String]
        -> TestTree
    testWithAdditionalDars name expectation lfVersion sharedDep warnBadInterfaceInstances setUpgradeField additionalDarsV1 additionalDarsV2 =
            testGeneral name name expectation lfVersion sharedDep warnBadInterfaceInstances setUpgradeField True additionalDarsV1 additionalDarsV2

    testGeneral
        :: String
        -> String
        -> Expectation
        -> LF.Version
        -> Dependency
        -> Bool
        -> Bool
        -> Bool
        -> [String] -> [String]
        -> TestTree
    testGeneral name location expectation lfVersion sharedDep warnBadInterfaceInstances setUpgradeField doTypecheck additionalDarsV1 additionalDarsV2 =
        let upgradeFieldTrailer = if not setUpgradeField then " (no upgrades field)" else ""
            doTypecheckTrailer = if not doTypecheck then " (disable typechecking)" else ""
        in
        testCase (name <> upgradeFieldTrailer <> doTypecheckTrailer) $ do
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

            (depV1Dar, depV2Dar) <- case sharedDep of
              SeparateDep -> do
                depFilePaths <- listDirectory =<< testRunfile (location </> "dep")
                let sharedDepFiles = flip map depFilePaths $ \path ->
                      ( "daml" </> path
                      , readFile =<< testRunfile (location </> "dep" </> path)
                      )
                let sharedDir = dir </> "shared"
                let sharedDar = sharedDir </> "out.dar"
                writeFiles sharedDir (projectFile "0.0.1" ("upgrades-example-" <> location <> "-dep") Nothing Nothing [] : sharedDepFiles)
                callProcessSilent damlc ["build", "--project-root", sharedDir, "-o", sharedDar]
                pure (Just sharedDar, Just sharedDar)
              SeparateDeps { shouldSwap } -> do
                depV1FilePaths <- listDirectory =<< testRunfile (location </> "dep-v1")
                let depV1Files = flip map depV1FilePaths $ \path ->
                      ( "daml" </> path
                      , readFile =<< testRunfile (location </> "dep-v1" </> path)
                      )
                let depV1Dir = dir </> "shared-v1"
                let depV1Dar = depV1Dir </> "out.dar"
                writeFiles depV1Dir (projectFile "0.0.1" ("upgrades-example-" <> location <> "-dep") Nothing Nothing [] : depV1Files)
                callProcessSilent damlc ["build", "--project-root", depV1Dir, "-o", depV1Dar]

                depV2FilePaths <- listDirectory =<< testRunfile (location </> "dep-v2")
                let depV2Files = flip map depV2FilePaths $ \path ->
                      ( "daml" </> path
                      , readFile =<< testRunfile (location </> "dep-v2" </> path)
                      )
                let depV2Dir = dir </> "shared-v2"
                let depV2Dar = depV2Dir </> "out.dar"
                writeFiles depV2Dir (projectFile "0.0.2" ("upgrades-example-" <> location <> "-dep") Nothing Nothing [] : depV2Files)
                callProcessSilent damlc ["build", "--project-root", depV2Dir, "-o", depV2Dar]

                if shouldSwap
                   then pure (Just depV2Dar, Just depV1Dar)
                   else pure (Just depV1Dar, Just depV2Dar)
              DependOnV1 ->
                pure (Nothing, Just oldDar)
              _ ->
                pure (Nothing, Nothing)

            v1AdditionalDarsRunFiles <- traverse testAdditionaDarRunfile additionalDarsV1
            writeFiles oldDir (projectFile "0.0.1" ("upgrades-example-" <> location) Nothing depV1Dar v1AdditionalDarsRunFiles : oldVersion)
            callProcessSilent damlc ["build", "--project-root", oldDir, "-o", oldDar]

            v2AdditionalDarsRunFiles <- traverse testAdditionaDarRunfile additionalDarsV2
            writeFiles newDir (projectFile "0.0.2" ("upgrades-example-" <> location) (if setUpgradeField then Just oldDar else Nothing) depV2Dar v2AdditionalDarsRunFiles : newVersion)

            case expectation of
              Succeed ->
                  callProcessSilent damlc ["build", "--project-root", newDir, "-o", newDar]
              FailWithError _ | not (doTypecheck && setUpgradeField) ->
                  callProcessSilent damlc ["build", "--project-root", newDir, "-o", newDar]
              FailWithError regex -> do
                  stderr <- callProcessForStderr damlc ["build", "--project-root", newDir, "-o", newDar]
                  let regexWithSeverity = "Severity: DsError\nMessage: \n" <> regex
                  let compiledRegex :: Regex
                      compiledRegex = makeRegexOpts defaultCompOpt { multiline = False } defaultExecOpt regexWithSeverity
                  unless (matchTest compiledRegex stderr) $
                      assertFailure ("`daml build` failed as expected, but did not give an error matching '" <> show regexWithSeverity <> "':\n" <> show stderr)
              SucceedWithWarning regex -> do
                  stderr <- callProcessForSuccessfulStderr damlc ["build", "--project-root", newDir, "-o", newDar]
                  let regexWithSeverity = "Severity: DsWarning\nMessage: \n" <> regex
                  let compiledRegex :: Regex
                      compiledRegex = makeRegexOpts defaultCompOpt { multiline = False } defaultExecOpt regexWithSeverity
                  if setUpgradeField && doTypecheck
                      then unless (matchTest compiledRegex stderr) $
                            assertFailure ("`daml build` succeeded, but did not give a warning matching '" <> show regexWithSeverity <> "':\n" <> show stderr)
                      else when (matchTest compiledRegex stderr) $
                            assertFailure ("`daml build` succeeded, did not `upgrade:` field set, should NOT give a warning matching '" <> show regexWithSeverity <> "':\n" <> show stderr)
          where
          projectFile version name upgradedFile mbDep darDeps =
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
                  ++ ["  - --warn-bad-interface-instances=yes" | warnBadInterfaceInstances ]
                  ++ ["upgrades: '" <> path <> "'" | Just path <- pure upgradedFile]
                  ++ renderDataDeps (maybeToList mbDep ++ darDeps)
              )

          renderDataDeps :: [String] -> [String]
          renderDataDeps [] = []
          renderDataDeps paths =
            ["data-dependencies:"] ++ [" -  '" <> path <> "'" | path <- paths]

    writeFiles dir fs =
        for_ fs $ \(file, ioContent) -> do
            content <- ioContent
            createDirectoryIfMissing True (takeDirectory $ dir </> file)
            writeFileUTF8 (dir </> file) content

data Expectation
  = Succeed
  | FailWithError T.Text
  | SucceedWithWarning T.Text
  deriving (Show, Eq, Ord)

data Dependency
  = NoDependencies
  | DependOnV1
  | SeparateDep
  | SeparateDeps { shouldSwap :: Bool }
