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
import Text.Regex.TDFA
import qualified Data.Text as T
import Safe (fromJustNote)
import Prelude hiding (unlines)
import qualified Prelude

unlines :: [String] -> IO String
unlines = pure . Prelude.unlines

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
                LF.versionDefault
                DependOnV1
                True
                True
            ] ++
            concat [
                [ test
                      "WarnsWhenTemplateChangesSignatories"
                      (SucceedWithWarning "\ESC\\[0;93mwarning while type checking template Main.A signatories:\n  The upgraded template A has changed the definition of its signatories.")
                      LF.versionDefault
                      NoDependencies
                      False
                      setUpgradeField
                , test
                      "WarnsWhenTemplateChangesObservers"
                      (SucceedWithWarning "\ESC\\[0;93mwarning while type checking template Main.A observers:\n  The upgraded template A has changed the definition of its observers.")
                      LF.versionDefault
                      NoDependencies
                      False
                      setUpgradeField
                , test
                      "SucceedsWhenATopLevelEnumChanges"
                      Succeed
                      LF.versionDefault
                      NoDependencies
                      False
                      setUpgradeField
                , test
                      "WarnsWhenTemplateChangesEnsure"
                      (SucceedWithWarning "\ESC\\[0;93mwarning while type checking template Main.A precondition:\n  The upgraded template A has changed the definition of its precondition.")
                      LF.versionDefault
                      NoDependencies
                      False
                      setUpgradeField
                , test
                      "WarnsWhenTemplateChangesKeyExpression"
                      (SucceedWithWarning "\ESC\\[0;93mwarning while type checking template Main.A key:\n  The upgraded template A has changed the expression for computing its key.")
                      contractKeysMinVersion
                      NoDependencies
                      False
                      setUpgradeField
                , test
                      "WarnsWhenTemplateChangesKeyMaintainers"
                      (SucceedWithWarning "\ESC\\[0;93mwarning while type checking template Main.A key:\n  The upgraded template A has changed the maintainers for its key.")
                      contractKeysMinVersion
                      NoDependencies
                      False
                      setUpgradeField
                , test
                      "FailsWhenTemplateChangesKeyType"
                      (FailWithError "\ESC\\[0;91merror type checking template Main.A key:\n  The upgraded template A cannot change its key type.")
                      contractKeysMinVersion
                      NoDependencies
                      False
                      setUpgradeField
                , test
                      "FailsWhenTemplateRemovesKeyType"
                      (FailWithError "\ESC\\[0;91merror type checking template Main.A key:\n  The upgraded template A cannot remove its key.")
                      contractKeysMinVersion
                      NoDependencies
                      False
                      setUpgradeField
                , test
                      "FailsWhenTemplateAddsKeyType"
                      (FailWithError "\ESC\\[0;91merror type checking template Main.A key:\n  The upgraded template A cannot add a key where it didn't have one previously.")
                      contractKeysMinVersion
                      NoDependencies
                      False
                      setUpgradeField
                , test
                      "FailsWhenNewFieldIsAddedToTemplateWithoutOptionalType"
                      (FailWithError "\ESC\\[0;91merror type checking template Main.A :\n  The upgraded template A has added new fields, but those fields are not Optional.")
                      LF.versionDefault
                      NoDependencies
                      False
                      setUpgradeField
                , test
                      "FailsWhenOldFieldIsDeletedFromTemplate"
                      (FailWithError "\ESC\\[0;91merror type checking template Main.A :\n  The upgraded template A is missing some of its original fields.")
                      LF.versionDefault
                      NoDependencies
                      False
                      setUpgradeField
                , test
                      "FailsWhenExistingFieldInTemplateIsChanged"
                      (FailWithError "\ESC\\[0;91merror type checking template Main.A :\n  The upgraded template A has changed the types of some of its original fields.")
                      LF.versionDefault
                      NoDependencies
                      False
                      setUpgradeField
                , test
                      "SucceedsWhenNewFieldWithOptionalTypeIsAddedToTemplate"
                      Succeed
                      LF.versionDefault
                      NoDependencies
                      False
                      setUpgradeField
                , test
                      "FailsWhenNewFieldIsAddedToTemplateChoiceWithoutOptionalType"
                      (FailWithError "\ESC\\[0;91merror type checking template Main.A choice C:\n  The upgraded input type of choice C on template A has added new fields, but those fields are not Optional.")
                      LF.versionDefault
                      NoDependencies
                      False
                      setUpgradeField
                , test
                      "FailsWhenOldFieldIsDeletedFromTemplateChoice"
                      (FailWithError "\ESC\\[0;91merror type checking template Main.A choice C:\n  The upgraded input type of choice C on template A is missing some of its original fields.")
                      LF.versionDefault
                      NoDependencies
                      False
                      setUpgradeField
                , test
                      "FailsWhenExistingFieldInTemplateChoiceIsChanged"
                      (FailWithError "\ESC\\[0;91merror type checking template Main.A choice C:\n  The upgraded input type of choice C on template A has changed the types of some of its original fields.")
                      LF.versionDefault
                      NoDependencies
                      False
                      setUpgradeField
                , test
                      "WarnsWhenControllersOfTemplateChoiceAreChanged"
                      (SucceedWithWarning "\ESC\\[0;93mwarning while type checking template Main.A choice C:\n  The upgraded choice C has changed the definition of controllers.")
                      LF.versionDefault
                      NoDependencies
                      False
                      setUpgradeField
                , test
                      "WarnsWhenObserversOfTemplateChoiceAreChanged"
                      (SucceedWithWarning "\ESC\\[0;93mwarning while type checking template Main.A choice C:\n  The upgraded choice C has changed the definition of observers.")
                      LF.versionDefault
                      NoDependencies
                      False
                      setUpgradeField
                , test
                      "FailsWhenTemplateChoiceChangesItsReturnType"
                      (FailWithError "\ESC\\[0;91merror type checking template Main.A choice C:\n  The upgraded choice C cannot change its return type.")
                      LF.versionDefault
                      NoDependencies
                      False
                      setUpgradeField
                , test
                      "SucceedsWhenTemplateChoiceReturnsATemplateWhichHasChanged"
                      Succeed
                      LF.versionDefault
                      NoDependencies
                      False
                      setUpgradeField
                , test
                      "SucceedsWhenTemplateChoiceInputArgumentHasChanged"
                      Succeed
                      LF.versionDefault
                      NoDependencies
                      False
                      setUpgradeField
                , test
                      "SucceedsWhenNewFieldWithOptionalTypeIsAddedToTemplateChoice"
                      Succeed
                      LF.versionDefault
                      NoDependencies
                      False
                      setUpgradeField
                , test
                      "FailsWhenATopLevelRecordAddsANonOptionalField"
                      (FailWithError "\ESC\\[0;91merror type checking data type Main.A:\n  The upgraded data type A has added new fields, but those fields are not Optional.")
                      LF.versionDefault
                      NoDependencies
                      False
                      setUpgradeField
                , test
                      "SucceedsWhenATopLevelRecordAddsAnOptionalFieldAtTheEnd"
                      Succeed
                      LF.versionDefault
                      NoDependencies
                      False
                      setUpgradeField
                , test
                      "FailsWhenATopLevelRecordAddsAnOptionalFieldBeforeTheEnd"
                      (FailWithError "\ESC\\[0;91merror type checking data type Main.A:\n  The upgraded data type A has changed the order of its fields - any new fields must be added at the end of the record.")
                      LF.versionDefault
                      NoDependencies
                      False
                      setUpgradeField
                , test
                      "SucceedsWhenATopLevelVariantAddsAVariant"
                      Succeed
                      LF.versionDefault
                      NoDependencies
                      False
                      setUpgradeField
                , test
                      "FailsWhenATopLevelVariantRemovesAVariant"
                      (FailWithError "\ESC\\[0;91merror type checking <none>:\n  Data type A.Z appears in package that is being upgraded, but does not appear in this package.")
                      LF.versionDefault
                      NoDependencies
                      False
                      setUpgradeField
                , test
                      "FailWhenATopLevelVariantChangesChangesTheOrderOfItsVariants"
                      (FailWithError "\ESC\\[0;91merror type checking data type Main.A:\n  The upgraded data type A has changed the order of its variants - any new variant must be added at the end of the variant.")
                      LF.versionDefault
                      NoDependencies
                      False
                      setUpgradeField
                , test
                      "FailsWhenATopLevelVariantAddsAFieldToAVariantsType"
                      (FailWithError "\ESC\\[0;91merror type checking data type Main.A:\n  The upgraded variant constructor Y from variant A has added a field.")
                      LF.versionDefault
                      NoDependencies
                      False
                      setUpgradeField
                , test
                      "SucceedsWhenATopLevelVariantAddsAnOptionalFieldToAVariantsType"
                      Succeed
                      LF.versionDefault
                      NoDependencies
                      False
                      setUpgradeField
                , test
                      "SucceedWhenATopLevelEnumAddsAField"
                      Succeed
                      LF.versionDefault
                      NoDependencies
                      False
                      setUpgradeField
                , test
                      "FailWhenATopLevelEnumChangesChangesTheOrderOfItsVariants"
                      (FailWithError "\ESC\\[0;91merror type checking data type Main.A:\n  The upgraded data type A has changed the order of its variants - any new variant must be added at the end of the enum.")
                      LF.versionDefault
                      NoDependencies
                      False
                      setUpgradeField
                , test
                      "SucceedsWhenATopLevelTypeSynonymChanges"
                      Succeed
                      LF.versionDefault
                      NoDependencies
                      False
                      setUpgradeField
                , test
                      "SucceedsWhenTwoDeeplyNestedTypeSynonymsResolveToTheSameDatatypes"
                      Succeed
                      LF.versionDefault
                      NoDependencies
                      False
                      setUpgradeField
                , test
                      "FailsWhenTwoDeeplyNestedTypeSynonymsResolveToDifferentDatatypes"
                      (FailWithError "\ESC\\[0;91merror type checking template Main.A :\n  The upgraded template A has changed the types of some of its original fields.")
                      LF.versionDefault
                      NoDependencies
                      False
                      setUpgradeField
                , test
                      "SucceedsWhenAnInterfaceIsOnlyDefinedInTheInitialPackage"
                      Succeed
                      LF.versionDefault
                      NoDependencies
                      False
                      setUpgradeField
                , test
                      "FailsWhenAnInterfaceIsDefinedInAnUpgradingPackageWhenItWasAlreadyInThePriorPackage"
                      (FailWithError "\ESC\\[0;91merror type checking interface Main.I :\n  Tried to upgrade interface I, but interfaces cannot be upgraded. They should be removed in any upgrading package.")
                      LF.versionDefault
                      NoDependencies
                      False
                      setUpgradeField
                , test
                      "FailsWhenAnInstanceIsDropped"
                      (FailWithError "\ESC\\[0;91merror type checking template Main.T :\n  Implementation of interface I by template T appears in package that is being upgraded, but does not appear in this package.")
                      LF.versionDefault
                      SeparateDep
                      False
                      setUpgradeField
                , test
                      "SucceedsWhenAnInstanceIsAddedSeparateDep"
                      Succeed
                      LF.versionDefault
                      SeparateDep
                      False
                      setUpgradeField
                , test
                      "SucceedsWhenAnInstanceIsAddedUpgradedPackage"
                      Succeed
                      LF.versionDefault
                      DependOnV1
                      True
                      setUpgradeField
                , test
                      "ValidUpgrade"
                      Succeed
                      contractKeysMinVersion
                      NoDependencies
                      False
                      setUpgradeField
                , test
                      "MissingModule"
                      (FailWithError "\ESC\\[0;91merror type checking <none>:\n  Module Other appears in package that is being upgraded, but does not appear in this package.")
                      LF.versionDefault
                      NoDependencies
                      False
                      setUpgradeField
                , test
                      "MissingTemplate"
                      (FailWithError "\ESC\\[0;91merror type checking <none>:\n  Template U appears in package that is being upgraded, but does not appear in this package.")
                      LF.versionDefault
                      NoDependencies
                      False
                      setUpgradeField
                , test
                      "MissingDataCon"
                      (FailWithError "\ESC\\[0;91merror type checking <none>:\n  Data type U appears in package that is being upgraded, but does not appear in this package.")
                      LF.versionDefault
                      NoDependencies
                      False
                      setUpgradeField
                , test
                      "MissingChoice"
                      (FailWithError "\ESC\\[0;91merror type checking template Main.T :\n  Choice C2 appears in package that is being upgraded, but does not appear in this package.")
                      LF.versionDefault
                      NoDependencies
                      False
                      setUpgradeField
                , test
                      "TemplateChangedKeyType"
                      (FailWithError "\ESC\\[0;91merror type checking template Main.T key:\n  The upgraded template T cannot change its key type.")
                      contractKeysMinVersion
                      NoDependencies
                      False
                      setUpgradeField
                , test
                      "RecordFieldsNewNonOptional"
                      (FailWithError "\ESC\\[0;91merror type checking data type Main.Struct:\n  The upgraded data type Struct has added new fields, but those fields are not Optional.")
                      LF.versionDefault
                      NoDependencies
                      False
                      setUpgradeField
                , test
                      "FailsWithSynonymReturnTypeChange"
                      (FailWithError "\ESC\\[0;91merror type checking template Main.T choice C:\n  The upgraded choice C cannot change its return type.")
                      LF.versionDefault
                      NoDependencies
                      False
                      setUpgradeField
                , test
                      "FailsWithSynonymReturnTypeChangeInSeparatePackage"
                      (FailWithError "\ESC\\[0;91merror type checking template Main.T choice C:\n  The upgraded choice C cannot change its return type.")
                      LF.versionDefault
                      SeparateDeps
                      False
                      setUpgradeField
                , test
                      "SucceedsWhenUpgradingADependency"
                      Succeed
                      LF.versionDefault
                      SeparateDeps
                      False
                      setUpgradeField
                , test
                      "FailsOnlyInModuleNotInReexports"
                      (FailWithError "\ESC\\[0;91merror type checking data type Other.A:\n  The upgraded data type A has added new fields, but those fields are not Optional.")
                      LF.versionDefault
                      NoDependencies
                      False
                      setUpgradeField
                , test
                      "SucceedsWhenAddingNonOptionalFieldsToUnserializableTypes"
                      Succeed
                      LF.versionDefault
                      NoDependencies
                      False
                      setUpgradeField
                , test
                      "FailsWhenMakingTypeUnserializable"
                      (FailWithError "\ESC\\[0;91merror type checking data type Main.MyData:\n  The upgraded data type MyData was serializable and is now unserializable. Datatypes cannot change their serializability via upgrades.")
                      LF.versionDefault
                      NoDependencies
                      False
                      setUpgradeField
                , test
                      "FailsWhenMakingTypeSerializable"
                      (FailWithError "\ESC\\[0;91merror type checking data type Main.MyData:\n  The upgraded data type MyData was unserializable and is now serializable. Datatypes cannot change their serializability via upgrades.")
                      LF.versionDefault
                      NoDependencies
                      False
                      setUpgradeField
                ]
            | setUpgradeField <- [True, False]
            ] ++
            concat [
                [ testGeneral
                      (prefix <> "WhenAnInterfaceAndATemplateAreDefinedInTheSamePackage")
                      "WarnsWhenAnInterfaceAndATemplateAreDefinedInTheSamePackage"
                      (expectation "type checking module Main:\n  This package defines both interfaces and templates.")
                      LF.versionDefault
                      NoDependencies
                      warnBadInterfaceInstances
                      True
                      doTypecheck
                , testGeneral
                      (prefix <> "WhenAnInterfaceIsUsedInThePackageThatItsDefinedIn")
                      "WarnsWhenAnInterfaceIsUsedInThePackageThatItsDefinedIn"
                      (expectation "type checking interface Main.I :\n  The interface I was defined in this package and implemented in this package by the following templates:")
                      LF.versionDefault
                      NoDependencies
                      warnBadInterfaceInstances
                      True
                      doTypecheck
                , testGeneral
                      (prefix <> "WhenAnInterfaceIsDefinedAndThenUsedInAPackageThatUpgradesIt")
                      "WarnsWhenAnInterfaceIsDefinedAndThenUsedInAPackageThatUpgradesIt"
                      (expectation "type checking template Main.T interface instance [0-9a-f]+:Main:I for Main:T:\n  The template T has implemented interface I, which is defined in a previous version of this package.")
                      LF.versionDefault
                      DependOnV1
                      warnBadInterfaceInstances
                      True
                      doTypecheck
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
    contractKeysMinVersion :: LF.Version
    contractKeysMinVersion = 
        fromJustNote
            "Expected at least one LF 2.x version to support contract keys." 
            (LF.featureMinVersion LF.featureContractKeys LF.V2)

    test
        :: String
        -> Expectation
        -> LF.Version
        -> Dependency
        -> Bool
        -> Bool
        -> TestTree
    test name expectation lfVersion sharedDep warnBadInterfaceInstances setUpgradeField =
            testGeneral name name expectation lfVersion sharedDep warnBadInterfaceInstances setUpgradeField True

    testGeneral
        :: String
        -> String
        -> Expectation
        -> LF.Version
        -> Dependency
        -> Bool
        -> Bool
        -> Bool
        -> TestTree
    testGeneral name location expectation lfVersion sharedDep warnBadInterfaceInstances setUpgradeField doTypecheck =
        let upgradeFieldTrailer = if not setUpgradeField then " (no upgrades field)" else ""
            doTypecheckTrailer = if not doTypecheck then " (disable typechecking)" else ""
        in
        testCase (name <> upgradeFieldTrailer <> doTypecheckTrailer) $
        withTempDir $ \dir -> do
            let newDir = dir </> "newVersion"
            let oldDir = dir </> "oldVersion"
            let newDar = newDir </> "out.dar"
            let oldDar = oldDir </> "old.dar"

            let testRunfile path = locateRunfiles (mainWorkspace </> "test-common/src/main/daml/upgrades" </> path)

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
                writeFiles sharedDir (projectFile ("upgrades-example-" <> location <> "-dep") Nothing Nothing : sharedDepFiles)
                callProcessSilent damlc ["build", "--project-root", sharedDir, "-o", sharedDar]
                pure (Just sharedDar, Just sharedDar)
              SeparateDeps -> do
                depV1FilePaths <- listDirectory =<< testRunfile (location </> "dep-v1")
                let depV1Files = flip map depV1FilePaths $ \path ->
                      ( "daml" </> path
                      , readFile =<< testRunfile (location </> "dep-v1" </> path)
                      )
                let depV1Dir = dir </> "shared-v1"
                let depV1Dar = depV1Dir </> "out.dar"
                writeFiles depV1Dir (projectFile ("upgrades-example-" <> location <> "-dep-v1") Nothing Nothing : depV1Files)
                callProcessSilent damlc ["build", "--project-root", depV1Dir, "-o", depV1Dar]

                depV2FilePaths <- listDirectory =<< testRunfile (location </> "dep-v2")
                let depV2Files = flip map depV2FilePaths $ \path ->
                      ( "daml" </> path
                      , readFile =<< testRunfile (location </> "dep-v2" </> path)
                      )
                let depV2Dir = dir </> "shared-v2"
                let depV2Dar = depV2Dir </> "out.dar"
                writeFiles depV2Dir (projectFile ("upgrades-example-" <> location <> "-dep-v2") Nothing Nothing : depV2Files)
                callProcessSilent damlc ["build", "--project-root", depV2Dir, "-o", depV2Dar]

                pure (Just depV1Dar, Just depV2Dar)
              DependOnV1 ->
                pure (Nothing, Just oldDar)
              _ ->
                pure (Nothing, Nothing)

            writeFiles oldDir (projectFile ("upgrades-example-" <> location) Nothing depV1Dar : oldVersion)
            callProcessSilent damlc ["build", "--project-root", oldDir, "-o", oldDar]

            writeFiles newDir (projectFile ("upgrades-example-" <> location <> "-v2") (if setUpgradeField then Just oldDar else Nothing) depV2Dar : newVersion)

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
          projectFile name upgradedFile mbDep =
              ( "daml.yaml"
              , unlines $
                [ "sdk-version: " <> sdkVersion
                , "name: " <> name
                , "source: daml"
                , "version: 0.0.1"
                , "dependencies:"
                , "  - daml-prim"
                , "  - daml-stdlib"
                , "build-options:"
                , "  - --target=" <> LF.renderVersion lfVersion
                , "  - --enable-interfaces=yes"
                ]
                  ++ ["  - --warn-bad-interface-instances=yes" | warnBadInterfaceInstances ]
                  ++ ["upgrades: '" <> path <> "'" | Just path <- pure upgradedFile]
                  ++ ["data-dependencies:\n -  '" <> path <> "'" | Just path <- pure mbDep]
                  ++ ["typecheck-upgrades: False" | not doTypecheck]
              )

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
  | SeparateDeps
