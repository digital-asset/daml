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
        [ test
              "WarnsWhenTemplateChangesSignatories"
              (SucceedWithWarning "\ESC\\[0;93mwarning while type checking template Main.A signatories:\n  The upgraded template A has changed the definition of its signatories.")
              LF.versionDefault
              NoDependencies
              [ ( "daml/Main.daml"
                , readFile =<< locateRunfiles (mainWorkspace </> "test-common/src/main/daml/upgrades/WarnsWhenTemplateChangesSignatories/v1/Main.daml")
                )
              ]
              [ ("daml/Main.daml"
                , readFile =<< locateRunfiles (mainWorkspace </> "test-common/src/main/daml/upgrades/WarnsWhenTemplateChangesSignatories/v2/Main.daml")
                )
              ]
        , test
              "WarnsWhenTemplateChangesObservers"
              (SucceedWithWarning "\ESC\\[0;93mwarning while type checking template Main.A observers:\n  The upgraded template A has changed the definition of its observers.")
              LF.versionDefault
              NoDependencies
              [ ( "daml/Main.daml"
                , readFile =<< locateRunfiles (mainWorkspace </> "test-common/src/main/daml/upgrades/WarnsWhenTemplateChangesObservers/v1/Main.daml")
                )
              ]
              [ ("daml/Main.daml"
                , readFile =<< locateRunfiles (mainWorkspace </> "test-common/src/main/daml/upgrades/WarnsWhenTemplateChangesObservers/v2/Main.daml")
                )
              ]
        , test
              "SucceedsWhenATopLevelEnumChanges"
              Succeed
              LF.versionDefault
              NoDependencies
              [ ( "daml/Main.daml"
                , readFile =<< locateRunfiles (mainWorkspace </> "test-common/src/main/daml/upgrades/FailsWhenATopLevelEnumChanges/v1/Main.daml")
                )
              ]
              [ ("daml/Main.daml"
                , readFile =<< locateRunfiles (mainWorkspace </> "test-common/src/main/daml/upgrades/FailsWhenATopLevelEnumChanges/v2/Main.daml")
                )
              ]
        , test
              "WarnsWhenTemplateChangesEnsure"
              (SucceedWithWarning "\ESC\\[0;93mwarning while type checking template Main.A precondition:\n  The upgraded template A has changed the definition of its precondition.")
              LF.versionDefault
              NoDependencies
              [ ( "daml/Main.daml"
                , readFile =<< locateRunfiles (mainWorkspace </> "test-common/src/main/daml/upgrades/WarnsWhenTemplateChangesEnsure/v1/Main.daml")
                )
              ]
              [ ("daml/Main.daml"
                , readFile =<< locateRunfiles (mainWorkspace </> "test-common/src/main/daml/upgrades/WarnsWhenTemplateChangesEnsure/v2/Main.daml")
                )
              ]
        , test
              "WarnsWhenTemplateChangesKeyExpression"
              (SucceedWithWarning "\ESC\\[0;93mwarning while type checking template Main.A key:\n  The upgraded template A has changed the expression for computing its key.")
              contractKeysMinVersion
              NoDependencies
              [ ( "daml/Main.daml"
                , readFile =<< locateRunfiles (mainWorkspace </> "test-common/src/main/daml/upgrades/WarnsWhenTemplateChangesKeyExpression/v1/Main.daml")
                )
              ]
              [ ("daml/Main.daml"
                , readFile =<< locateRunfiles (mainWorkspace </> "test-common/src/main/daml/upgrades/WarnsWhenTemplateChangesKeyExpression/v2/Main.daml")
                )
              ]
        , test
              "WarnsWhenTemplateChangesKeyMaintainers"
              (SucceedWithWarning "\ESC\\[0;93mwarning while type checking template Main.A key:\n  The upgraded template A has changed the maintainers for its key.")
              contractKeysMinVersion
              NoDependencies
              [ ( "daml/Main.daml"
                , readFile =<< locateRunfiles (mainWorkspace </> "test-common/src/main/daml/upgrades/WarnsWhenTemplateChangesKeyMaintainers/v1/Main.daml")
                )
              ]
              [ ("daml/Main.daml"
                , readFile =<< locateRunfiles (mainWorkspace </> "test-common/src/main/daml/upgrades/WarnsWhenTemplateChangesKeyMaintainers/v2/Main.daml")
                )
              ]
        , test
              "FailsWhenTemplateChangesKeyType"
              (FailWithError "\ESC\\[0;91merror type checking template Main.A key:\n  The upgraded template A cannot change its key type.")
              contractKeysMinVersion
              NoDependencies
              [ ( "daml/Main.daml"
                , readFile =<< locateRunfiles (mainWorkspace </> "test-common/src/main/daml/upgrades/FailsWhenTemplateChangesKeyType/v1/Main.daml")
                )
              ]
              [ ("daml/Main.daml"
                , readFile =<< locateRunfiles (mainWorkspace </> "test-common/src/main/daml/upgrades/FailsWhenTemplateChangesKeyType/v2/Main.daml")
                )
              ]
        , test
              "FailsWhenTemplateRemovesKeyType"
              (FailWithError "\ESC\\[0;91merror type checking template Main.A key:\n  The upgraded template A cannot remove its key.")
              contractKeysMinVersion
              NoDependencies
              [ ( "daml/Main.daml"
                , readFile =<< locateRunfiles (mainWorkspace </> "test-common/src/main/daml/upgrades/FailsWhenTemplateRemovesKeyType/v1/Main.daml")
                )
              ]
              [ ("daml/Main.daml"
                , readFile =<< locateRunfiles (mainWorkspace </> "test-common/src/main/daml/upgrades/FailsWhenTemplateRemovesKeyType/v2/Main.daml")
                )
              ]
        , test
              "FailsWhenTemplateAddsKeyType"
              (FailWithError "\ESC\\[0;91merror type checking template Main.A key:\n  The upgraded template A cannot add a key where it didn't have one previously.")
              contractKeysMinVersion
              NoDependencies
              [ ( "daml/Main.daml"
                , readFile =<< locateRunfiles (mainWorkspace </> "test-common/src/main/daml/upgrades/FailsWhenTemplateAddsKeyType/v1/Main.daml")
                )
              ]
              [ ("daml/Main.daml"
                , readFile =<< locateRunfiles (mainWorkspace </> "test-common/src/main/daml/upgrades/FailsWhenTemplateAddsKeyType/v2/Main.daml")
                )
              ]
        , test
              "FailsWhenNewFieldIsAddedToTemplateWithoutOptionalType"
              (FailWithError "\ESC\\[0;91merror type checking template Main.A :\n  The upgraded template A has added new fields, but those fields are not Optional.")
              LF.versionDefault
              NoDependencies
              [ ( "daml/Main.daml"
                , readFile =<< locateRunfiles (mainWorkspace </> "test-common/src/main/daml/upgrades/FailsWhenNewFieldIsAddedToTemplateWithoutOptionalType/v1/Main.daml")
                )
              ]
              [ ("daml/Main.daml"
                , readFile =<< locateRunfiles (mainWorkspace </> "test-common/src/main/daml/upgrades/FailsWhenNewFieldIsAddedToTemplateWithoutOptionalType/v2/Main.daml")
                )
              ]
        , test
              "FailsWhenOldFieldIsDeletedFromTemplate"
              (FailWithError "\ESC\\[0;91merror type checking template Main.A :\n  The upgraded template A is missing some of its original fields.")
              LF.versionDefault
              NoDependencies
              [ ( "daml/Main.daml"
                , readFile =<< locateRunfiles (mainWorkspace </> "test-common/src/main/daml/upgrades/FailsWhenOldFieldIsDeletedFromTemplate/v1/Main.daml")
                )
              ]
              [ ("daml/Main.daml"
                , readFile =<< locateRunfiles (mainWorkspace </> "test-common/src/main/daml/upgrades/FailsWhenOldFieldIsDeletedFromTemplate/v2/Main.daml")
                )
              ]
        , test
              "FailsWhenExistingFieldInTemplateIsChanged"
              (FailWithError "\ESC\\[0;91merror type checking template Main.A :\n  The upgraded template A has changed the types of some of its original fields.")
              LF.versionDefault
              NoDependencies
              [ ( "daml/Main.daml"
                , readFile =<< locateRunfiles (mainWorkspace </> "test-common/src/main/daml/upgrades/FailsWhenExistingFieldInTemplateIsChanged/v1/Main.daml")
                )
              ]
              [ ("daml/Main.daml"
                , readFile =<< locateRunfiles (mainWorkspace </> "test-common/src/main/daml/upgrades/FailsWhenExistingFieldInTemplateIsChanged/v2/Main.daml")
                )
              ]
        , test
              "SucceedsWhenNewFieldWithOptionalTypeIsAddedToTemplate"
              Succeed
              LF.versionDefault
              NoDependencies
              [ ( "daml/Main.daml"
                , readFile =<< locateRunfiles (mainWorkspace </> "test-common/src/main/daml/upgrades/SucceedsWhenNewFieldWithOptionalTypeIsAddedToTemplate/v1/Main.daml")
                )
              ]
              [ ("daml/Main.daml"
                , readFile =<< locateRunfiles (mainWorkspace </> "test-common/src/main/daml/upgrades/SucceedsWhenNewFieldWithOptionalTypeIsAddedToTemplate/v2/Main.daml")
                )
              ]
        , test
              "FailsWhenNewFieldIsAddedToTemplateChoiceWithoutOptionalType"
              (FailWithError "\ESC\\[0;91merror type checking template Main.A choice C:\n  The upgraded input type of choice C on template A has added new fields, but those fields are not Optional.")
              LF.versionDefault
              NoDependencies
              [ ( "daml/Main.daml"
                , readFile =<< locateRunfiles (mainWorkspace </> "test-common/src/main/daml/upgrades/FailsWhenNewFieldIsAddedToTemplateChoiceWithoutOptionalType/v1/Main.daml")
                )
              ]
              [ ("daml/Main.daml"
                , readFile =<< locateRunfiles (mainWorkspace </> "test-common/src/main/daml/upgrades/FailsWhenNewFieldIsAddedToTemplateChoiceWithoutOptionalType/v2/Main.daml")
                )
              ]
        , test
              "FailsWhenOldFieldIsDeletedFromTemplateChoice"
              (FailWithError "\ESC\\[0;91merror type checking template Main.A choice C:\n  The upgraded input type of choice C on template A is missing some of its original fields.")
              LF.versionDefault
              NoDependencies
              [ ( "daml/Main.daml"
                , readFile =<< locateRunfiles (mainWorkspace </> "test-common/src/main/daml/upgrades/FailsWhenOldFieldIsDeletedFromTemplateChoice/v1/Main.daml")
                )
              ]
              [ ("daml/Main.daml"
                , readFile =<< locateRunfiles (mainWorkspace </> "test-common/src/main/daml/upgrades/FailsWhenOldFieldIsDeletedFromTemplateChoice/v2/Main.daml")
                )
              ]
        , test
              "FailsWhenExistingFieldInTemplateChoiceIsChanged"
              (FailWithError "\ESC\\[0;91merror type checking template Main.A choice C:\n  The upgraded input type of choice C on template A has changed the types of some of its original fields.")
              LF.versionDefault
              NoDependencies
              [ ( "daml/Main.daml"
                , readFile =<< locateRunfiles (mainWorkspace </> "test-common/src/main/daml/upgrades/FailsWhenExistingFieldInTemplateChoiceIsChanged/v1/Main.daml")
                )
              ]
              [ ( "daml/Main.daml"
                , readFile =<< locateRunfiles (mainWorkspace </> "test-common/src/main/daml/upgrades/FailsWhenExistingFieldInTemplateChoiceIsChanged/v2/Main.daml")
                )
              ]
        , test
              "WarnsWhenControllersOfTemplateChoiceAreChanged"
              (SucceedWithWarning "\ESC\\[0;93mwarning while type checking template Main.A choice C:\n  The upgraded choice C has changed the definition of controllers.")
              LF.versionDefault
              NoDependencies
              [ ( "daml/Main.daml"
                , readFile =<< locateRunfiles (mainWorkspace </> "test-common/src/main/daml/upgrades/WarnsWhenControllersOfTemplateChoiceAreChanged/v1/Main.daml")
                )
              ]
              [ ("daml/Main.daml"
                , readFile =<< locateRunfiles (mainWorkspace </> "test-common/src/main/daml/upgrades/WarnsWhenControllersOfTemplateChoiceAreChanged/v2/Main.daml")
                )
              ]
        , test
              "WarnsWhenObserversOfTemplateChoiceAreChanged"
              (SucceedWithWarning "\ESC\\[0;93mwarning while type checking template Main.A choice C:\n  The upgraded choice C has changed the definition of observers.")
              LF.versionDefault
              NoDependencies
              [ ( "daml/Main.daml"
                , readFile =<< locateRunfiles (mainWorkspace </> "test-common/src/main/daml/upgrades/WarnsWhenObserversOfTemplateChoiceAreChanged/v1/Main.daml")
                )
              ]
              [ ("daml/Main.daml"
                , readFile =<< locateRunfiles (mainWorkspace </> "test-common/src/main/daml/upgrades/WarnsWhenObserversOfTemplateChoiceAreChanged/v2/Main.daml")
                )
              ]
        , test
              "FailsWhenTemplateChoiceChangesItsReturnType"
              (FailWithError "\ESC\\[0;91merror type checking template Main.A choice C:\n  The upgraded choice C cannot change its return type.")
              LF.versionDefault
              NoDependencies
              [ ( "daml/Main.daml"
                , readFile =<< locateRunfiles (mainWorkspace </> "test-common/src/main/daml/upgrades/FailsWhenTemplateChoiceChangesItsReturnType/v1/Main.daml")
                )
              ]
              [ ("daml/Main.daml"
                , readFile =<< locateRunfiles (mainWorkspace </> "test-common/src/main/daml/upgrades/FailsWhenTemplateChoiceChangesItsReturnType/v2/Main.daml")
                )
              ]
        , test
              "SucceedsWhenTemplateChoiceReturnsATemplateWhichHasChanged"
              Succeed
              LF.versionDefault
              NoDependencies
              [ ( "daml/Main.daml"
                , readFile =<< locateRunfiles (mainWorkspace </> "test-common/src/main/daml/upgrades/SucceedsWhenTemplateChoiceReturnsATemplateWhichHasChanged/v1/Main.daml")
                )
              ]
              [ ("daml/Main.daml"
                , readFile =<< locateRunfiles (mainWorkspace </> "test-common/src/main/daml/upgrades/SucceedsWhenTemplateChoiceReturnsATemplateWhichHasChanged/v2/Main.daml")
                )
              ]
        , test
              "SucceedsWhenTemplateChoiceInputArgumentHasChanged"
              Succeed
              LF.versionDefault
              NoDependencies
              [ ( "daml/Main.daml"
                , readFile =<< locateRunfiles (mainWorkspace </> "test-common/src/main/daml/upgrades/SucceedsWhenTemplateChoiceInputArgumentHasChanged/v1/Main.daml")
                )
              ]
              [ ("daml/Main.daml"
                , readFile =<< locateRunfiles (mainWorkspace </> "test-common/src/main/daml/upgrades/SucceedsWhenTemplateChoiceInputArgumentHasChanged/v2/Main.daml")
                )
              ]
        , test
              "SucceedsWhenNewFieldWithOptionalTypeIsAddedToTemplateChoice"
              Succeed
              LF.versionDefault
              NoDependencies
              [ ( "daml/Main.daml"
                , readFile =<< locateRunfiles (mainWorkspace </> "test-common/src/main/daml/upgrades/SucceedsWhenNewFieldWithOptionalTypeIsAddedToTemplateChoice/v1/Main.daml")
                )
              ]
              [ ("daml/Main.daml"
                , readFile =<< locateRunfiles (mainWorkspace </> "test-common/src/main/daml/upgrades/SucceedsWhenNewFieldWithOptionalTypeIsAddedToTemplateChoice/v2/Main.daml")
                )
              ]
        , test
              "FailsWhenATopLevelRecordAddsANonOptionalField"
              (FailWithError "\ESC\\[0;91merror type checking data type Main.A:\n  The upgraded data type A has added new fields, but those fields are not Optional.")
              LF.versionDefault
              NoDependencies
              [ ( "daml/Main.daml"
                , readFile =<< locateRunfiles (mainWorkspace </> "test-common/src/main/daml/upgrades/FailsWhenATopLevelRecordAddsANonOptionalField/v1/Main.daml")
                )
              ]
              [ ("daml/Main.daml"
                , readFile =<< locateRunfiles (mainWorkspace </> "test-common/src/main/daml/upgrades/FailsWhenATopLevelRecordAddsANonOptionalField/v2/Main.daml")
                )
              ]
        , test
              "SucceedsWhenATopLevelRecordAddsAnOptionalFieldAtTheEnd"
              Succeed
              LF.versionDefault
              NoDependencies
              [ ( "daml/Main.daml"
                , readFile =<< locateRunfiles (mainWorkspace </> "test-common/src/main/daml/upgrades/SucceedsWhenATopLevelRecordAddsAnOptionalFieldAtTheEnd/v1/Main.daml")
                )
              ]
              [ ("daml/Main.daml"
                , readFile =<< locateRunfiles (mainWorkspace </> "test-common/src/main/daml/upgrades/SucceedsWhenATopLevelRecordAddsAnOptionalFieldAtTheEnd/v2/Main.daml")
                )
              ]
        , test
              "FailsWhenATopLevelRecordAddsAnOptionalFieldBeforeTheEnd"
              (FailWithError "\ESC\\[0;91merror type checking data type Main.A:\n  The upgraded data type A has changed the order of its fields - any new fields must be added at the end of the record.")
              LF.versionDefault
              NoDependencies
              [ ( "daml/Main.daml"
                , readFile =<< locateRunfiles (mainWorkspace </> "test-common/src/main/daml/upgrades/FailsWhenATopLevelRecordAddsAnOptionalFieldBeforeTheEnd/v1/Main.daml")
                )
              ]
              [ ("daml/Main.daml"
                , readFile =<< locateRunfiles (mainWorkspace </> "test-common/src/main/daml/upgrades/FailsWhenATopLevelRecordAddsAnOptionalFieldBeforeTheEnd/v2/Main.daml")
                )
              ]
        , test
              "SucceedsWhenATopLevelVariantAddsAVariant"
              Succeed
              LF.versionDefault
              NoDependencies
              [ ( "daml/Main.daml"
                , readFile =<< locateRunfiles (mainWorkspace </> "test-common/src/main/daml/upgrades/SucceedsWhenATopLevelVariantAddsAVariant/v1/Main.daml")
                )
              ]
              [ ("daml/Main.daml"
                , readFile =<< locateRunfiles (mainWorkspace </> "test-common/src/main/daml/upgrades/SucceedsWhenATopLevelVariantAddsAVariant/v2/Main.daml")
                )
              ]
        , test
              "FailsWhenATopLevelVariantRemovesAVariant"
              (FailWithError "\ESC\\[0;91merror type checking <none>:\n  Data type A.Z appears in package that is being upgraded, but does not appear in this package.")
              LF.versionDefault
              NoDependencies
              [ ( "daml/Main.daml"
                , readFile =<< locateRunfiles (mainWorkspace </> "test-common/src/main/daml/upgrades/FailsWhenATopLevelVariantRemovesAVariant/v1/Main.daml")
                )
              ]
              [ ("daml/Main.daml"
                , readFile =<< locateRunfiles (mainWorkspace </> "test-common/src/main/daml/upgrades/FailsWhenATopLevelVariantRemovesAVariant/v2/Main.daml")
                )
              ]
        , test
              "FailWhenATopLevelVariantChangesChangesTheOrderOfItsVariants"
              (FailWithError "\ESC\\[0;91merror type checking data type Main.A:\n  The upgraded data type A has changed the order of its variants - any new variant must be added at the end of the variant.")
              LF.versionDefault
              NoDependencies
              [ ( "daml/Main.daml"
                , readFile =<< locateRunfiles (mainWorkspace </> "test-common/src/main/daml/upgrades/FailWhenATopLevelVariantChangesChangesTheOrderOfItsVariants/v1/Main.daml")
                )
              ]
              [ ("daml/Main.daml"
                , readFile =<< locateRunfiles (mainWorkspace </> "test-common/src/main/daml/upgrades/FailWhenATopLevelVariantChangesChangesTheOrderOfItsVariants/v2/Main.daml")
                )
              ]
        , test
              "FailsWhenATopLevelVariantAddsAFieldToAVariantsType"
              (FailWithError "\ESC\\[0;91merror type checking data type Main.A:\n  The upgraded variant constructor Y from variant A has added a field.")
              LF.versionDefault
              NoDependencies
              [ ( "daml/Main.daml"
                , readFile =<< locateRunfiles (mainWorkspace </> "test-common/src/main/daml/upgrades/FailsWhenATopLevelVariantAddsAFieldToAVariantsType/v1/Main.daml")
                )
              ]
              [ ("daml/Main.daml"
                , readFile =<< locateRunfiles (mainWorkspace </> "test-common/src/main/daml/upgrades/FailsWhenATopLevelVariantAddsAFieldToAVariantsType/v2/Main.daml")
                )
              ]
        , test
              "SucceedsWhenATopLevelVariantAddsAnOptionalFieldToAVariantsType"
              Succeed
              LF.versionDefault
              NoDependencies
              [ ( "daml/Main.daml"
                , readFile =<< locateRunfiles (mainWorkspace </> "test-common/src/main/daml/upgrades/SucceedsWhenATopLevelVariantAddsAnOptionalFieldToAVariantsType/v1/Main.daml")
                )
              ]
              [ ("daml/Main.daml"
                , readFile =<< locateRunfiles (mainWorkspace </> "test-common/src/main/daml/upgrades/SucceedsWhenATopLevelVariantAddsAnOptionalFieldToAVariantsType/v2/Main.daml")
                )
              ]
        , test
              "SucceedWhenATopLevelEnumAddsAField"
              Succeed
              LF.versionDefault
              NoDependencies
              [ ( "daml/Main.daml"
                , readFile =<< locateRunfiles (mainWorkspace </> "test-common/src/main/daml/upgrades/SucceedWhenATopLevelEnumAddsAField/v1/Main.daml")
                )
              ]
              [ ("daml/Main.daml"
                , readFile =<< locateRunfiles (mainWorkspace </> "test-common/src/main/daml/upgrades/SucceedWhenATopLevelEnumAddsAField/v2/Main.daml")
                )
              ]
        , test
              "FailWhenATopLevelEnumChangesChangesTheOrderOfItsVariants"
              (FailWithError "\ESC\\[0;91merror type checking data type Main.A:\n  The upgraded data type A has changed the order of its variants - any new variant must be added at the end of the enum.")
              LF.versionDefault
              NoDependencies
              [ ( "daml/Main.daml"
                , readFile =<< locateRunfiles (mainWorkspace </> "test-common/src/main/daml/upgrades/FailWhenATopLevelEnumChangesChangesTheOrderOfItsVariants/v1/Main.daml")
                )
              ]
              [ ("daml/Main.daml"
                , readFile =<< locateRunfiles (mainWorkspace </> "test-common/src/main/daml/upgrades/FailWhenATopLevelEnumChangesChangesTheOrderOfItsVariants/v2/Main.daml")
                )
              ]
        , test
              "SucceedsWhenATopLevelTypeSynonymChanges"
              Succeed
              LF.versionDefault
              NoDependencies
              [ ( "daml/Main.daml"
                , readFile =<< locateRunfiles (mainWorkspace </> "test-common/src/main/daml/upgrades/SucceedsWhenATopLevelTypeSynonymChanges/v1/Main.daml")
                )
              ]
              [ ("daml/Main.daml"
                , readFile =<< locateRunfiles (mainWorkspace </> "test-common/src/main/daml/upgrades/SucceedsWhenATopLevelTypeSynonymChanges/v2/Main.daml")
                )
              ]
        , test
              "SucceedsWhenTwoDeeplyNestedTypeSynonymsResolveToTheSameDatatypes"
              Succeed
              LF.versionDefault
              NoDependencies
              [ ( "daml/Main.daml"
                , readFile =<< locateRunfiles (mainWorkspace </> "test-common/src/main/daml/upgrades/SucceedsWhenTwoDeeplyNestedTypeSynonymsResolveToTheSameDatatypes/v1/Main.daml")
                )
              ]
              [ ("daml/Main.daml"
                , readFile =<< locateRunfiles (mainWorkspace </> "test-common/src/main/daml/upgrades/SucceedsWhenTwoDeeplyNestedTypeSynonymsResolveToTheSameDatatypes/v2/Main.daml")
                )
              ]
        , test
              "FailsWhenTwoDeeplyNestedTypeSynonymsResolveToDifferentDatatypes"
              (FailWithError "\ESC\\[0;91merror type checking template Main.A :\n  The upgraded template A has changed the types of some of its original fields.")
              LF.versionDefault
              NoDependencies
              [ ( "daml/Main.daml"
                , readFile =<< locateRunfiles (mainWorkspace </> "test-common/src/main/daml/upgrades/FailsWhenTwoDeeplyNestedTypeSynonymsResolveToDifferentDatatypes/v1/Main.daml")
                )
              ]
              [ ("daml/Main.daml"
                , readFile =<< locateRunfiles (mainWorkspace </> "test-common/src/main/daml/upgrades/FailsWhenTwoDeeplyNestedTypeSynonymsResolveToDifferentDatatypes/v2/Main.daml")
                )
              ]
        , test
              "SucceedsWhenAnInterfaceIsOnlyDefinedInTheInitialPackage"
              Succeed
              LF.versionDefault
              NoDependencies
              [ ( "daml/Main.daml"
                , readFile =<< locateRunfiles (mainWorkspace </> "test-common/src/main/daml/upgrades/SucceedsWhenAnInterfaceIsOnlyDefinedInTheInitialPackage/v1/Main.daml")
                )
              ]
              [ ("daml/Main.daml"
                , readFile =<< locateRunfiles (mainWorkspace </> "test-common/src/main/daml/upgrades/SucceedsWhenAnInterfaceIsOnlyDefinedInTheInitialPackage/v2/Main.daml")
                )
              ]
        , test
              "FailsWhenAnInterfaceIsDefinedInAnUpgradingPackageWhenItWasAlreadyInThePriorPackage"
              (FailWithError "\ESC\\[0;91merror type checking interface Main.I :\n  Tried to upgrade interface I, but interfaces cannot be upgraded. They should be removed in any upgrading package.")
              LF.versionDefault
              NoDependencies
              [ ( "daml/Main.daml"
                , readFile =<< locateRunfiles (mainWorkspace </> "test-common/src/main/daml/upgrades/FailsWhenAnInterfaceIsDefinedInAnUpgradingPackageWhenItWasAlreadyInThePriorPackage/v1/Main.daml")
                )
              ]
              [ ("daml/Main.daml"
                , readFile =<< locateRunfiles (mainWorkspace </> "test-common/src/main/daml/upgrades/FailsWhenAnInterfaceIsDefinedInAnUpgradingPackageWhenItWasAlreadyInThePriorPackage/v2/Main.daml")
                )
              ]
        , test
              "WarnsWhenAnInterfaceAndATemplateAreDefinedInTheSamePackage"
              (SucceedWithWarning "\ESC\\[0;93mwarning while type checking module Main:\n  This package defines both interfaces and templates.\n  \n  This is not recommended - templates are upgradeable, but interfaces are not, which means that this version of the package and its templates can never be uninstalled.\n  \n  It is recommended that interfaces are defined in their own package separate from their implementations.")
              LF.versionDefault
              NoDependencies
              [ ( "daml/Main.daml"
                , readFile =<< locateRunfiles (mainWorkspace </> "test-common/src/main/daml/upgrades/WarnsWhenAnInterfaceAndATemplateAreDefinedInTheSamePackage/v1/Main.daml")
                )
              ]
              [ ("daml/Main.daml"
                , readFile =<< locateRunfiles (mainWorkspace </> "test-common/src/main/daml/upgrades/WarnsWhenAnInterfaceAndATemplateAreDefinedInTheSamePackage/v2/Main.daml")
                )
              ]
        , test
              "WarnsWhenAnInterfaceIsUsedInThePackageThatItsDefinedIn"
              (SucceedWithWarning "\ESC\\[0;93mwarning while type checking interface Main.I :\n  The interface I was defined in this package and implemented in this package by the following templates:\n  \n  'T'\n  \n  However, it is recommended that interfaces are defined in their own package separate from their implementations.")
              LF.versionDefault
              NoDependencies
              [ ( "daml/Main.daml"
                , readFile =<< locateRunfiles (mainWorkspace </> "test-common/src/main/daml/upgrades/WarnsWhenAnInterfaceIsUsedInThePackageThatItsDefinedIn/v1/Main.daml")
                )
              ]
              [ ("daml/Main.daml"
                , readFile =<< locateRunfiles (mainWorkspace </> "test-common/src/main/daml/upgrades/WarnsWhenAnInterfaceIsUsedInThePackageThatItsDefinedIn/v2/Main.daml")
                )
              ]
        , test
              "WarnsWhenAnInterfaceIsDefinedAndThenUsedInAPackageThatUpgradesIt"
              (SucceedWithWarning "\ESC\\[0;93mwarning while type checking template Main.T interface instance [0-9a-f]+:Main:I for Main:T:\n  The template T has implemented interface I, which is defined in a previous version of this package.")
              LF.versionDefault
              DependOnV1
              [ ( "daml/Main.daml"
                , readFile =<< locateRunfiles (mainWorkspace </> "test-common/src/main/daml/upgrades/WarnsWhenAnInterfaceIsDefinedAndThenUsedInAPackageThatUpgradesIt/v1/Main.daml")
                )
              ]
              [ ("daml/Main.daml"
                , readFile =<< locateRunfiles (mainWorkspace </> "test-common/src/main/daml/upgrades/WarnsWhenAnInterfaceIsDefinedAndThenUsedInAPackageThatUpgradesIt/v2/Main.daml")
                )
              ]
        , test
              "FailsWhenAnInstanceIsDropped"
              (FailWithError "\ESC\\[0;91merror type checking template Main.T :\n  Implementation of interface I by template T appears in package that is being upgraded, but does not appear in this package.")
              LF.versionDefault
              (SeparateDep [
                ( "daml/Dep.daml"
                , readFile =<< locateRunfiles (mainWorkspace </> "test-common/src/main/daml/upgrades/FailsWhenAnInstanceIsDropped/dep/Dep.daml")
                )
              ])
              [ ( "daml/Main.daml"
                , readFile =<< locateRunfiles (mainWorkspace </> "test-common/src/main/daml/upgrades/FailsWhenAnInstanceIsDropped/v1/Main.daml")
                )
              ]
              [ ("daml/Main.daml"
                , readFile =<< locateRunfiles (mainWorkspace </> "test-common/src/main/daml/upgrades/FailsWhenAnInstanceIsDropped/v2/Main.daml")
                )
              ]
        , test
              "SucceedsWhenAnInstanceIsAddedSeparateDep"
              Succeed
              LF.versionDefault
              (SeparateDep [
                ( "daml/Dep.daml"
                , readFile =<< locateRunfiles (mainWorkspace </> "test-common/src/main/daml/upgrades/SucceedsWhenAnInstanceIsAddedSeparateDep/dep/Dep.daml")
                )
              ])
              [ ( "daml/Main.daml"
                , readFile =<< locateRunfiles (mainWorkspace </> "test-common/src/main/daml/upgrades/SucceedsWhenAnInstanceIsAddedSeparateDep/v1/Main.daml")
                )
              ]
              [ ("daml/Main.daml"
                , readFile =<< locateRunfiles (mainWorkspace </> "test-common/src/main/daml/upgrades/SucceedsWhenAnInstanceIsAddedSeparateDep/v2/Main.daml")
                )
              ]
        , test
              "SucceedsWhenAnInstanceIsAddedUpgradedPackage"
              Succeed
              LF.versionDefault
              DependOnV1
              [ ( "daml/Main.daml"
                , readFile =<< locateRunfiles (mainWorkspace </> "test-common/src/main/daml/upgrades/SucceedsWhenAnInstanceIsAddedUpgradedPackage/v1/Main.daml")
                )
              ]
              [ ( "daml/Main.daml"
                , readFile =<< locateRunfiles (mainWorkspace </> "test-common/src/main/daml/upgrades/SucceedsWhenAnInstanceIsAddedUpgradedPackage/v2/Main.daml")
                )
              ]
        , test
              "CannotUpgradeView"
              (FailWithError ".*Tried to implement a view of type (‘|\915\199\255)IView(’|\915\199\214) on interface (‘|\915\199\255)V1.I(’|\915\199\214), but the definition of interface (‘|\915\199\255)V1.I(’|\915\199\214) requires a view of type (‘|\915\199\255)V1.IView(’|\915\199\214)")
              LF.versionDefault
              DependOnV1
              [ ( "daml/Main.daml"
                , readFile =<< locateRunfiles (mainWorkspace </> "test-common/src/main/daml/upgrades/CannotUpgradeView/v1/Main.daml")
                )
              ]
              [ ( "daml/Main.daml"
                , readFile =<< locateRunfiles (mainWorkspace </> "test-common/src/main/daml/upgrades/CannotUpgradeView/v2/Main.daml")
                )
              ]
        , test
              "ValidUpgrade"
              Succeed
              contractKeysMinVersion
              NoDependencies
              [ ( "daml/Main.daml"
                , readFile =<< locateRunfiles (mainWorkspace </>
                    "test-common/src/main/daml/upgrades/ValidUpgrade/v1/Main.daml")
                )
              ]
              [ ( "daml/Main.daml"
                , readFile =<< locateRunfiles (mainWorkspace </>
                    "test-common/src/main/daml/upgrades/ValidUpgrade/v1/Main.daml")
                )
              ]
        , test
              "MissingModule"
              (FailWithError "\ESC\\[0;91merror type checking <none>:\n  Module Other appears in package that is being upgraded, but does not appear in this package.")
              LF.versionDefault
              NoDependencies
              [ ( "daml/Main.daml"
                , readFile =<< locateRunfiles (mainWorkspace </>
                    "test-common/src/main/daml/upgrades/MissingModule/v1/Main.daml")
                )
              , ( "daml/Other.daml"
                , readFile =<< locateRunfiles (mainWorkspace </>
                    "test-common/src/main/daml/upgrades/MissingModule/v1/Other.daml")
                )
              ]
              [ ( "daml/Main.daml"
                , readFile =<< locateRunfiles (mainWorkspace </>
                    "test-common/src/main/daml/upgrades/MissingModule/v2/Main.daml")
                )
              ]
        , test
              "MissingTemplate"
              (FailWithError "\ESC\\[0;91merror type checking <none>:\n  Template U appears in package that is being upgraded, but does not appear in this package.")
              LF.versionDefault
              NoDependencies
              [ ( "daml/Main.daml"
                , readFile =<< locateRunfiles (mainWorkspace </>
                    "test-common/src/main/daml/upgrades/MissingTemplate/v1/Main.daml")
                )
              ]
              [ ( "daml/Main.daml"
                , readFile =<< locateRunfiles (mainWorkspace </>
                    "test-common/src/main/daml/upgrades/MissingTemplate/v2/Main.daml")
                )
              ]
        , test
              "MissingDataCon"
              (FailWithError "\ESC\\[0;91merror type checking <none>:\n  Data type U appears in package that is being upgraded, but does not appear in this package.")
              LF.versionDefault
              NoDependencies
              [ ( "daml/Main.daml"
                , readFile =<< locateRunfiles (mainWorkspace </>
                    "test-common/src/main/daml/upgrades/MissingDataCon/v1/Main.daml")
                )
              ]
              [ ( "daml/Main.daml"
                , readFile =<< locateRunfiles (mainWorkspace </>
                    "test-common/src/main/daml/upgrades/MissingDataCon/v2/Main.daml")
                )
              ]
        , test
              "MissingChoice"
              (FailWithError "\ESC\\[0;91merror type checking template Main.T :\n  Choice C2 appears in package that is being upgraded, but does not appear in this package.")
              LF.versionDefault
              NoDependencies
              [ ( "daml/Main.daml"
                , readFile =<< locateRunfiles (mainWorkspace </>
                    "test-common/src/main/daml/upgrades/MissingChoice/v1/Main.daml")
                )
              ]
              [ ( "daml/Main.daml"
                , readFile =<< locateRunfiles (mainWorkspace </>
                    "test-common/src/main/daml/upgrades/MissingChoice/v2/Main.daml")
                )
              ]
        , test
              "TemplateChangedKeyType"
              (FailWithError "\ESC\\[0;91merror type checking template Main.T key:\n  The upgraded template T cannot change its key type.")
              contractKeysMinVersion
              NoDependencies
              [ ( "daml/Main.daml"
                , readFile =<< locateRunfiles (mainWorkspace </>
                    "test-common/src/main/daml/upgrades/TemplateChangedKeyType/v1/Main.daml")
                )
              ]
              [ ( "daml/Main.daml"
                , readFile =<< locateRunfiles (mainWorkspace </>
                    "test-common/src/main/daml/upgrades/TemplateChangedKeyType/v2/Main.daml")
                )
              ]
        , test
              "RecordFieldsNewNonOptional"
              (FailWithError "\ESC\\[0;91merror type checking data type Main.Struct:\n  The upgraded data type Struct has added new fields, but those fields are not Optional.")
              LF.versionDefault
              NoDependencies
              [ ( "daml/Main.daml"
                , readFile =<< locateRunfiles (mainWorkspace </>
                    "test-common/src/main/daml/upgrades/RecordFieldsNewNonOptional/v1/Main.daml")
                )
              ]
              [ ( "daml/Main.daml"
                , readFile =<< locateRunfiles (mainWorkspace </>
                    "test-common/src/main/daml/upgrades/RecordFieldsNewNonOptional/v2/Main.daml")
                )
              ]
        ]
  where
    contractKeysMinVersion :: LF.Version
    contractKeysMinVersion = 
        fromJustNote
            "Expected at least one LF 2.x version to support contract keys." 
            (LF.featureMinVersion LF.featureContractKeys LF.V2)

    test ::
           String
        -> Expectation
        -> LF.Version
        -> Dependency
        -> [(FilePath, IO String)]
        -> [(FilePath, IO String)]
        -> TestTree
    test name expectation lfVersion sharedDep oldVersion newVersion =
        testCase name $
        withTempDir $ \dir -> do
            let newDir = dir </> "newVersion"
            let oldDir = dir </> "oldVersion"
            let newDar = newDir </> "out.dar"
            let oldDar = oldDir </> "old.dar"

            (depV1Dar, depV2Dar) <- case sharedDep of
              SeparateDep sharedDep -> do
                let sharedDir = dir </> "shared"
                let sharedDar = sharedDir </> "out.dar"
                writeFiles sharedDir (projectFile lfVersion ("upgrades-example-" <> name <> "-dep") Nothing Nothing : sharedDep)
                callProcessSilent damlc ["build", "--project-root", sharedDir, "-o", sharedDar]
                pure (Just sharedDar, Just sharedDar)
              DependOnV1 ->
                pure (Nothing, Just oldDar)
              _ ->
                pure (Nothing, Nothing)

            writeFiles oldDir (projectFile lfVersion ("upgrades-example-" <> name) Nothing depV1Dar : oldVersion)
            callProcessSilent damlc ["build", "--project-root", oldDir, "-o", oldDar]

            writeFiles newDir (projectFile lfVersion ("upgrades-example-" <> name <> "-v2") (Just oldDar) depV2Dar : newVersion)
            case expectation of
              Succeed ->
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
                  unless (matchTest compiledRegex stderr) $
                      assertFailure ("`daml build` succeeded, but did not give a warning matching '" <> show regexWithSeverity <> "':\n" <> show stderr)

    writeFiles dir fs =
        for_ fs $ \(file, ioContent) -> do
            content <- ioContent
            createDirectoryIfMissing True (takeDirectory $ dir </> file)
            writeFileUTF8 (dir </> file) content

    projectFile lfVersion name upgradedFile mbDep =
        ( "daml.yaml"
        , unlines $
          [ "sdk-version: " <> sdkVersion
          , "name: " <> name
          , "source: daml"
          , "version: 0.0.1"
          , "dependencies:"
          , "  - daml-prim"
          , "  - daml-stdlib"
          , "typecheck-upgrades: true"
          , "build-options:"
          , "  - --target=" <> LF.renderVersion lfVersion
          , "  - --enable-interfaces=yes"
          ] ++ ["upgrades: '" <> path <> "'" | Just path <- pure upgradedFile]
            ++ ["data-dependencies:\n -  '" <> path <> "'" | Just path <- pure mbDep]
        )

data Expectation
  = Succeed
  | FailWithError T.Text
  | SucceedWithWarning T.Text
  deriving (Show, Eq, Ord)

data Dependency
  = NoDependencies
  | DependOnV1
  | SeparateDep [(FilePath, IO String)]
