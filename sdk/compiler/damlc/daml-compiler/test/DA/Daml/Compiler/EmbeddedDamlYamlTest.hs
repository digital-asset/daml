-- Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

module DA.Daml.Compiler.EmbeddedDamlYamlTest (main) where

import DA.Daml.Compiler.EmbeddedDamlYaml
import qualified DA.Daml.LF.Ast as LF
import qualified Data.ByteString.Lazy.Char8 as BSLC
import qualified Data.Map.Strict as Map
import Data.List (isInfixOf, isPrefixOf, tails)
import qualified Module as Ghc
import Test.Tasty
import Test.Tasty.HUnit

main :: IO ()
main = defaultMain $ testGroup "EmbeddedDamlYaml"
    [ relativeSourceTests
    , renderTests
    , dataDependencyTests
    , packageExposureFlagTests
    ]

relativeSourceTests :: TestTree
relativeSourceTests = testGroup "relativeSource"
    [ testCase "a source already relative to the package root is kept" $
        relativeSource "/pkg" "daml" @?= Just "daml"
    , testCase "an absolute source inside the package root is made relative" $
        relativeSource "/pkg" "/pkg/daml" @?= Just "daml"
    , testCase "a trailing slash on the package root makes no difference" $
        relativeSource "/pkg/" "/pkg/daml" @?= Just "daml"
    , testCase "the package root itself is the current directory" $
        relativeSource "/pkg" "/pkg" @?= Just "."
    , testCase "a source naming a file rather than a directory is kept" $
        relativeSource "/pkg" "/pkg/daml/Main.daml" @?= Just "daml/Main.daml"
    , testCase "a source above the package root is refused" $
        relativeSource "/pkg" "../shared" @?= Nothing
    , testCase "an absolute source outside the package root is refused" $
        relativeSource "/pkg" "/elsewhere/daml" @?= Nothing
    ]

renderTests :: TestTree
renderTests = testGroup "renderEmbeddedDamlYaml"
    [ testCase "records the compiler, name, version and source" $ do
        let rendered = render (sample { edyVersion = Just (LF.PackageVersion "1.0.0") })
        assertInfixOf "- damlc:3.5.2" rendered
        assertInfixOf "name: my-package" rendered
        assertInfixOf "version: 1.0.0" rendered
        assertInfixOf "source: daml" rendered
    , testCase "turns off interpolation, the values here are already resolved" $
        assertInfixOf "environment-variable-interpolation: false" (render sample)
    , testCase "omits every optional field when unset" $ do
        let rendered = render sample
        assertBool "no version key" (not ("version:" `isInfixOf` rendered))
        assertBool "no upgrades key" (not ("upgrades:" `isInfixOf` rendered))
        assertBool "no data-dependencies key" (not ("data-dependencies:" `isInfixOf` rendered))
        assertBool "no module-prefixes key" (not ("module-prefixes:" `isInfixOf` rendered))
        assertBool "no build-options key" (not ("build-options:" `isInfixOf` rendered))
    , testCase "points at the DALFs the DAR carries" $
        assertInfixOf "- my-package-1.0.0-abc/dep-1.0.0-def.dalf"
            (render sample { edyDataDependencies = [Ghc.stringToUnitId "dep-1.0.0"] })
    , testCase "always writes a dependencies list" $
        assertInfixOf "dependencies: []" (render sample)

    , testCase "records module prefixes" $
        assertInfixOf "foo-1.0.0: V1"
            (render sample { edyModulePrefixes =
                Map.fromList [(Ghc.stringToUnitId "foo-1.0.0", Ghc.mkModuleName "V1")] })
    , testCase "records the package an upgrade upgrades" $
        assertInfixOf "upgrades: 3a7f9c"
            (render sample { edyUpgrades = Just (LF.PackageId "3a7f9c") })
    , testCase "copies build-options verbatim" $
        assertInfixOf "- --target=2.3"
            (render sample { edyBuildOptions = ["--target=2.3"] })
    , testCase "quotes a build-option that would otherwise not round-trip" $
        assertInfixOf "'--ghc-option=-Wall: yes'"
            (render sample { edyBuildOptions = ["--ghc-option=-Wall: yes"] })
    , testCase "writes the fields in a readable order" $ do
        let rendered = render (sample { edyVersion = Just (LF.PackageVersion "1.0.0") })
        assertBool "components before name" (indexOf "components:" rendered < indexOf "name:" rendered)
        assertBool "name before source" (indexOf "name:" rendered < indexOf "source:" rendered)
    ]
  where
    sample = EmbeddedDamlYaml
        { edyName = LF.PackageName "my-package"
        , edyVersion = Nothing
        , edyDamlcVersion = "3.5.2"
        , edySource = "daml"
        , edyUpgrades = Nothing
        , edyDataDependencies = []
        , edyModulePrefixes = Map.empty
        , edyBuildOptions = []
        }
    render = BSLC.unpack . renderEmbeddedDamlYaml dalfPaths
    dalfPaths = Map.fromList
        [ (Ghc.stringToUnitId "dep-1.0.0", "my-package-1.0.0-abc/dep-1.0.0-def.dalf") ]
    assertInfixOf needle haystack =
        assertBool (show needle <> " not found in:\n" <> haystack) (needle `isInfixOf` haystack)
    indexOf needle haystack =
        length (takeWhile (not . (needle `isPrefixOf`)) (tails haystack))

dataDependencyTests :: TestTree
dataDependencyTests = testGroup "dataDependencies"
    [ testCase "a dependency the DAR carries is pointed at" $
        pointedAt ["dep-1.0.0"] @?= ["dep-1.0.0"]
    , testCase "the base packages are left to the compiler" $
        pointedAt ["daml-prim", "daml-stdlib-0.0.0", "dep-1.0.0"] @?= ["dep-1.0.0"]
    , testCase "a package a component supplied is pointed at too" $
        pointedAt ["daml-script-3.5.2", "dep-1.0.0"]
            @?= ["daml-script-3.5.2", "dep-1.0.0"]
    ]
  where
    pointedAt = map Ghc.unitIdString . dataDependencies . map Ghc.stringToUnitId

packageExposureFlagTests :: TestTree
packageExposureFlagTests = testGroup "packageExposureFlags"
    [ testCase "a direct dependency the DAR carries is exposed" $
        flagsFor ["dep-1.0.0"] ["dep-1.0.0"] @?= ["--package=dep-1.0.0"]
    , testCase "a dependency the package does not depend on directly is left hidden" $
        flagsFor ["dep-1.0.0"] ["dep-1.0.0", "transitive-1.0.0"]
            @?= ["--package=dep-1.0.0"]
    , testCase "flags follow the order the direct dependencies were given in" $
        flagsFor ["b-1.0.0", "a-1.0.0"] ["a-1.0.0", "b-1.0.0"]
            @?= ["--package=b-1.0.0", "--package=a-1.0.0"]
    ]
  where
    flagsFor directDeps carried =
        packageExposureFlags
            (map Ghc.stringToUnitId directDeps)
            (dataDependencies (map Ghc.stringToUnitId carried))
