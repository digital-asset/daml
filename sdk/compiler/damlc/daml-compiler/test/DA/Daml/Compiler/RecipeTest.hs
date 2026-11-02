-- Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

module DA.Daml.Compiler.RecipeTest (main) where

import qualified DA.Daml.LF.Ast as LF
import DA.Daml.Compiler.Recipe
import System.Directory (createDirectoryIfMissing)
import System.FilePath ((</>))
import System.IO.Extra (withTempDir)
import Test.Tasty
import Test.Tasty.HUnit

main :: IO ()
main = defaultMain $
  testGroup "DA.Daml.Compiler.Recipe"
    [ sdkPackageNameTests
    , resolveDependencyFieldTests
    ]

sdkPackageNameTests :: TestTree
sdkPackageNameTests = testGroup "sdkPackageName"
  [ testCase "recognises daml-prim" $
      sdkPackageName "daml-prim-0.0.0" @?= Just "daml-prim"
  , testCase "recognises daml-stdlib" $
      sdkPackageName "daml-stdlib-2.9.0" @?= Just "daml-stdlib"
  , testCase "recognises daml-script" $
      sdkPackageName "daml-script-2.9.0" @?= Just "daml-script"
  , testCase "recognises daml3-script" $
      sdkPackageName "daml3-script-2.9.0" @?= Just "daml3-script"
  , testCase "recognises daml-trigger" $
      sdkPackageName "daml-trigger-2.9.0" @?= Just "daml-trigger"
  , testCase "does not match a user package merely containing the substring" $
      -- "my-daml-prim-helper" is not an SDK unit, even though it contains
      -- "daml-prim"; sdkPackageName must anchor on a prefix, not search.
      sdkPackageName "my-daml-prim-helper-1.0.0" @?= Nothing
  , testCase "rejects an ordinary user package" $
      sdkPackageName "my-package-1.0.0" @?= Nothing
  ]

resolveDependencyFieldTests :: TestTree
resolveDependencyFieldTests = testGroup "resolveDependencyField"
  [ testCase "reports a package with a _pkg_ marker as a dependency" $ withTempDir $ \depsDir -> do
      let pkgId = LF.PackageId "aaaa"
      createDirectoryIfMissing True (depsDir </> "aaaa")
      writeFile (depsDir </> "aaaa" </> "_pkg_") ""
      field <- resolveDependencyField depsDir pkgId
      case field of
        FieldDependencies -> pure ()
        FieldDataDependencies -> assertFailure "expected FieldDependencies"
  , testCase "reports a package with no marker as a data-dependency" $ withTempDir $ \depsDir -> do
      let pkgId = LF.PackageId "bbbb"
      createDirectoryIfMissing True (depsDir </> "bbbb")
      field <- resolveDependencyField depsDir pkgId
      case field of
        FieldDataDependencies -> pure ()
        FieldDependencies -> assertFailure "expected FieldDataDependencies"
  , testCase "reports a package with both _pkg_ and _data_ as a data-dependency" $ withTempDir $ \depsDir -> do
      -- DA.Cli.Damlc.DependencyDb can mark the same package-id directory with
      -- both _pkg_ and _data_ (e.g. installed once as a dependency's
      -- transitive dalf, then again as an explicitly-named
      -- data-dependency). DA.Cli.Damlc.Packaging's own package-db
      -- construction breaks that tie by excluding anything data-dependency-
      -- marked from the dependency set (dalfsFromDependencyFps \\
      -- dalfsFromDataDependencyFps); resolveDependencyField must agree with
      -- that, not just check _pkg_ in isolation.
      let pkgId = LF.PackageId "cccc"
      createDirectoryIfMissing True (depsDir </> "cccc")
      writeFile (depsDir </> "cccc" </> "_pkg_") ""
      writeFile (depsDir </> "cccc" </> "_data_") ""
      field <- resolveDependencyField depsDir pkgId
      case field of
        FieldDataDependencies -> pure ()
        FieldDependencies -> assertFailure "expected FieldDataDependencies when both markers are present"
  ]
