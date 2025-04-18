-- Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

module DA.Daml.Package.ConfigTest (main) where

import qualified DA.Daml.LF.Ast as LF
import DA.Daml.Package.Config
import DA.Test.Util
import qualified Data.Map.Strict as Map
import qualified Data.Text as T
import Test.Tasty
import Test.Tasty.HUnit
import Control.Exception (throw)
import DA.Daml.Project.Types (parseUnresolvedVersion)

main :: IO ()
main = defaultMain $
  testGroup "package-config"
    [ checkPkgConfigTests
    ]

checkPkgConfigTests :: TestTree
checkPkgConfigTests = testGroup "checkPkgConfig"
  [ testCase "accepts stable version" $ do
      checkPkgConfig (config (LF.PackageName "foobar") (LF.PackageVersion "1.1.1")) @?= []
  , testCase "accepts GHC snapshot version" $ do
      checkPkgConfig (config (LF.PackageName "foobar") (LF.PackageVersion "2.0.0.20211130.8536.0")) @?= []
  , testCase "rejects semver snapshot version" $ do
      [err] <- pure $ checkPkgConfig (config (LF.PackageName "foobar") (LF.PackageVersion "2.0.0-snapshot.20211130.8536.0.683ab871"))
      assertInfixOf "Invalid package version" (T.unpack err)
  ]
  where
    config name version = PackageConfigFields
      { pName = name
      , pSrc = "src"
      , pExposedModules = Nothing
      , pVersion = Just version
      , pDependencies = []
      , pDataDependencies = []
      , pModulePrefixes = Map.empty
      , pSdkVersion = either throw id (parseUnresolvedVersion "0.0.0")
      , pUpgradeDar = Nothing
      }

