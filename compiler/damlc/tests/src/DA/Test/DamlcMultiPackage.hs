-- Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

module DA.Test.DamlcMultiPackage (main) where

{- HLINT ignore "locateRunfiles/package_app" -}

import Control.Exception (try)
import Control.Monad.Extra (forM, forM_, unless, void)
import DA.Bazel.Runfiles (exe, locateRunfiles, mainWorkspace)
import qualified DA.Daml.Dar.Reader as Reader
import qualified DA.Daml.LF.Ast as LF
import DA.Daml.LF.Ast.Version (version1_15)
import DA.Test.Util (defaultJvmMemoryLimits, limitJvmMemory, withEnv)
import qualified Data.HashMap.Strict as HashMap
import Data.List (find, intercalate, intersect, isInfixOf, isPrefixOf, sort, union, (\\))
import qualified Data.Map as Map
import Data.Maybe (fromMaybe, fromJust)
import qualified Data.NameMap as NM
import qualified Data.Text as T
import qualified Data.Text.IO as TIO
import Data.Time.Clock (UTCTime)
import SdkVersion (SdkVersioned, sdkVersion, withSdkVersions)
import System.Directory.Extra (canonicalizePath, createDirectoryIfMissing, doesFileExist, getModificationTime, listDirectory, removeFile, withCurrentDirectory)
import System.Environment.Blank (setEnv)
import System.Exit (ExitCode (..))
import System.FilePath (getSearchPath, isExtensionOf, makeRelative, replaceFileName, searchPathSeparator, takeFileName, (</>))
import System.Info.Extra (isWindows)
import System.IO.Extra (newTempDir, withTempDir)
import System.Process (CreateProcess (..), proc, readCreateProcessWithExitCode, readCreateProcess, readProcess)
import Test.Tasty (TestTree, defaultMain, testGroup, withResource)
import Test.Tasty.HUnit (HUnitFailure (..), assertFailure, assertBool, assertEqual, testCase, (@?=))
import Text.Regex.TDFA (Regex, makeRegex, matchTest)

-- Abstraction over the folder structure of a project, consisting of many packages.
data ProjectStructure
  = DamlYaml
      { dyName :: T.Text
      , dyVersion :: T.Text
      , dySdkVersion :: Maybe T.Text
      , dySource :: T.Text
      , dyOutPath :: Maybe T.Text
      , dyModulePrefixes :: [(PackageIdentifier, T.Text)]
      , dyGhcOptions :: [T.Text]
      , dyDeps :: [T.Text]
      }
  | MultiPackage
      { mpPackages :: [T.Text]
      , mpProjects :: [T.Text]
      , mpCompositeDars :: [CompositeDarDefinition]
      }
  | Dir
      { dName :: T.Text 
      , dContents :: [ProjectStructure]
      }
  | DamlSource -- Simple daml source file with correct name (derived from module name) and given module dependencies (as instance imports)
      { dsModuleName :: T.Text
      , dsDeps :: [T.Text]
      }
  | GenericFile -- Raw file, with full contents given
      { gfName :: T.Text
      , gfContent :: T.Text
      }

data PackageIdentifier = PackageIdentifier
  { piName :: T.Text
  , piVersion :: T.Text
  }
  deriving (Eq, Ord)
instance Show PackageIdentifier where
  show pi = T.unpack (piName pi) <> "-" <> T.unpack (piVersion pi)

data CompositeDarDefinition = CompositeDarDefinition
  { cddName :: T.Text
  , cddVersion :: T.Text
  , cddPackages :: [T.Text]
  , cddDars :: [T.Text]
  , cddPath :: T.Text
  }

{- Remaining tests needed:
- multi-sdk
    Use Dylan's `releases-endpoint` and `alternate-download` in daml-config to defer sdk download
    Create a mock server/api that serves this file to the downloader
    Run a test that attempts to use 2.7.5, then either ensure this endpoint is hit, or somehow check the sdk version of the generated dar.
-}

main :: IO ()
main = withSdkVersions $ do
  damlAssistant <- locateRunfiles (mainWorkspace </> "daml-assistant" </> exe "daml")
  release <- locateRunfiles (mainWorkspace </> "release" </> "sdk-release-tarball-ce.tar.gz")
  oldPath <- getSearchPath
  javaPath <- locateRunfiles "local_jdk/bin"

  withTempDir $ \damlHome -> do
    setEnv "DAML_HOME" damlHome True
    -- Install sdk `env:DAML_SDK_RELEASE_VERSION` into temp DAML_HOME
    -- corresponds to:
    --   - `0.0.0` on PR builds
    --   - `x.y.z-snapshot.yyyymmdd.nnnnn.m.vpppppppp` on MAIN/Release builds
    void $ readProcess damlAssistant ["install", release, "--install-with-custom-version", sdkVersion] ""
    -- Install a copy under the release version 10.0.0
    void $ readProcess damlAssistant ["install", release, "--install-with-custom-version", "10.0.0"] ""

    limitJvmMemory defaultJvmMemoryLimits
    withEnv
        [("PATH", Just $ intercalate [searchPathSeparator] $ javaPath : oldPath)] 
        (defaultMain $ tests damlAssistant)


tests :: SdkVersioned => FilePath -> TestTree
tests damlAssistant =
  testGroup
    "Multi-Package build"
    [ testGroup
        "Simple two package project"
        [ test "Build A with search" [] "./package-a" simpleTwoPackageProject
            $ Right [PackageIdentifier "package-a" "0.0.1"]
        , test "Build B with search" [] "./package-b" simpleTwoPackageProject
            $ Right [PackageIdentifier "package-a" "0.0.1", PackageIdentifier "package-b" "0.0.1"]
        , test "Build all from A with search" ["--all"] "./package-a" simpleTwoPackageProject
            $ Right [PackageIdentifier "package-a" "0.0.1", PackageIdentifier "package-b" "0.0.1"]
        , test "Build all from B with search" ["--all"] "./package-b" simpleTwoPackageProject
            $ Right [PackageIdentifier "package-a" "0.0.1", PackageIdentifier "package-b" "0.0.1"]
        , test "Build all from root" ["--all"] "" simpleTwoPackageProject
            $ Right [PackageIdentifier "package-a" "0.0.1", PackageIdentifier "package-b" "0.0.1"]
        , test "Build all from A with explicit path" ["--all", "--multi-package-path=.."] "./package-a" simpleTwoPackageProject
            $ Right [PackageIdentifier "package-a" "0.0.1", PackageIdentifier "package-b" "0.0.1"]
        , test "Build B from nested directory with search" [] "./package-b/daml" simpleTwoPackageProject
            $ Right [PackageIdentifier "package-a" "0.0.1", PackageIdentifier "package-b" "0.0.1"]
        ]
    , testGroup
        "Diamond project"
        [ test "Build D with search" [] "./package-d" diamondProject $ Right
            [ PackageIdentifier "package-a" "0.0.1"
            , PackageIdentifier "package-b" "0.0.1"
            , PackageIdentifier "package-c" "0.0.1"
            , PackageIdentifier "package-d" "0.0.1"
            ]
        , test "Build C with search" [] "./package-c" diamondProject $ Right
            [ PackageIdentifier "package-a" "0.0.1"
            , PackageIdentifier "package-c" "0.0.1"
            ]
        , test "Build B with search" [] "./package-b" diamondProject $ Right
            [ PackageIdentifier "package-a" "0.0.1"
            , PackageIdentifier "package-b" "0.0.1"
            ]
        , test "Build A with search" [] "./package-a" diamondProject $ Right
            [ PackageIdentifier "package-a" "0.0.1" ]
        , test "Build all from root" ["--all"] "" diamondProject $ Right
            [ PackageIdentifier "package-a" "0.0.1"
            , PackageIdentifier "package-b" "0.0.1"
            , PackageIdentifier "package-c" "0.0.1"
            , PackageIdentifier "package-d" "0.0.1"
            ]
        , test "Build all from A" ["--all"] "./package-a" diamondProject $ Right
            [ PackageIdentifier "package-a" "0.0.1"
            , PackageIdentifier "package-b" "0.0.1"
            , PackageIdentifier "package-c" "0.0.1"
            , PackageIdentifier "package-d" "0.0.1"
            ]
        ]
    , testGroup
        "Multi project"
        [ test "Build package B" [] "./packages/package-b" multiProject $ Right
            [ PackageIdentifier "lib-a" "0.0.1"
            , PackageIdentifier "lib-b" "0.0.1"
            , PackageIdentifier "package-a" "0.0.1"
            , PackageIdentifier "package-b" "0.0.1"
            ]
        , test "Build package A" [] "./packages/package-a" multiProject $ Right
            [ PackageIdentifier "lib-a" "0.0.1"
            , PackageIdentifier "lib-b" "0.0.1"
            , PackageIdentifier "package-a" "0.0.1"
            ]
        , test "Build lib B" [] "./libs/lib-b" multiProject $ Right
            [ PackageIdentifier "lib-a" "0.0.1"
            , PackageIdentifier "lib-b" "0.0.1"
            ]
        , test "Build lib A" [] "./libs/lib-a" multiProject $ Right
            [ PackageIdentifier "lib-a" "0.0.1" ]
        , test "Build all from packages" ["--all"] "./packages" multiProject $ Right
            [ PackageIdentifier "lib-a" "0.0.1"
            , PackageIdentifier "lib-b" "0.0.1"
            , PackageIdentifier "package-a" "0.0.1"
            , PackageIdentifier "package-b" "0.0.1"
            ]
        , test "Build all from libs" ["--all"] "./libs" multiProject $ Right
            [ PackageIdentifier "lib-a" "0.0.1"
            , PackageIdentifier "lib-b" "0.0.1"
            ]
        ]
    , testGroup
        "Cycle handling"
        [ test "Permitted multi-package project cycle from lib-a" [] "./libs/lib-a" cyclicMultiPackage $ Right
            [ PackageIdentifier "lib-a" "0.0.1"
            ]
        , test "Permitted multi-package project cycle from package-a" [] "./packages/package-a" cyclicMultiPackage $ Right
            [ PackageIdentifier "lib-a" "0.0.1"
            , PackageIdentifier "package-a" "0.0.1"
            ]
        , test "Permitted multi-package project cycle from libs --all" ["--all"] "./libs" cyclicMultiPackage $ Right
            [ PackageIdentifier "lib-a" "0.0.1"
            , PackageIdentifier "package-a" "0.0.1"
            ]
        , test "Permitted multi-package project cycle from packages --all" ["--all"] "./packages" cyclicMultiPackage $ Right
            [ PackageIdentifier "lib-a" "0.0.1"
            , PackageIdentifier "package-a" "0.0.1"
            ]
        , test "Illegal package dep cycle from package-a" [] "./package-a" cyclicPackagesProject $ Left "recursion detected"
        , test "Illegal package dep cycle from package-b" [] "./package-b" cyclicPackagesProject $ Left "recursion detected"
        , test "Illegal package dep cycle from root --all" ["--all"] "" cyclicPackagesProject $ Left "recursion detected"
        ]
    , testGroup
        "Special flag behaviour"
        [ test "Multi-package build rebuilds dar with --output" [] "./package-b" customOutPathProject
            $ Right [PackageIdentifier "package-a" "0.0.1", PackageIdentifier "package-b" "0.0.1"]
        , test "Build --all doesn't forward options flags like --ghc-options" ["--all", "--ghc-option=-Werror"] "" warningProject
            $ Right [PackageIdentifier "package-a" "0.0.1", PackageIdentifier "package-b" "0.0.1"]
        , test "Build package a forwards options flags like --ghc-options only to package a" ["--ghc-option=-Werror"] "./package-a" warningProject
            $ Left "Pattern match\\(es\\) are non-exhaustive"
        , test "Build package b forwards options flags like --ghc-options only to package b" ["--ghc-option=-Werror"] "./package-b" warningProject
            $ Left "Created .+(\\/|\\\\)package-a-0\\.0\\.1\\.dar(.|\n)+Pattern match\\(es\\) are non-exhaustive"
            -- ^ Special regex ensures that package-a built fine (so didn't take the flag)
        ]
    , testGroup
        "Package name/version collision tests"
        [ test "Build --all with same package names, different version" ["--all"] "" sameNameDifferentVersionProject
            $ Right [PackageIdentifier "package" "0.0.1", PackageIdentifier "package" "0.0.2"]
        , test "Build --all with same package names and version" ["--all"] "" sameNameSameVersionProject
            $ Left "Package package-0\\.0\\.1 imports a package with the same name\\."
        ]
    , testGroup
        "Caching"
        [ testCache
            "All dars are cached"
            (["--all"], "")
            [PackageIdentifier "package-a" "0.0.1", PackageIdentifier "package-b" "0.0.1"] -- First time builds both
            (const $ pure ()) -- No modifications
            (["--all"], "")
            [] -- So second time rebuilds nothing
            simpleTwoPackageProject
        , testCache
            "All dars are rebuilt with caching disabled"
            (["--all"], "")
            [PackageIdentifier "package-a" "0.0.1", PackageIdentifier "package-b" "0.0.1"] -- First time builds both
            (const $ pure ()) -- No modifications
            (["--all", "--no-cache"], "") -- Cache disabled
            [PackageIdentifier "package-a" "0.0.1", PackageIdentifier "package-b" "0.0.1"] -- So second time rebuilds everything
            simpleTwoPackageProject
        , testCache
            "Just B rebuilds if its code is modified"
            (["--all"], "")
            [PackageIdentifier "package-a" "0.0.1", PackageIdentifier "package-b" "0.0.1"] -- First time builds both
            (const $ appendFile "./package-b/daml/PackageBMain.daml" "\nmyDef = 3") -- Modify package-b/daml/PackageBMain.daml
            (["--all"], "")
            [PackageIdentifier "package-b" "0.0.1"] -- So second time rebuilds only B, as nothing depends on B
            simpleTwoPackageProject
        , testCache
            "A and B rebuild if A's code is modified"
            (["--all"], "")
            [PackageIdentifier "package-a" "0.0.1", PackageIdentifier "package-b" "0.0.1"] -- First time builds both
            (const $ appendFile "./package-a/daml/PackageAMain.daml" "\nmyDef = 3") -- Modify package-a/daml/PackageAMain.daml
            (["--all"], "")
            [PackageIdentifier "package-a" "0.0.1", PackageIdentifier "package-b" "0.0.1"] -- So second time rebuilds A and B, as B depends on A
            simpleTwoPackageProject
        , testCache
            "Only A is rebuild if its Dar is deleted but it's package-id doesn't change"
            (["--all"], "")
            [PackageIdentifier "package-a" "0.0.1", PackageIdentifier "package-b" "0.0.1"] -- First time builds both
            (const $ removeFile "package-a/.daml/dist/package-a-0.0.1.dar") -- Delete package-a/.daml/dist/package-a-0.0.1.dar
            (["--all"], "")
            [PackageIdentifier "package-a" "0.0.1"] -- So second time rebuilds only A, as its package-id hasn't changed so B is not stale
            simpleTwoPackageProject
        , testCache
            "A and B rebuild if A's code is modified and A's dar is deleted"
            (["--all"], "")
            [PackageIdentifier "package-a" "0.0.1", PackageIdentifier "package-b" "0.0.1"] -- First time builds both
            -- Modify package-a/daml/PackageAMain.daml, Delete package-a/.daml/dist/package-a-0.0.1.dar
            (const $ do
              appendFile "./package-a/daml/PackageAMain.daml" "\nmyDef = 3"
              removeFile "package-a/.daml/dist/package-a-0.0.1.dar"
            )
            (["--all"], "")
            [PackageIdentifier "package-a" "0.0.1", PackageIdentifier "package-b" "0.0.1"] -- So second time rebuilds A and B, as B depends on A
            simpleTwoPackageProject
        , testCache
            "B rebuilds if A is manually rebuilt after change"
            (["--all"], "")
            [PackageIdentifier "package-a" "0.0.1", PackageIdentifier "package-b" "0.0.1"] -- First time builds both
            -- Modify package-a/daml/PackageAMain.daml, manually rebuild package-a/.daml/dist/package-a-0.0.1.dar
            (\manualBuild -> do
              appendFile "./package-a/daml/PackageAMain.daml" "\nmyDef = 3"
              manualBuild "./package-a"
            )
            (["--all"], "")
            [PackageIdentifier "package-a" "0.0.1", PackageIdentifier "package-b" "0.0.1"] -- Both have been rebuilt (A manually)
            simpleTwoPackageProject
        , testCache
            "B rebuilds if A is manually rebuilt after change and dar deleted"
            (["--all"], "")
            [PackageIdentifier "package-a" "0.0.1", PackageIdentifier "package-b" "0.0.1"] -- First time builds both
            -- Modify package-a/daml/PackageAMain.daml, Delete package-a/.daml/dist/package-a-0.0.1.dar
            (\manualBuild -> do
              appendFile "./package-a/daml/PackageAMain.daml" "\nmyDef = 3"
              removeFile "package-a/.daml/dist/package-a-0.0.1.dar"
              manualBuild "./package-a"
            )
            (["--all"], "")
            [PackageIdentifier "package-a" "0.0.1", PackageIdentifier "package-b" "0.0.1"] -- Both have been rebuilt (A manually)
            simpleTwoPackageProject
        , testCache
            "Top package is always built (A)"
            (["--all"], "")
            [PackageIdentifier "package-a" "0.0.1", PackageIdentifier "package-b" "0.0.1"] -- First time builds both
            (const $ pure ())
            ([], "./package-a")
            [PackageIdentifier "package-a" "0.0.1"]
            simpleTwoPackageProject
        , testCache
            "Top package is always built (B)"
            (["--all"], "")
            [PackageIdentifier "package-a" "0.0.1", PackageIdentifier "package-b" "0.0.1"] -- First time builds both
            (const $ pure ())
            ([], "./package-b")
            [PackageIdentifier "package-b" "0.0.1"]
            simpleTwoPackageProject
        , testCache
            "Only above in the dependency tree is invalidated"
            (["--all"], "")
            [PackageIdentifier "package-a" "0.0.1", PackageIdentifier "package-b" "0.0.1", PackageIdentifier "package-c" "0.0.1", PackageIdentifier "package-d" "0.0.1"]
            (const $ appendFile "./package-b/daml/PackageBMain.daml" "\nmyDef = 3")
            (["--all"], "")
            [PackageIdentifier "package-b" "0.0.1", PackageIdentifier "package-d" "0.0.1"] -- Only D depends on B, so only those rebuild
            diamondProject
        , testCache
            "Nested source directory file invalidation"
            (["--all"], "")
            [PackageIdentifier "package-a" "0.0.1", PackageIdentifier "package-b" "0.0.1"]
            (const $ appendFile "./package-a/daml/daml2/daml3/daml4/PackageAMain.daml" "\nmyDef = 3")
            (["--all"], "")
            [PackageIdentifier "package-a" "0.0.1", PackageIdentifier "package-b" "0.0.1"]
            (simpleTwoPackageProjectSource "daml/daml2/daml3/daml4")
        , testCache
            "Direct source directory file invalidation"
            (["--all"], "")
            [PackageIdentifier "package-a" "0.0.1", PackageIdentifier "package-b" "0.0.1"]
            (const $ appendFile "./package-a/PackageAMain.daml" "\nmyDef = 3")
            (["--all"], "")
            [PackageIdentifier "package-a" "0.0.1", PackageIdentifier "package-b" "0.0.1"]
            (simpleTwoPackageProjectSource ".")
        , testCache
            "Source daml file dependency invalidation"
            (["--all"], "")
            [PackageIdentifier "package-a" "0.0.1", PackageIdentifier "package-b" "0.0.1"]
            (const $ appendFile "./package-a/daml/PackageAAux.daml" "\nmyDef = 3")
            (["--all"], "")
            [PackageIdentifier "package-a" "0.0.1", PackageIdentifier "package-b" "0.0.1"]
            simpleTwoPackageProjectSourceDaml
        , testCache
            "Source daml file dependency invalidation with upwards structure"
            (["--all"], "")
            [PackageIdentifier "package-a" "0.0.1", PackageIdentifier "package-b" "0.0.1"]
            (const $ appendFile "./package-a/daml/PackageAAux.daml" "\nmyDef = 3")
            (["--all"], "")
            [PackageIdentifier "package-a" "0.0.1", PackageIdentifier "package-b" "0.0.1"]
            simpleTwoPackageProjectSourceDamlUpwards
        , testCache
            "Changing the package name/version with a fixed --output should invalidate the cache"
            (["--all"], "")
            [PackageIdentifier "package-a" "0.0.1", PackageIdentifier "package-b" "0.0.1"]
            (const $ buildProjectStructure "./package-a" (damlYaml "package-a2" "0.0.1" []) {dyOutPath = Just "../package-a.dar"})
            (["--all"], "")
            [PackageIdentifier "package-a" "0.0.1", PackageIdentifier "package-b" "0.0.1"]
            customOutPathProject
        , -- This passes because *something* is setting DAML_SDK_VERSION to 0.0.0, overriding the sdk version listed in the daml.yaml
          -- So we only detect that it has changed, but the new version isn't being used.
          testCache
            "Sdk version should invalidate the cache."
            (["--all"], "")
            [PackageIdentifier "package-a" "0.0.1", PackageIdentifier "package-b" "0.0.1"]
            (const $ buildProjectStructure "./package-a" $ (damlYaml "package-a" "0.0.1" []) {dySdkVersion = Just "10.0.0"})
            (["--all"], "")
            [PackageIdentifier "package-a" "0.0.1"]
            simpleTwoPackageProject
        , -- These next 3 tests rely on caching using the daml.yaml, which is currently doesn't. They should all fail.
          -- The user-facing solution for this now is --no-cache, or building directory on that package.
          testCacheFails
            -- This test fails as we do not check deps that aren't in the daml.yaml
            "Removing a required dependency should invalidate the cache"
            "package-b-0.0.1 should have rebuilt, but didn't" -- This is incorrect, B should have tried to rebuild and failed with a "Module not found" error.
            (["--all"], "")
            [PackageIdentifier "package-a" "0.0.1", PackageIdentifier "package-b" "0.0.1"]
            (const $ buildProjectStructure "./package-b" $ damlYaml "package-b" "0.0.1" [])
            (["--all"], "")
            [PackageIdentifier "package-b" "0.0.1"]
            simpleTwoPackageProject
        , testCacheFails
            "Changing module prefixes should invalidate the cache"
            "package-b-0.0.1 should have rebuilt, but didn't"
            (["--all"], "")
            [PackageIdentifier "package-a" "0.0.1", PackageIdentifier "package-b" "0.0.1"]
            (const $ buildProjectStructure "./package-b" $ damlYaml "package-b" "0.0.1" [])
            (["--all"], "")
            [PackageIdentifier "package-b" "0.0.1"]
            simpleTwoPackageProjectModulePrefixes
        , testCacheFails
            "Changing ghc-options, or other `build-options` should invalidate the cache"
            "package-b-0.0.1 should have rebuilt, but didn't"
            (["--all"], "")
            [PackageIdentifier "package-a" "0.0.1", PackageIdentifier "package-b" "0.0.1"]
            (const $ buildProjectStructure "./package-b" $ (damlYaml "package-b" "0.0.1" []) {dyGhcOptions = ["-wError"]})
            (["--all"], "")
            [PackageIdentifier "package-b" "0.0.1"]
            warningProject
        ]
    , testGroup
        "Composite Dar Building"
        [ test "Building a single composite dar works and builds only the packages needed" ["--composite-dar=main"] "" compositeDarProject $ Right 
            [ PackageIdentifier "package-a" "0.0.1"
            , PackageIdentifier "package-b" "0.0.1"
            , PackageIdentifier "main" "0.0.1"
            ]
        , test "Building two composite dars builds only the packages needed" ["--composite-dar=main", "--composite-dar=test"] "" compositeDarProject $ Right 
            [ PackageIdentifier "package-a" "0.0.1"
            , PackageIdentifier "package-b" "0.0.1"
            , PackageIdentifier "package-c" "0.0.1"
            , PackageIdentifier "package-d" "0.0.1"
            , PackageIdentifier "main" "0.0.1"
            , PackageIdentifier "test" "0.0.1"
            ]
        , test "Building --all and a composite dar builds every package except the other composite dar" ["--all", "--composite-dar=main"] "" compositeDarProject $ Right 
            [ PackageIdentifier "package-a" "0.0.1"
            , PackageIdentifier "package-b" "0.0.1"
            , PackageIdentifier "package-c" "0.0.1"
            , PackageIdentifier "package-d" "0.0.1"
            , PackageIdentifier "package-e" "0.0.1"
            , PackageIdentifier "main" "0.0.1"
            ]
        , test "Building --all-composite-dars builds all composite dars and only packages needed for them" ["--all-composite-dars"] "" compositeDarProject $ Right 
            [ PackageIdentifier "package-a" "0.0.1"
            , PackageIdentifier "package-b" "0.0.1"
            , PackageIdentifier "package-c" "0.0.1"
            , PackageIdentifier "package-d" "0.0.1"
            , PackageIdentifier "main" "0.0.1"
            , PackageIdentifier "test" "0.0.1"
            ]
        , test "Building a single composite dar from a subdirectory works" ["--composite-dar=main"] "./package-a" compositeDarProject $ Right 
            [ PackageIdentifier "package-a" "0.0.1"
            , PackageIdentifier "package-b" "0.0.1"
            , PackageIdentifier "main" "0.0.1"
            ]
        , test "Building a transitive composite dar gives an error including where the dar is" ["--composite-dar=unshadowed-transitive"] "./first" compositeDarShadowingProject
            $ Left "Found definition for unshadowed-transitive in the following sub-projects"
        , test "Building a repeated transitive dar (same name and version) gives error" ["--composite-dar=main"] "" compositeDarDuplicateProject
            $ Left "Multiple composite dars with the same name and version: main-0.0.1"
        , test "Building a composite dar by name only when another with the same name exists fails" ["--composite-dar=same-name"] "./first" compositeDarShadowingProject
            $ Left "Multiple composite dars with the same name were found, to specify which you need, use one of the following full names:\n  - same-name-0.0.1\n  - same-name-0.0.2\n"
        , test "Building a duplicate name dar by name and version succeeds" ["--composite-dar=same-name-0.0.1"] "./first" compositeDarShadowingProject
            $ Right [ PackageIdentifier "same-name" "0.0.1", PackageIdentifier "same-name-package" "0.0.1" ]
        , test "Building a non-existent composite dar fails as expected" ["--composite-dar=some-composite-dar"] "./first" compositeDarShadowingProject
            $ Left "Couldn't find composite dar with the name some-composite-dar in"
        , testWithStdout "Building a shadowed transitive dar succeeds with warning" ["--composite-dar=shadowed-transitive"] "./first" compositeDarShadowingProject
            (Right [ PackageIdentifier "shadowed-transitive" "0.0.1", PackageIdentifier "shadowed-transitive-package" "0.0.1" ])
            (Just "Warning: Found definition for shadowed-transitive in the following sub-projects:")
        ]
    , testGroup
        "Composite Dar Artifact"
        [ withCompositeDar compositeDarProject "main-0.0.1" $ \getDarData -> testGroup "main-0.0.1" $
            [ testCase "Only has expected packages" $ do
                darInfo <- snd <$> getDarData
                let dalfInfos = fmap snd $ HashMap.toList $ Reader.packages darInfo
                    notPrimOrStdlib dalfName = not $ "daml-prim" `isPrefixOf` dalfName || "daml-stdlib" `isPrefixOf` dalfName
                    nonDamlDalfInfos = filter (notPrimOrStdlib . takeFileName . Reader.dalfFilePath) dalfInfos
                    nonDamlPkgNames = maybe "unknown" LF.unPackageName . Reader.dalfPackageName <$> nonDamlDalfInfos

                sort nonDamlPkgNames @?= ["main", "package-a", "package-b"]
            , testCase "Main package has correct name and version" $ do
                darInfo <- snd <$> getDarData
                let ownDalfInfo = fromMaybe (error "Missing own package") $ HashMap.lookup (Reader.mainPackageId darInfo) $ Reader.packages darInfo

                Reader.dalfPackageName ownDalfInfo @?= Just (LF.PackageName "main")
                Reader.dalfPackageVersion ownDalfInfo @?= Just (LF.PackageVersion "0.0.1")
            , testCase "All packages have correct LF version" $ do
                darInfo <- snd <$> getDarData
                let dalfInfos = fmap snd $ HashMap.toList $ Reader.packages darInfo
                    isPrimOrStdlib dalfInfo = 
                      let dalfName = takeFileName $ Reader.dalfFilePath dalfInfo
                       in "daml-prim" `isPrefixOf` dalfName || "daml-stdlib" `isPrefixOf` dalfName
                -- We do not check daml-prim/daml-stdlib, as they are in very old LF versions
                forM_ (filter (not . isPrimOrStdlib) dalfInfos) $ \dalfInfo -> do
                  let dalfLfVersion = LF.packageLfVersion $ Reader.dalfPackage dalfInfo
                      dalfName = maybe "unknown" (T.unpack . LF.unPackageName) $ Reader.dalfPackageName dalfInfo
                  
                  assertEqual ("LF version for " <> dalfName <> " was incorrect") version1_15 dalfLfVersion
            , testCase "Dar has no source code" $ do
                darInfo <- snd <$> getDarData
                let damlFiles = filter (isExtensionOf "daml") $ Reader.files darInfo
                unless (null damlFiles) $ assertFailure $ "Expected no daml files but found " <> show damlFiles
            , testCase "Main package is empty" $ do
                darInfo <- snd <$> getDarData
                let ownDalfInfo = fromMaybe (error "Missing own package") $ HashMap.lookup (Reader.mainPackageId darInfo) $ Reader.packages darInfo
                assertBool "Main package has modules" $ NM.null $ LF.packageModules $ Reader.dalfPackage ownDalfInfo
            , testCase "Works with java codegen" $ do
                darPath <- fst <$> getDarData
                let codegenPath = replaceFileName darPath "java-codegen"
                void $ readProcess damlAssistant ["codegen", "java", darPath, "--output-directory", codegenPath] ""
                listDirectory codegenPath >>= assertBool "Codegen directory is empty" . not . null
            ] <> 
            [ testCase "Works with js codegen" $ do
                darPath <- fst <$> getDarData
                let codegenPath = replaceFileName darPath "js-codegen"
                void $ readProcess damlAssistant ["codegen", "js", darPath, "-o", codegenPath] ""
                listDirectory codegenPath >>= assertBool "Codegen directory is empty" . not . null
            -- The '@daml/types' NPM package is not available on Windows which
            -- is required by 'daml2js'.
            | not isWindows
            ]
        ]
    ]

  where
    test
      :: String
      -> [String]
      -> FilePath
      -> [ProjectStructure]
      -- Left is error regex, right is success + expected packages to have build.
      -- Any created dar files that aren't listed here throw an error.
      -> Either T.Text [PackageIdentifier]
      -> TestTree
    test name flags runPath projectStructure expectedResult =
      testWithStdout name flags runPath projectStructure expectedResult Nothing

    testWithStdout
      :: String
      -> [String]
      -> FilePath
      -> [ProjectStructure]
      -- Left is error regex, right is success + expected packages to have build.
      -- Any created dar files that aren't listed here throw an error.
      -> Either T.Text [PackageIdentifier]
      -> Maybe String
      -> TestTree
    testWithStdout name flags runPath projectStructure expectedResult expectedStdout =
      testCase name $
      withTempDir $ \dir -> do
        allPossibleDars <- buildProject dir projectStructure
        runBuildAndAssert dir flags runPath allPossibleDars expectedResult expectedStdout

    testCache 
      :: String -- name
      -> ([String], FilePath) -- args, runPath
      -> [PackageIdentifier] -- what should have been built
      -> ((FilePath -> IO ()) -> IO ()) -- Modifications
      -> ([String], FilePath) -- args, runPath
      -> [PackageIdentifier] -- what should have been built
      -> [ProjectStructure] -- structure
      -> TestTree
    testCache name firstRun firstRunPkgs doModification secondRun secondRunPkgs projectStructure =
      testCase name $ testCacheIO firstRun firstRunPkgs doModification secondRun secondRunPkgs projectStructure

    -- Tests that currently fail, and require fixing
    testCacheFails
      :: String -- name
      -> String -- Expected error
      -> ([String], FilePath) -- args, runPath
      -> [PackageIdentifier] -- what should have been built
      -> ((FilePath -> IO ()) -> IO ()) -- Modifications
      -> ([String], FilePath) -- args, runPath
      -> [PackageIdentifier] -- what should have been built
      -> [ProjectStructure] -- structure
      -> TestTree
    testCacheFails name expectedMsg firstRun firstRunPkgs doModification secondRun secondRunPkgs projectStructure =
      testCase name $ do
        res <- try @HUnitFailure $ testCacheIO firstRun firstRunPkgs doModification secondRun secondRunPkgs projectStructure
        case res of
          Left (HUnitFailure _ msg) | expectedMsg `isInfixOf` msg -> pure ()
          _ -> assertFailure $ "Expected failure containing " <> expectedMsg <> " but got " <> show res

    testCacheIO
      :: ([String], FilePath) -- args, runPath
      -> [PackageIdentifier] -- what should have been built
      -> ((FilePath -> IO ()) -> IO ()) -- Modifications
      -> ([String], FilePath) -- args, runPath
      -> [PackageIdentifier] -- what should have been built
      -> [ProjectStructure] -- structure
      -> IO ()
    testCacheIO firstRun firstRunPkgs doModification secondRun secondRunPkgs projectStructure =
      withTempDir $ \dir -> do
        allPossibleDars <- buildProject dir projectStructure
        let runBuild :: ([String], FilePath) -> [PackageIdentifier] -> IO ()
            runBuild (flags, runPath) pkgs = runBuildAndAssert dir flags runPath allPossibleDars (Right pkgs) Nothing
            getPkgsLastModified :: [PackageIdentifier] -> IO (Map.Map PackageIdentifier UTCTime)
            getPkgsLastModified pkgs =
              -- fromJust is safe as long as called after a runBuild, since that asserts all pkgs exists in allPossibleDars
              Map.fromList <$> traverse (\pkg -> fmap (pkg,) $ getModificationTime $ dir </> fromJust (Map.lookup pkg allPossibleDars)) pkgs
        
        -- Do the first build, get the modified times of all files built
        runBuild firstRun firstRunPkgs
        modifiedTimes <- getPkgsLastModified firstRunPkgs
        
        -- Apply the modification
        withCurrentDirectory dir $ doModification $
          \path -> void $ readCreateProcessWithExitCode ((proc damlAssistant ["build"]) {cwd = Just path}) []
        
        -- Run the second build, expecting all the secondRunPkgs and the pre-existing firstRunPkgs
        runBuild secondRun (secondRunPkgs `union` firstRunPkgs)

        -- Packages that we expect to have been built by first and second should have their modified time changes
        let pkgsExpectedModified = secondRunPkgs `intersect` firstRunPkgs
        expectedChangedModifiedTimes <- getPkgsLastModified pkgsExpectedModified
        -- fromJust is safe as newModifiedTimes is a subset of modifiedTimes
        void $ Map.traverseWithKey
          (\pkg newTime -> assertBool (show pkg <> " should have rebuilt, but didn't") $ newTime /= fromJust (Map.lookup pkg modifiedTimes))
          expectedChangedModifiedTimes

        -- Packages that we expect to have been built by first and not second should not have their modified time changed
        let pkgExpectedUnchanged = firstRunPkgs \\ secondRunPkgs
        expectedUnchangedModifiedTimes <- getPkgsLastModified pkgExpectedUnchanged
        void $ Map.traverseWithKey 
          (\pkg newTime -> assertBool (show pkg <> " shouldn't have rebuilt, but did") $ newTime == fromJust (Map.lookup pkg modifiedTimes))
          expectedUnchangedModifiedTimes

    runBuildAndAssert
      :: FilePath
      -> [String]
      -> FilePath
      -> Map.Map PackageIdentifier FilePath
      -> Either T.Text [PackageIdentifier]
      -> Maybe String
      -> IO ()
    runBuildAndAssert dir flags runPath allPossibleDars expectedResult expectedStdout = do
      -- Quick check to ensure all the package identifiers are possible
      case expectedResult of
        Left _ -> pure ()
        Right expectedPackageIdentifiers ->
          forM_ expectedPackageIdentifiers $ \pkg ->
            unless (Map.member pkg allPossibleDars) $
              assertFailure $ "Package " <> show pkg <> " can never be built by this setup. Did you mean one of: "
                <> intercalate ", " (show <$> Map.keys allPossibleDars)

      runPath <- canonicalizePath $ dir </> runPath
      let args = ["build", "--enable-multi-package=yes"] <> flags
          process = (proc damlAssistant args) {cwd = Just runPath}
      (exitCode, out, err) <- readCreateProcessWithExitCode process ""
      forM_ expectedStdout $ \expectedOut -> assertBool "Stdout did not contained expected string" $ expectedOut `isInfixOf` out

      case expectedResult of
        Right expectedPackageIdentifiers -> do
          unless (exitCode == ExitSuccess) $ assertFailure $ "Expected success and got " <> show exitCode <> ".\n  StdErr: \n  " <> err

          void $ flip Map.traverseWithKey allPossibleDars $ \pkg darPath -> do
            darExists <- doesFileExist $ dir </> darPath
            let darShouldExist = pkg `elem` expectedPackageIdentifiers
            unless (darExists == darShouldExist) $ do
              assertFailure $ if darExists
                then "Found dar for " <> show pkg <> " when it should not have been built."
                else "Couldn't find dar for " <> show pkg <> " when it should have been built."
        Left regex -> do
          assertBool "succeeded unexpectedly" $ exitCode /= ExitSuccess
          unless (matchTest (makeRegex regex :: Regex) err) $
            assertFailure ("Regex '" <> show regex <> "' did not match stderr:\n" <> show err)

    -- Returns paths of all possible expected Dars
    buildProject :: FilePath -> [ProjectStructure] -> IO (Map.Map PackageIdentifier FilePath)
    buildProject initialPath = fmap mconcat . traverse (buildProjectStructure' initialPath initialPath)

    -- Build a single "node" for convenient modification of daml.yamls
    buildProjectStructure :: FilePath -> ProjectStructure -> IO ()
    buildProjectStructure path projectStructure = void $ buildProjectStructure' path path projectStructure

    buildProjectStructure' :: FilePath -> FilePath -> ProjectStructure -> IO (Map.Map PackageIdentifier FilePath)
    buildProjectStructure' initialPath path = \case
      damlYaml@DamlYaml {} -> do
        TIO.writeFile (path </> "daml.yaml") $ T.unlines $
          [ "sdk-version: " <> fromMaybe (T.pack sdkVersion) (dySdkVersion damlYaml)
          , "name: " <> dyName damlYaml
          , "source: " <> dySource damlYaml
          , "version: " <> dyVersion damlYaml
          , "dependencies:"
          , "  - daml-prim"
          , "  - daml-stdlib"
          , "data-dependencies:"
          ]
          ++ fmap ("  - " <>) (dyDeps damlYaml)
          ++ ["module-prefixes:"]
          ++ fmap (\(pkg, pref) -> "  " <> T.pack (show pkg) <> ": " <> pref) (dyModulePrefixes damlYaml)
          ++ ["build-options:"]
          ++ maybe [] (\outputPath -> 
              [ "  - --output"
              , "  - " <> outputPath
              ]
            ) (dyOutPath damlYaml)
          ++ fmap ("  - --ghc-option=" <>) (dyGhcOptions damlYaml)
        let relDarPath = fromMaybe (".daml/dist/" <> dyName damlYaml <> "-" <> dyVersion damlYaml <> ".dar") (dyOutPath damlYaml)
        outPath <- canonicalizePath $ path </> T.unpack relDarPath
        pure $ Map.singleton (PackageIdentifier (dyName damlYaml) (dyVersion damlYaml)) $ makeRelative initialPath outPath
      multiPackage@MultiPackage {} -> do
        TIO.writeFile (path </> "multi-package.yaml") $ T.unlines
          $  ["packages:"] ++ fmap ("  - " <>) (mpPackages multiPackage)
          ++ ["projects:"] ++ fmap ("  - " <>) (mpProjects multiPackage)
          ++ ["composite-dars:"]
          ++ concatMap (\cd ->
               [ "  - name: " <> cddName cd
               , "    version: " <> cddVersion cd
               ]
               <> ["    packages: "] <> fmap ("      - " <>) (cddPackages cd)
               <> ["    dars: "] <> fmap ("      - " <>) (cddDars cd)
               <> ["    path: " <> cddPath cd]
             ) (mpCompositeDars multiPackage)
        fmap Map.fromList $ forM (mpCompositeDars multiPackage) $ \cd -> do
          outPath <- canonicalizePath $ path </> T.unpack (cddPath cd)
          pure (PackageIdentifier (cddName cd) (cddVersion cd), outPath)
      dir@Dir {} -> do
        let newDir = path </> (T.unpack $ dName dir)
        createDirectoryIfMissing True newDir
        mconcat <$> traverse (buildProjectStructure' initialPath newDir) (dContents dir)
      damlSource@DamlSource {} -> do
        let damlFileName = T.unpack $ last (T.split (=='.') $ dsModuleName damlSource) <> ".daml"
        TIO.writeFile (path </> damlFileName) $ T.unlines $
          ["module " <> dsModuleName damlSource <> " where"]
          ++ fmap (\dep -> "import " <> dep <> " ()") (dsDeps damlSource)
        pure Map.empty
      genericFile@GenericFile {} -> do
        TIO.writeFile (path </> T.unpack (gfName genericFile)) $ gfContent genericFile
        pure Map.empty

    withCompositeDar :: [ProjectStructure] -> String -> (IO (FilePath, Reader.InspectInfo) -> TestTree) -> TestTree
    withCompositeDar projectStructure compositeDarFullName f =
      let acquireResource = do
            (dir, removeDir) <- newTempDir
            darMapping <- buildProject dir projectStructure

            let isCorrectPackage :: PackageIdentifier -> Bool
                isCorrectPackage pkgId = compositeDarFullName == show pkgId
            compositeDarPath <-
              maybe (assertFailure "Failed to find given composite dar name. Use the full path (name-version)") (pure . snd)
                $ find (isCorrectPackage . fst) $ Map.toList darMapping

            let args = ["build", "--enable-multi-package=yes", "--composite-dar=" <> compositeDarFullName]
                process = (proc damlAssistant args) {cwd = Just dir}
            _ <- readCreateProcess process ""
            darInfo <- Reader.getDarInfo compositeDarPath
            pure (removeDir, (compositeDarPath, darInfo))
       in withResource acquireResource fst $ f . fmap snd

----- Testing project fixtures

-- daml.yaml with current sdk version, default ouput path and source set to `daml`
damlYaml :: T.Text -> T.Text -> [T.Text] -> ProjectStructure
damlYaml name version deps = DamlYaml name version Nothing "daml" Nothing [] [] deps

-- B depends on A
simpleTwoPackageProject :: [ProjectStructure]
simpleTwoPackageProject =
  [ MultiPackage ["./package-a", "./package-b"] [] []
  , Dir "package-a"
    [ damlYaml "package-a" "0.0.1" []
    , Dir "daml" [DamlSource "PackageAMain" []]
    ]
  , Dir "package-b"
    [ damlYaml "package-b" "0.0.1" ["../package-a/.daml/dist/package-a-0.0.1.dar"]
    , Dir "daml" [DamlSource "PackageBMain" ["PackageAMain"]]
    ]
  ]

-- B and C depend on A, D depends on B and C
diamondProject :: [ProjectStructure]
diamondProject =
  [ MultiPackage ["./package-a", "./package-b", "./package-c", "./package-d"] [] []
  , Dir "package-a"
    [ damlYaml "package-a" "0.0.1" []
    , Dir "daml" [DamlSource "PackageAMain" []]
    ]
  , Dir "package-b"
    [ damlYaml "package-b" "0.0.1" ["../package-a/.daml/dist/package-a-0.0.1.dar"]
    , Dir "daml" [DamlSource "PackageBMain" ["PackageAMain"]]
    ]
  , Dir "package-c"
    [ damlYaml "package-c" "0.0.1" ["../package-a/.daml/dist/package-a-0.0.1.dar"]
    , Dir "daml" [DamlSource "PackageCMain" ["PackageAMain"]]
    ]
  , Dir "package-d"
    [ damlYaml "package-d" "0.0.1" ["../package-b/.daml/dist/package-b-0.0.1.dar", "../package-c/.daml/dist/package-c-0.0.1.dar"]
    , Dir "daml" [DamlSource "PackageDMain" ["PackageBMain", "PackageCMain"]]
    ]
  ]

-- Package-b depends on package-a, package-a depends on lib-b, lib-b depends on lib-a
-- Straight line dependency tree crossing a "project" border
multiProject :: [ProjectStructure]
multiProject =
  [ Dir "libs"
    [ MultiPackage ["./lib-a", "./lib-b"] [] []
    , Dir "lib-a"
      [ damlYaml "lib-a" "0.0.1" []
      , Dir "daml" [DamlSource "LibAMain" []]
      ]
    , Dir "lib-b"
      [ damlYaml "lib-b" "0.0.1" ["../lib-a/.daml/dist/lib-a-0.0.1.dar"]
      , Dir "daml" [DamlSource "LibBMain" ["LibAMain"]]
      ]
    ]
  , Dir "packages"
    [ MultiPackage ["./package-a", "./package-b"] ["../libs"] []
    , Dir "package-a"
      [ damlYaml "package-a" "0.0.1" ["../../libs/lib-b/.daml/dist/lib-b-0.0.1.dar"]
      , Dir "daml" [DamlSource "PackageAMain" ["LibBMain"]]
      ]
    , Dir "package-b"
      [ damlYaml "package-b" "0.0.1" ["../package-a/.daml/dist/package-a-0.0.1.dar"]
      , Dir "daml" [DamlSource "PackageBMain" ["PackageAMain"]]
      ]
    ]
  ]

-- Cyclic `project` definitions in multi-package.yamls
cyclicMultiPackage :: [ProjectStructure]
cyclicMultiPackage =
  [ Dir "libs"
    [ MultiPackage ["./lib-a"] ["../packages"] []
    , Dir "lib-a"
      [ damlYaml "lib-a" "0.0.1" []
      , Dir "daml" [DamlSource "LibAMain" []]
      ]
    ]
  , Dir "packages"
    [ MultiPackage ["./package-a"] ["../libs"] []
    , Dir "package-a"
      [ damlYaml "package-a" "0.0.1" ["../../libs/lib-a/.daml/dist/lib-a-0.0.1.dar"]
      , Dir "daml" [DamlSource "PackageAMain" ["LibAMain"]]
      ]
    ]
  ]

-- Cyclic dar dependencies in daml.yamls
cyclicPackagesProject :: [ProjectStructure]
cyclicPackagesProject =
  [ MultiPackage ["./package-a", "./package-b"] [] []
  , Dir "package-a"
    [ damlYaml "package-a" "0.0.1" ["../package-b/.daml/dist/package-b-0.0.1.dar"]
    , Dir "daml" [DamlSource "PackageAMain" ["PackageBMain"]]
    ]
  , Dir "package-b"
    [ damlYaml "package-b" "0.0.1" ["../package-a/.daml/dist/package-a-0.0.1.dar"]
    , Dir "daml" [DamlSource "PackageBMain" ["PackageAMain"]]
    ]
  ]

-- Package that defines --output, putting `dar` outside of `.daml/dist`
customOutPathProject :: [ProjectStructure]
customOutPathProject =
  [ MultiPackage ["./package-a", "./package-b"] [] []
  , Dir "package-a"
    [ (damlYaml "package-a" "0.0.1" []) {dyOutPath = Just "../package-a.dar" }
    , Dir "daml" [DamlSource "PackageAMain" []]
    ]
  , Dir "package-b"
    [ damlYaml "package-b" "0.0.1" ["../package-a.dar"]
    , Dir "daml" [DamlSource "PackageBMain" ["PackageAMain"]]
    ]
  ]

-- Project where both packages throw warnings, used to detect flag forwarding via -Werror
warningProject :: [ProjectStructure]
warningProject =
  [ MultiPackage ["./package-a", "./package-b"] [] []
  , Dir "package-a"
    [ damlYaml "package-a" "0.0.1" []
    , Dir "daml" [GenericFile "PackageAMain.daml" $ "module PackageAMain where\n" <> warnText]
    ]
  , Dir "package-b"
    [ damlYaml "package-b" "0.0.1" ["../package-a/.daml/dist/package-a-0.0.1.dar"]
    , Dir "daml" [GenericFile "PackageBMain.daml" $ "module PackageBMain where\nimport PackageAMain ()\n" <> warnText]
    ]
  ]
  where
    -- Gives a non-exhaustive case warning
    warnText = "x = case True of True -> True"

-- Same name but different version project
-- v2 depends on v1
sameNameDifferentVersionProject :: [ProjectStructure]
sameNameDifferentVersionProject =
  [ MultiPackage ["./package-v1", "./package-v2"] [] []
  , Dir "package-v1"
    [ damlYaml "package" "0.0.1" []
    , Dir "daml" [DamlSource "PackageV1Main" []]
    ]
  , Dir "package-v2"
    [ damlYaml "package" "0.0.2" ["../package-v1/.daml/dist/package-0.0.1.dar"]
    , Dir "daml" [DamlSource "PackageV2Main" ["PackageV1Main"]]
    ]
  ]

-- Same name and same version project - illegal dependency
-- v1-again depends on v1
sameNameSameVersionProject :: [ProjectStructure]
sameNameSameVersionProject =
  [ MultiPackage ["./package-v1", "./package-v1-again"] [] []
  , Dir "package-v1"
    [ damlYaml "package" "0.0.1" []
    , Dir "daml" [DamlSource "PackageV1Main" []]
    ]
  , Dir "package-v1-again"
    [ damlYaml "package" "0.0.1" ["../package-v1/.daml/dist/package-0.0.1.dar"]
    , Dir "daml" [DamlSource "PackageV1MainSequel" ["PackageV1Main"]]
    ]
  ]

-- B depends on A with specified source folder for package-a
simpleTwoPackageProjectSource :: T.Text -> [ProjectStructure]
simpleTwoPackageProjectSource path =
  [ MultiPackage ["./package-a", "./package-b"] [] []
  , Dir "package-a"
    [ (damlYaml "package-a" "0.0.1" []) {dySource = path}
    , Dir path [DamlSource "PackageAMain" []]
    ]
  , Dir "package-b"
    [ damlYaml "package-b" "0.0.1" ["../package-a/.daml/dist/package-a-0.0.1.dar"]
    , Dir "daml" [DamlSource "PackageBMain" ["PackageAMain"]]
    ]
  ]

-- B depends on A where package-a uses a .daml file source in daml.yaml
simpleTwoPackageProjectSourceDaml :: [ProjectStructure]
simpleTwoPackageProjectSourceDaml =
  [ MultiPackage ["./package-a", "./package-b"] [] []
  , Dir "package-a"
    [ (damlYaml "package-a" "0.0.1" []) {dySource = "daml/PackageAMain.daml"}
    , Dir "daml" [DamlSource "PackageAMain" ["PackageAAux"], DamlSource "PackageAAux" []]
    ]
  , Dir "package-b"
    [ damlYaml "package-b" "0.0.1" ["../package-a/.daml/dist/package-a-0.0.1.dar"]
    , Dir "daml" [DamlSource "PackageBMain" ["PackageAMain"]]
    ]
  ]

-- B depends on A where package-a uses a .daml file source in daml.yaml
-- This daml file depends on another daml file higher up the file system hierarchy
simpleTwoPackageProjectSourceDamlUpwards :: [ProjectStructure]
simpleTwoPackageProjectSourceDamlUpwards =
  [ MultiPackage ["./package-a", "./package-b"] [] []
  , Dir "package-a"
    [ (damlYaml "package-a" "0.0.1" []) {dySource = "daml/PackageA/PackageAMain.daml"}
    , Dir "daml" [DamlSource "PackageAAux" [], Dir "PackageA" [DamlSource "PackageA.PackageAMain" ["PackageAAux"]]]
    ]
  , Dir "package-b"
    [ damlYaml "package-b" "0.0.1" ["../package-a/.daml/dist/package-a-0.0.1.dar"]
    , Dir "daml" [DamlSource "PackageBMain" ["PackageA.PackageAMain"]]
    ]
  ]

simpleTwoPackageProjectModulePrefixes :: [ProjectStructure]
simpleTwoPackageProjectModulePrefixes =
  [ MultiPackage ["./package-a", "./package-b"] [] []
  , Dir "package-a"
    [ damlYaml "package-a" "0.0.1" []
    , Dir "daml" [DamlSource "PackageAMain" []]
    ]
  , Dir "package-b"
    [ (damlYaml "package-b" "0.0.1" ["../package-a/.daml/dist/package-a-0.0.1.dar"]) {dyModulePrefixes = [(PackageIdentifier "package-a" "0.0.1", "A")]}
    , Dir "daml" [DamlSource "PackageBMain" ["A.PackageAMain"]]
    ]
  ]

-- 5 Packages, A-E. 2 Composite dars, first depends on A,B, second on C,D
-- Each package has a unique module name and defines a template, for codegen testing
-- (java codegen won't generate any files if all modules are empty)
compositeDarProject :: [ProjectStructure]
compositeDarProject =
    [ MultiPackage ["./package-a", "./package-b", "./package-c", "./package-d", "./package-e"] []
        [ CompositeDarDefinition "main" "0.0.1" ["./package-a", "./package-b"] [] "./main.dar"
        , CompositeDarDefinition "test" "0.0.1" ["./package-c", "./package-d"] [] "./test.dar"
        ]
    , simplePackage "package-a" "Main1"
    , simplePackage "package-b" "Main2"
    , simplePackage "package-c" "Main3"
    , simplePackage "package-d" "Main4"
    , simplePackage "package-e" "Main5"
    ]
  where
    -- Simple package with one module that defines a template
    -- Names need to be unique for java codegen
    simplePackage :: T.Text -> T.Text -> ProjectStructure
    simplePackage name moduleName = Dir name
      [ damlYaml name "0.0.1" []
      , Dir "daml" [GenericFile (moduleName <> ".daml") ("module " <> moduleName <> " where template T with p : Party where signatory p")]
      ]

-- first multi-package
--   shadowed-transitive
--   same-name-0.0.1
--   same-name-0.0.2
--     
-- second multi-package
--   unshadowed-transitive
--   shadowed-transitive
-- each composite dar has one package unique to it
compositeDarShadowingProject :: [ProjectStructure]
compositeDarShadowingProject = 
    [ Dir "first" $ compositeDarsStructure [("shadowed-transitive", "0.0.1"), ("same-name", "0.0.1"), ("same-name", "0.0.2")] ["../second"]
    , Dir "second" $ compositeDarsStructure [("shadowed-transitive", "0.0.1"), ("unshadowed-transitive", "0.0.1")] []
    ]
  where
    -- For each name and version, make a composite dar entry in the multi-package and a single package that it is composed of
    compositeDarsStructure :: [(T.Text, T.Text)] -> [T.Text] -> [ProjectStructure]
    compositeDarsStructure compositeDars projects =
      [ MultiPackage ((\(name, version) -> "./" <> name <> "-" <> version) <$> compositeDars) projects $
          (\(name, version) -> CompositeDarDefinition name version ["./" <> name <> "-" <> version] [] ("./" <> name <> "-" <> version <> ".dar")) <$> compositeDars
      ] <>
      ((\(name, version) ->
        Dir (name <> "-" <> version)
          [ damlYaml (name <> "-package") version []
          , Dir "daml" [DamlSource "Main" []]
          ]
      ) <$> compositeDars)

compositeDarDuplicateProject :: [ProjectStructure]
compositeDarDuplicateProject =
  [ MultiPackage ["./package-a"] []
      [ CompositeDarDefinition "main" "0.0.1" ["./package-a"] [] "./main.dar"
      , CompositeDarDefinition "main" "0.0.1" ["./package-a"] [] "./main2.dar"
      ]
  , Dir "package-a"
      [ damlYaml "package-a" "0.0.1" []
      , Dir "daml" [DamlSource "Main" []]
      ]
  ]
