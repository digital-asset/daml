-- Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

module DA.Test.DamlcMultiPackage (main) where

{- HLINT ignore "locateRunfiles/package_app" -}

import Control.Monad.Extra
import DA.Bazel.Runfiles
import Data.List
import qualified Data.Map as Map
import Data.Maybe (fromMaybe, fromJust)
import qualified Data.Text as T
import qualified Data.Text.IO as TIO
import Data.Time.Clock
import SdkVersion
import System.Directory.Extra
import System.Environment
import System.Exit
import System.FilePath
import System.IO.Extra
import System.Process
import Test.Tasty
import Test.Tasty.HUnit
import Text.Regex.TDFA

main :: IO ()
main = do
  damlAssistant <- locateRunfiles (mainWorkspace </> "daml-assistant" </> exe "daml")
  defaultMain $ tests damlAssistant

data ProjectStructure
  = DamlYaml
      { dyName :: T.Text
      , dyVersion :: T.Text
      , dySdkVersion :: Maybe T.Text
      , dySource :: T.Text
      , dyOutPath :: Maybe T.Text
      , dyDeps :: [T.Text]
      }
  | MultiPackage
      { mpPackages :: [T.Text]
      , mpProjects :: [T.Text]
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

simpleTwoPackageProject :: [ProjectStructure]
simpleTwoPackageProject =
  [ MultiPackage ["./package-a", "./package-b"] []
  , Dir "package-a"
    [ DamlYaml "package-a" "0.0.1" Nothing "daml" Nothing []
    , Dir "daml" [DamlSource "PackageAMain" []]
    ]
  , Dir "package-b"
    [ DamlYaml "package-b" "0.0.1" Nothing "daml" Nothing ["../package-a/.daml/dist/package-a-0.0.1.dar"]
    , Dir "daml" [DamlSource "PackageBMain" ["PackageAMain"]]
    ]
  ]

-- B and C depend on A, D depends on B and C
diamondProject :: [ProjectStructure]
diamondProject =
  [ MultiPackage ["./package-a", "./package-b", "./package-c", "./package-d"] []
  , Dir "package-a"
    [ DamlYaml "package-a" "0.0.1" Nothing "daml" Nothing []
    , Dir "daml" [DamlSource "PackageAMain" []]
    ]
  , Dir "package-b"
    [ DamlYaml "package-b" "0.0.1" Nothing "daml" Nothing ["../package-a/.daml/dist/package-a-0.0.1.dar"]
    , Dir "daml" [DamlSource "PackageBMain" ["PackageAMain"]]
    ]
  , Dir "package-c"
    [ DamlYaml "package-c" "0.0.1" Nothing "daml" Nothing ["../package-a/.daml/dist/package-a-0.0.1.dar"]
    , Dir "daml" [DamlSource "PackageCMain" ["PackageAMain"]]
    ]
  , Dir "package-d"
    [ DamlYaml "package-d" "0.0.1" Nothing "daml" Nothing ["../package-b/.daml/dist/package-b-0.0.1.dar", "../package-c/.daml/dist/package-c-0.0.1.dar"]
    , Dir "daml" [DamlSource "PackageDMain" ["PackageBMain", "PackageCMain"]]
    ]
  ]

multiProject :: [ProjectStructure]
multiProject =
  [ Dir "libs"
    [ MultiPackage ["./lib-a", "./lib-b"] []
    , Dir "lib-a"
      [ DamlYaml "lib-a" "0.0.1" Nothing "daml" Nothing []
      , Dir "daml" [DamlSource "LibAMain" []]
      ]
    , Dir "lib-b"
      [ DamlYaml "lib-b" "0.0.1" Nothing "daml" Nothing ["../lib-a/.daml/dist/lib-a-0.0.1.dar"]
      , Dir "daml" [DamlSource "LibBMain" ["LibAMain"]]
      ]
    ]
  , Dir "packages"
    [ MultiPackage ["./package-a", "./package-b"] ["../libs"]
    , Dir "package-a"
      [ DamlYaml "package-a" "0.0.1" Nothing "daml" Nothing ["../../libs/lib-b/.daml/dist/lib-b-0.0.1.dar"]
      , Dir "daml" [DamlSource "PackageAMain" ["LibBMain"]]
      ]
    , Dir "package-b"
      [ DamlYaml "package-b" "0.0.1" Nothing "daml" Nothing ["../package-a/.daml/dist/package-a-0.0.1.dar"]
      , Dir "daml" [DamlSource "PackageBMain" ["PackageAMain"]]
      ]
    ]
  ]

cyclicMultiPackage :: [ProjectStructure]
cyclicMultiPackage =
  [ Dir "libs"
    [ MultiPackage ["./lib-a"] ["../packages"]
    , Dir "lib-a"
      [ DamlYaml "lib-a" "0.0.1" Nothing "daml" Nothing []
      , Dir "daml" [DamlSource "LibAMain" []]
      ]
    ]
  , Dir "packages"
    [ MultiPackage ["./package-a"] ["../libs"]
    , Dir "package-a"
      [ DamlYaml "package-a" "0.0.1" Nothing "daml" Nothing ["../../libs/lib-a/.daml/dist/lib-a-0.0.1.dar"]
      , Dir "daml" [DamlSource "PackageAMain" ["LibAMain"]]
      ]
    ]
  ]

cyclicPackagesProject :: [ProjectStructure]
cyclicPackagesProject =
  [ MultiPackage ["./package-a", "./package-b"] []
  , Dir "package-a"
    [ DamlYaml "package-a" "0.0.1" Nothing "daml" Nothing ["../package-b/.daml/dist/package-b-0.0.1.dar"]
    , Dir "daml" [DamlSource "PackageAMain" ["PackageBMain"]]
    ]
  , Dir "package-b"
    [ DamlYaml "package-b" "0.0.1" Nothing "daml" Nothing ["../package-a/.daml/dist/package-a-0.0.1.dar"]
    , Dir "daml" [DamlSource "PackageBMain" ["PackageAMain"]]
    ]
  ]

customOutPathProject :: [ProjectStructure]
customOutPathProject =
  [ MultiPackage ["./package-a", "./package-b"] []
  , Dir "package-a"
    [ DamlYaml "package-a" "0.0.1" Nothing "daml" (Just "../package-a.dar") []
    , Dir "daml" [DamlSource "PackageAMain" []]
    ]
  , Dir "package-b"
    [ DamlYaml "package-b" "0.0.1" Nothing "daml" Nothing ["../package-a.dar"]
    , Dir "daml" [DamlSource "PackageBMain" ["PackageAMain"]]
    ]
  ]

-- Project where both packages throw warnings, used to detect flag forwarding via -werror
warningProject :: [ProjectStructure]
warningProject =
  [ MultiPackage ["./package-a", "./package-b"] []
  , Dir "package-a"
    [ DamlYaml "package-a" "0.0.1" Nothing "daml" (Just "../package-a.dar") []
    , Dir "daml" [GenericFile "PackageAMain.daml" $ "module PackageAMain where\n" <> warnText]
    ]
  , Dir "package-b"
    [ DamlYaml "package-b" "0.0.1" Nothing "daml" Nothing ["../package-a.dar"]
    , Dir "daml" [GenericFile "PackageBMain.daml" $ "module PackageBMain where\nimport PackageAMain ()\n" <> warnText]
    ]
  ]
  where
    -- Gives a non-exhaustive case warning
    warnText = "x = case True of True -> True"

-- Same name but different version project
sameNameDifferentVersionProject :: [ProjectStructure]
sameNameDifferentVersionProject =
  [ MultiPackage ["./package-v1", "./package-v2"] []
  , Dir "package-v1"
    [ DamlYaml "package" "0.0.1" Nothing "daml" Nothing []
    , Dir "daml" [DamlSource "PackageV1Main" []]
    ]
  , Dir "package-v2"
    [ DamlYaml "package" "0.0.2" Nothing "daml" Nothing ["../package-v1/.daml/dist/package-0.0.1.dar"]
    , Dir "daml" [DamlSource "PackageV2Main" ["PackageV1Main"]]
    ]
  ]

-- Same name and same version project - illegal dependency
sameNameSameVersionProject :: [ProjectStructure]
sameNameSameVersionProject =
  [ MultiPackage ["./package-v1", "./package-v1-again"] []
  , Dir "package-v1"
    [ DamlYaml "package" "0.0.1" Nothing "daml" Nothing []
    , Dir "daml" [DamlSource "PackageV1Main" []]
    ]
  , Dir "package-v1-again"
    [ DamlYaml "package" "0.0.1" Nothing "daml" Nothing ["../package-v1/.daml/dist/package-0.0.1.dar"]
    , Dir "daml" [DamlSource "PackageV1MainSequel" ["PackageV1Main"]]
    ]
  ]

{- Cases to test
DONE
- build and build all with reconnecting trees (diamond shape)
- multi-package project field
- cycle detection (both data deps and projects)
- custom out path handling
- flags are forwarded in the correct cases - create a daml file with a warning, use the --ghc-option="-werror" option
- same name different version
- same name and version (should fail)
- calling from within source code
LEFT
- caching logic, somehow ??
    needs to also check that having a weird source doesn't break caching
    check if a file updated: either via the output logs or maaaybe a file changed stamp?
    most tests will now be 2 builds, a setup, followed by some kind of cache invalidation step, then a build to assess what happened
      assertions:
      what was built after setup
      what was built after invalidation - this cannot just be file exists checks, could track all the last modified times of each file after setup, and check it changed?
    cache invalidation:
      - delete a dar
      - run a single build
      - edit a file (+ optionally run a single build)
      we use withCurrentDirectory and local paths, provide generic IO action
      also provide a clean way to envoke a normal build on a directory, so maybe (FilePath -> IO ()) -> IO ()?

Need fancy daml-assistant changes:
- multi-sdk
    Add a flag to daml assistant to make it use custom api/downloading directory
    Bring in an old sdk version as a data resource for this test (using @daml-sdk-2.7.5//:daml)
    Create a mock server/api that serves this file to the downloader
    do test
-}

tests :: FilePath -> TestTree
tests damlAssistant =
  testGroup
    "Multi-Package build"
    [ testGroup
        "Simple two package project"
        [ test "Build A with search" ["--multi-package-search"] "./package-a" simpleTwoPackageProject
            $ Right [PackageIdentifier "package-a" "0.0.1"]
        , test "Build B with search" ["--multi-package-search"] "./package-b" simpleTwoPackageProject
            $ Right [PackageIdentifier "package-a" "0.0.1", PackageIdentifier "package-b" "0.0.1"]
        , test "Build all from A with search" ["--multi-package-search", "--all"] "./package-a" simpleTwoPackageProject
            $ Right [PackageIdentifier "package-a" "0.0.1", PackageIdentifier "package-b" "0.0.1"]
        , test "Build all from B with search" ["--multi-package-search", "--all"] "./package-b" simpleTwoPackageProject
            $ Right [PackageIdentifier "package-a" "0.0.1", PackageIdentifier "package-b" "0.0.1"]
        , test "Build all from root" ["--all"] "" simpleTwoPackageProject
            $ Right [PackageIdentifier "package-a" "0.0.1", PackageIdentifier "package-b" "0.0.1"]
        , test "Build all from A with explicit path" ["--all", "--multi-package-path=.."] "./package-a" simpleTwoPackageProject
            $ Right [PackageIdentifier "package-a" "0.0.1", PackageIdentifier "package-b" "0.0.1"]
        , test "Build B from nested directory with search" ["--multi-package-search"] "./package-b/daml" simpleTwoPackageProject
            $ Right [PackageIdentifier "package-a" "0.0.1", PackageIdentifier "package-b" "0.0.1"]
        ]
    , testGroup
        "Diamond project"
        [ test "Build D with search" ["--multi-package-search"] "./package-d" diamondProject $ Right
            [ PackageIdentifier "package-a" "0.0.1"
            , PackageIdentifier "package-b" "0.0.1"
            , PackageIdentifier "package-c" "0.0.1"
            , PackageIdentifier "package-d" "0.0.1"
            ]
        , test "Build C with search" ["--multi-package-search"] "./package-c" diamondProject $ Right
            [ PackageIdentifier "package-a" "0.0.1"
            , PackageIdentifier "package-c" "0.0.1"
            ]
        , test "Build B with search" ["--multi-package-search"] "./package-b" diamondProject $ Right
            [ PackageIdentifier "package-a" "0.0.1"
            , PackageIdentifier "package-b" "0.0.1"
            ]
        , test "Build A with search" ["--multi-package-search"] "./package-a" diamondProject $ Right
            [ PackageIdentifier "package-a" "0.0.1" ]
        , test "Build all from root" ["--all"] "" diamondProject $ Right
            [ PackageIdentifier "package-a" "0.0.1"
            , PackageIdentifier "package-b" "0.0.1"
            , PackageIdentifier "package-c" "0.0.1"
            , PackageIdentifier "package-d" "0.0.1"
            ]
        , test "Build all from A" ["--multi-package-search", "--all"] "./package-a" diamondProject $ Right
            [ PackageIdentifier "package-a" "0.0.1"
            , PackageIdentifier "package-b" "0.0.1"
            , PackageIdentifier "package-c" "0.0.1"
            , PackageIdentifier "package-d" "0.0.1"
            ]
        ]
    , testGroup
        "Multi project"
        [ test "Build package B" ["--multi-package-search"] "./packages/package-b" multiProject $ Right
            [ PackageIdentifier "lib-a" "0.0.1"
            , PackageIdentifier "lib-b" "0.0.1"
            , PackageIdentifier "package-a" "0.0.1"
            , PackageIdentifier "package-b" "0.0.1"
            ]
        , test "Build package A" ["--multi-package-search"] "./packages/package-a" multiProject $ Right
            [ PackageIdentifier "lib-a" "0.0.1"
            , PackageIdentifier "lib-b" "0.0.1"
            , PackageIdentifier "package-a" "0.0.1"
            ]
        , test "Build lib B" ["--multi-package-search"] "./libs/lib-b" multiProject $ Right
            [ PackageIdentifier "lib-a" "0.0.1"
            , PackageIdentifier "lib-b" "0.0.1"
            ]
        , test "Build lib A" ["--multi-package-search"] "./libs/lib-a" multiProject $ Right
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
        "Cycle detection"
        [ test "Multi-package project cycle from lib-a" ["--multi-package-search"] "./libs/lib-a" cyclicMultiPackage $ Left "Cycle detected"
        , test "Multi-package project cycle from package-a" ["--multi-package-search"] "./packages/package-a" cyclicMultiPackage $ Left "Cycle detected"
        , test "Multi-package project cycle from libs --all" ["--all"] "./libs" cyclicMultiPackage $ Left "Cycle detected"
        , test "Multi-package project cycle from packages --all" ["--all"] "./packages" cyclicMultiPackage $ Left "Cycle detected"
        , test "Package dep cycle from package-a" ["--multi-package-search"] "./package-a" cyclicPackagesProject $ Left "recursion detected"
        , test "Package dep cycle from package-b" ["--multi-package-search"] "./package-b" cyclicPackagesProject $ Left "recursion detected"
        , test "Package dep cycle from root --all" ["--all"] "" cyclicPackagesProject $ Left "recursion detected"
        ]
    , testGroup
        "Special flag behaviour"
        [ test "Multi-package build rebuilds dar with --output" ["--multi-package-search"] "./package-b" customOutPathProject
            $ Right [PackageIdentifier "package-a" "0.0.1", PackageIdentifier "package-b" "0.0.1"]
        , test "Build --all doesn't forward options flags like --ghc-options" ["--all", "--ghc-option=-Werror"] "" warningProject
            $ Right [PackageIdentifier "package-a" "0.0.1", PackageIdentifier "package-b" "0.0.1"]
        , test "Build package a forwards options flags like --ghc-options only to package a" ["--multi-package-search", "--ghc-option=-Werror"] "./package-a" warningProject
            $ Left "Pattern match\\(es\\) are non-exhaustive"
        , test "Build package b forwards options flags like --ghc-options only to package b" ["--multi-package-search", "--ghc-option=-Werror"] "./package-b" warningProject
            $ Left "Created .+/package-a\\.dar(.|\n)+Pattern match\\(es\\) are non-exhaustive"
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
            "B rebuilds is A is manually rebuilt after change"
            (["--all"], "")
            [PackageIdentifier "package-a" "0.0.1", PackageIdentifier "package-b" "0.0.1"] -- First time builds both
            -- Modify package-a/daml/PackageAMain.daml, Delete package-a/.daml/dist/package-a-0.0.1.dar
            (\manualBuild -> do
              appendFile "./package-a/daml/PackageAMain.daml" "\nmyDef = 3"
              manualBuild "./package-a"
            )
            (["--all"], "")
            [PackageIdentifier "package-a" "0.0.1", PackageIdentifier "package-b" "0.0.1"] -- Both have been rebuilt
            simpleTwoPackageProject
        , testCache
            "Top package is always built (A)"
            (["--all"], "")
            [PackageIdentifier "package-a" "0.0.1", PackageIdentifier "package-b" "0.0.1"] -- First time builds both
            (const $ pure ())
            (["--multi-package-search"], "./package-a")
            [PackageIdentifier "package-a" "0.0.1"]
            simpleTwoPackageProject
        , testCache
            "Top package is always built (B)"
            (["--all"], "")
            [PackageIdentifier "package-a" "0.0.1", PackageIdentifier "package-b" "0.0.1"] -- First time builds both
            (const $ pure ())
            (["--multi-package-search"], "./package-b")
            [PackageIdentifier "package-b" "0.0.1"] -- B is rebuilt but gives same package-id, so A is not rebuilt
            simpleTwoPackageProject
        , testCache
            "Only above in the dependency tree is invalidated"
            (["--all"], "")
            [PackageIdentifier "package-a" "0.0.1", PackageIdentifier "package-b" "0.0.1", PackageIdentifier "package-c" "0.0.1", PackageIdentifier "package-d" "0.0.1"]
            (const $ appendFile "./package-b/daml/PackageBMain.daml" "\nmyDef = 3")
            (["--all"], "")
            [PackageIdentifier "package-b" "0.0.1", PackageIdentifier "package-d" "0.0.1"] -- Only D depends on B, so only those rebuild
            diamondProject
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
      testCase name $
      withTempDir $ \dir -> do
        allPossibleDars <- buildProject dir projectStructure
        runBuildAndAssert dir flags runPath allPossibleDars expectedResult

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
      testCase name $
      withTempDir $ \dir -> do
        allPossibleDars <- buildProject dir projectStructure
        let runBuild :: ([String], FilePath) -> [PackageIdentifier] -> IO ()
            runBuild (flags, runPath) pkgs =  runBuildAndAssert dir flags runPath allPossibleDars (Right pkgs)
            getPkgsLastModified :: [PackageIdentifier] -> IO (Map.Map PackageIdentifier UTCTime)
            getPkgsLastModified pkgs =
              -- fromJust is safe as long as called after a runBuild, since that asserts all pkgs exists in allPossibleDars
              Map.fromList <$> traverse (\pkg -> fmap (pkg,) $ getModificationTime $ dir </> fromJust (Map.lookup pkg allPossibleDars)) pkgs
        
        -- Do the first build, get the modified times of all files built
        runBuild firstRun firstRunPkgs
        modifiedTimes <- getPkgsLastModified firstRunPkgs
        
        -- Apply the modification
        withCurrentDirectory dir $ doModification $
          \path -> void $ readCreateProcess ((proc damlAssistant ["build"]) {cwd = Just path}) []
        
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
      -> IO ()
    runBuildAndAssert dir flags runPath allPossibleDars expectedResult = do
      -- Quick check to ensure all the package identifiers are possible
      case expectedResult of
        Left _ -> pure ()
        Right expectedPackageIdentifiers ->
          forM_ expectedPackageIdentifiers $ \pkg ->
            unless (Map.member pkg allPossibleDars) $
              assertFailure $ "Package " <> show pkg <> " can never be built by this setup. Did you mean one of: "
                <> intercalate ", " (show <$> Map.keys allPossibleDars)

      env <- getEnvironment
      runPath <- canonicalizePath $ dir </> runPath
      let args = ["build", "--enable-multi-package=yes"] <> flags
          process = (proc damlAssistant args) {cwd = Just runPath, env = Just $ ("DAML_ASSISTANT", damlAssistant):env}
      (exitCode, _, err) <- readCreateProcessWithExitCode process ""
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
    buildProject initialPath = fmap mconcat . traverse (buildProjectStructure initialPath)
      where
        buildProjectStructure :: FilePath -> ProjectStructure -> IO (Map.Map PackageIdentifier FilePath)
        buildProjectStructure path = \case
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
              ++ maybe [] (\outputPath -> 
                  [ "build-options:"
                  , "  - --output"
                  , "  - " <> outputPath
                  ]
                ) (dyOutPath damlYaml)
            let relDarPath = fromMaybe (".daml/dist/" <> dyName damlYaml <> "-" <> dyVersion damlYaml <> ".dar") (dyOutPath damlYaml)
            outPath <- canonicalizePath $ path </> T.unpack relDarPath
            pure $ Map.singleton (PackageIdentifier (dyName damlYaml) (dyVersion damlYaml)) $ makeRelative initialPath outPath
          multiPackage@MultiPackage {} -> do
            TIO.writeFile (path </> "multi-package.yaml") $ T.unlines
              $  ["packages:"] ++ fmap ("  - " <>) (mpPackages multiPackage)
              ++ ["projects:"] ++ fmap ("  - " <>) (mpProjects multiPackage)
            pure Map.empty
          dir@Dir {} -> do
            let newDir = path </> (T.unpack $ dName dir)
            createDirectoryIfMissing True newDir
            mconcat <$> traverse (buildProjectStructure newDir) (dContents dir)
          damlSource@DamlSource {} -> do
            let damlFileName = T.unpack $ last (T.split (=='.') $ dsModuleName damlSource) <> ".daml"
            TIO.writeFile (path </> damlFileName) $ T.unlines $
              ["module " <> dsModuleName damlSource <> " where"]
              ++ fmap (\dep -> "import " <> dep <> " ()") (dsDeps damlSource)
            pure Map.empty
          genericFile@GenericFile {} -> do
            TIO.writeFile (path </> T.unpack (gfName genericFile)) $ gfContent genericFile
            pure Map.empty