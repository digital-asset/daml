-- Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0
module DA.Test.DataDependencies (main) where

import qualified "zip-archive" Codec.Archive.Zip as Zip
import Control.Monad.Extra
import DA.Bazel.Runfiles
import qualified DA.Daml.LF.Ast as LF
import DA.Daml.LF.Reader (readDalfs, Dalfs(..))
import qualified DA.Daml.LF.Proto3.Archive as LFArchive
import DA.Daml.StablePackages (numStablePackagesForVersion)
import DA.Test.Process
import DA.Test.Util
import qualified Data.ByteString.Lazy as BSL
import Data.List (intercalate, sort, (\\))
import qualified Data.NameMap as NM
import Module (unitIdString)
import System.Directory.Extra
import System.Environment.Blank
import System.FilePath
import System.Info.Extra
import System.IO.Extra
import System.Process
import Test.Tasty
import Test.Tasty.HUnit

import SdkVersion

main :: IO ()
main = do
    setEnv "TASTY_NUM_THREADS" "3" True
    damlc <- locateRunfiles (mainWorkspace </> "compiler" </> "damlc" </> exe "damlc")
    damlcLegacy <- locateRunfiles ("damlc_legacy" </> exe "damlc_legacy")
    damlScriptDar <- locateRunfiles (mainWorkspace </> "daml-script" </> "daml" </> "daml-script.dar")
    oldProjDar <- locateRunfiles (mainWorkspace </> "compiler" </> "damlc" </> "tests" </> "dars" </> "old-proj-0.13.55-snapshot.20200309.3401.0.6f8c3ad8-1.8.dar")
    libWithScriptDar <- locateRunfiles (mainWorkspace </> "compiler" </> "damlc" </> "tests" </> "dars" </> "lib-with-script-0.0.1-sdk-2.2.0-lf-1.14.dar")
    let validate dar = callProcessSilent damlc ["validate-dar", dar]
    defaultMain $ tests Tools{..}

data Tools = Tools -- and places
  { damlc :: FilePath
  , damlcLegacy :: FilePath
  , validate :: FilePath -> IO ()
  , damlScriptDar :: FilePath
  , oldProjDar :: FilePath
  , libWithScriptDar :: FilePath
  }

damlcForTarget :: Tools -> LF.Version -> FilePath
damlcForTarget Tools{damlc, damlcLegacy} target
  | target `elem` LF.supportedOutputVersions = damlc
  | otherwise = damlcLegacy

darPackageIds :: FilePath -> IO [LF.PackageId]
darPackageIds fp = do
    archive <- Zip.toArchive <$> BSL.readFile fp
    Dalfs mainDalf dalfDeps <- either fail pure $ readDalfs archive
    Right dalfPkgIds  <- pure $ mapM (LFArchive.decodeArchivePackageId . BSL.toStrict) $ mainDalf : dalfDeps
    pure dalfPkgIds

-- | We test two sets of versions:
-- 1. Versions no longer supported as output versions by damlc are tested against
--    1.14.
-- 2. For all other versions we test them against the next version + an extra (1.dev, 1.dev) pair.
lfVersionTestPairs :: [(LF.Version, LF.Version)]
lfVersionTestPairs =
    let legacyPairs = map (, LF.version1_14) (LF.supportedInputVersions \\ LF.supportedOutputVersions)
        versions = sort LF.supportedOutputVersions ++ [LF.versionDev]
    in legacyPairs ++ zip versions (tail versions)

tests :: Tools -> TestTree
tests tools = testGroup "Data Dependencies" $
    [ testCaseSteps ("Cross Daml-LF version: " <> LF.renderVersion depLfVer <> " -> " <> LF.renderVersion targetLfVer)  $ \step -> withTempDir $ \tmpDir -> do
          let proja = tmpDir </> "proja"
          let projb = tmpDir </> "projb"

          step "Build proja"
          createDirectoryIfMissing True (proja </> "src")
          writeFileUTF8 (proja </> "src" </> "A.daml") $ unlines
              [ "module A where"
              , "import DA.Text"
              , "data A = A Int deriving Show"
              -- This ensures that we have a reference to daml-stdlib and therefore daml-prim.
              , "x : [Text]"
              , "x = lines \"abc\\ndef\""
              , "data X = X" -- This should generate a Daml-LF enum

              , "template T"
              , "  with"
              , "    p : Party"
              , "  where"
              , "    signatory p"

              , "createT = create @T"
              , "signatoryT = signatory @T"
              , "archiveT = archive @T"
              ]
          writeFileUTF8 (proja </> "daml.yaml") $ unlines
              [ "sdk-version: " <> sdkVersion
              , "name: proja"
              , "version: 0.0.1"
              , "source: src"
              , "dependencies: [daml-prim, daml-stdlib]"
              ]
          callProcessSilent (damlcForTarget tools depLfVer)
                ["build"
                , "--project-root", proja
                , "--target", LF.renderVersion depLfVer
                , "-o", proja </> "proja.dar"
                ]
          projaPkgIds <- darPackageIds (proja </> "proja.dar")
          -- daml-stdlib, daml-prim and proja
          length projaPkgIds @?= numStablePackagesForVersion depLfVer + 2 + 1

          step "Build projb"
          createDirectoryIfMissing True (projb </> "src")
          writeFileUTF8 (projb </> "src" </> "B.daml") $ unlines
              [ "module B where"
              , "import A"
              , "import DA.Assert"
              , "data B = B A"
              , "f : X"
              , "f = X"

              , "test = scenario do"
              , "  alice <- getParty \"Alice\""
              , "  let t = T alice"
              , "  signatoryT t === [alice]"
              , "  cid <- submit alice $ createT t"
              , "  submit alice $ archiveT cid"
              ]
          writeFileUTF8 (projb </> "daml.yaml") $ unlines
              [ "sdk-version: " <> sdkVersion
              , "name: projb"
              , "version: 0.0.1"
              , "source: src"
              , "dependencies: [daml-prim, daml-stdlib]"
              , "data-dependencies: [" <> show (proja </> "proja.dar") <> "]"
              ]
          callProcessSilent damlc
            [ "build"
            , "--enable-scenarios=yes" -- TODO: https://github.com/digital-asset/daml/issues/11316
            , "--project-root", projb
            , "--target", LF.renderVersion targetLfVer
            , "-o", projb </> "projb.dar" ]
          step "Validating DAR"
          validate $ projb </> "projb.dar"
          projbPkgIds <- darPackageIds (projb </> "projb.dar")
          -- daml-prim, daml-stdlib for targetLfVer, daml-prim, daml-stdlib for depLfVer if targetLfVer /= depLfVer, proja and projb
          length projbPkgIds @?= numStablePackagesForVersion targetLfVer
              + 2 + (if targetLfVer /= depLfVer then 2 else 0) + 1 + 1
          length (filter (`notElem` projaPkgIds) projbPkgIds) @?=
              ( numStablePackagesForVersion targetLfVer
              - numStablePackagesForVersion depLfVer ) + -- new stable packages
              1 + -- projb
              (if targetLfVer /= depLfVer then 2 else 0) -- different daml-stdlib/daml-prim
    | (depLfVer, targetLfVer) <- lfVersionTestPairs
    ] <>
    [ testCaseSteps ("Cross Daml-LF version with stdlib orphan instances: " <> LF.renderVersion depLfVer <> " -> " <> LF.renderVersion targetLfVer)  $ \step -> withTempDir $ \tmpDir -> do
          let proja = tmpDir </> "proja"
          let projb = tmpDir </> "projb"

          step "Build proja"
          createDirectoryIfMissing True (proja </> "src")
          writeFileUTF8 (proja </> "src" </> "A.daml") $ unlines
              [ "module A where"
              , "f : ()"
              , "f = ()"
              ]
          writeFileUTF8 (proja </> "daml.yaml") $ unlines
              [ "sdk-version: " <> sdkVersion
              , "name: proja"
              , "version: 0.0.1"
              , "source: src"
              , "dependencies: [daml-prim, daml-stdlib]"
              ]
          callProcessSilent (damlcForTarget tools depLfVer)
                ["build"
                , "--project-root", proja
                , "--target", LF.renderVersion depLfVer
                , "-o", proja </> "proja.dar"
                ]
          projaPkgIds <- darPackageIds (proja </> "proja.dar")
          -- daml-stdlib, daml-prim and proja
          length projaPkgIds @?= numStablePackagesForVersion depLfVer + 2 + 1

          step "Build projb"
          createDirectoryIfMissing True (projb </> "src")
          writeFileUTF8 (projb </> "src" </> "B.daml") $ unlines
              [ "module B where"
              , "import A qualified"
              , "f : ()"
              , "f = A.f"
              ]
          writeFileUTF8 (projb </> "daml.yaml") $ unlines
              [ "sdk-version: " <> sdkVersion
              , "name: projb"
              , "version: 0.0.1"
              , "source: src"
              , "dependencies: [daml-prim, daml-stdlib]"
              , "data-dependencies: [" <> show (proja </> "proja.dar") <> "]"
              ]
          callProcessSilent damlc
            [ "build"
            , "--project-root", projb
            , "--target", LF.renderVersion targetLfVer
            , "-o", projb </> "projb.dar" ]
          step "Validating DAR"
          validate $ projb </> "projb.dar"
    | (depLfVer, targetLfVer) <- lfVersionTestPairs
    ] <>
    [ testCaseSteps ("Cross Daml-LF version with custom orphan instance: " <> LF.renderVersion depLfVer <> " -> " <> LF.renderVersion targetLfVer)  $ \step -> withTempDir $ \tmpDir -> do
          let proja = tmpDir </> "proja"
          let projb = tmpDir </> "projb"
          let projc = tmpDir </> "projc"

          step "Build proja"
          createDirectoryIfMissing True (proja </> "src")
          writeFileUTF8 (proja </> "src" </> "AC.daml") $ unlines
              [ "module AC where"
              , "class AC a where"
              , "  ac : a -> a"
              ]
          writeFileUTF8 (proja </> "src" </> "AT.daml") $ unlines
              [ "module AT where"
              , "data AT"
              ]
          writeFileUTF8 (proja </> "src" </> "AI.daml") $ unlines
              [ "module AI where"
              , "import AC"
              , "import AT"
              , "instance AC AT where"
              , "  ac a = a"
              ]
          writeFileUTF8 (proja </> "daml.yaml") $ unlines
              [ "sdk-version: " <> sdkVersion
              , "name: proja"
              , "version: 0.0.1"
              , "source: src"
              , "dependencies: [daml-prim, daml-stdlib]"
              ]
          callProcessSilent (damlcForTarget tools depLfVer)
                ["build"
                , "--project-root", proja
                , "--target", LF.renderVersion depLfVer
                , "-o", proja </> "proja.dar"
                ]

          step "Build projb"
          createDirectoryIfMissing True (projb </> "src")
          writeFileUTF8 (projb </> "src" </> "B.daml") $ unlines
              [ "module B where"
              , "import AI ()"
              ]
          writeFileUTF8 (projb </> "daml.yaml") $ unlines
              [ "sdk-version: " <> sdkVersion
              , "name: projb"
              , "version: 0.0.1"
              , "source: src"
              , "dependencies: [daml-prim, daml-stdlib]"
              , "data-dependencies: [" <> show (proja </> "proja.dar") <> "]"
              ]
          callProcessSilent (damlcForTarget tools depLfVer)
            ["build"
            , "--project-root", projb
            , "--target", LF.renderVersion depLfVer
            , "-o", projb </> "projb.dar"
            ]

          step "Build projc"
          createDirectoryIfMissing True (projc </> "src")
          writeFileUTF8 (projc </> "src" </> "C.daml") $ unlines
              [ "module C where"
              , "import B ()"
              ]
          writeFileUTF8 (projc </> "daml.yaml") $ unlines
              [ "sdk-version: " <> sdkVersion
              , "name: projc"
              , "version: 0.0.1"
              , "source: src"
              , "dependencies: [daml-prim, daml-stdlib]"
              , "data-dependencies: [" <> show (projb </> "projb.dar") <> "]"
              ]
          callProcessSilent damlc
            [ "build"
            , "--project-root", projc
            , "--target", LF.renderVersion targetLfVer
            , "-o", projc </> "projc.dar" ]
          step "Validating DAR"
          validate $ projc </> "projc.dar"
    | (depLfVer, targetLfVer) <- lfVersionTestPairs
    ] <>
    [ testCaseSteps ("Cross Daml-LF version with double data-dependency from old SDK: " <> LF.renderVersion depLfVer <> " -> " <> LF.renderVersion targetLfVer) $
        -- Given a dar "Old" built with an older SDK, this tests that a project
        -- which depends on "Old" through different paths on its dependency graph
        -- will not end up with multiple copies of daml-prim and daml-stdlib
        -- from the older SDK, which would prevent it from compiling.
        \step -> withTempDir $ \tmpDir -> do
            let proja = tmpDir </> "proja"
            let projb = tmpDir </> "projb"

            step "Build proja"
            createDirectoryIfMissing True (proja </> "src")
            writeFileUTF8 (proja </> "src" </> "A.daml") $ unlines
                [ "module A where"
                , "import Old ()"
                , "template T"
                , "  with"
                , "    party : Party"
                , "  where"
                , "    signatory party"
                ]
            writeFileUTF8 (proja </> "daml.yaml") $ unlines
                [ "sdk-version: " <> sdkVersion
                , "name: proja"
                , "version: 0.0.1"
                , "source: src"
                , "dependencies: [daml-prim, daml-stdlib]"
                , "data-dependencies:"
                , " - " <> show oldProjDar
                ]
            callProcessSilent (damlcForTarget tools depLfVer)
                ["build"
                , "--project-root", proja
                , "--target", LF.renderVersion depLfVer
                , "-o", proja </> "proja.dar"
                ]

            step "Build projb"
            createDirectoryIfMissing True (projb </> "src")
            writeFileUTF8 (projb </> "src" </> "B.daml") $ unlines
                [ "module B where"
                , "import Old ()"
                , "import A ()"
                ]
            writeFileUTF8 (projb </> "daml.yaml") $ unlines
                [ "sdk-version: " <> sdkVersion
                , "name: projb"
                , "version: 0.0.1"
                , "source: src"
                , "dependencies: [daml-prim, daml-stdlib]"
                , "data-dependencies: "
                , " - " <> show oldProjDar
                , " - " <> show (proja </> "proja.dar")
                ]
            callProcessSilent damlc
                ["build"
                , "--project-root", projb
                , "--target", LF.renderVersion targetLfVer
                , "-o", projb </> "projb.dar"
                ]

            step "Validating DAR"
            validate $ projb </> "projb.dar"
    | (depLfVer, targetLfVer) <- lfVersionTestPairs
    ] <>
    [ testCaseSteps "Mixed dependencies and data-dependencies" $ \step -> withTempDir $ \tmpDir -> do
          step "Building 'lib'"
          createDirectoryIfMissing True (tmpDir </> "lib")
          writeFileUTF8 (tmpDir </> "lib" </> "daml.yaml") $ unlines
              [ "sdk-version: " <> sdkVersion
              , "version: 0.0.1"
              , "name: lib"
              , "source: ."
              , "dependencies: [daml-prim, daml-stdlib]"
              ]
          writeFileUTF8 (tmpDir </> "lib" </> "Lib.daml") $ unlines
              [ "module Lib where"
              , "inc : Int -> Int"
              , "inc = (+ 1)"
              ]
          callProcessSilent damlc
              [ "build"
              , "--project-root", tmpDir </> "lib"
              , "-o", tmpDir </> "lib" </> "lib.dar"]
          libPackageIds <- darPackageIds (tmpDir </> "lib" </> "lib.dar")

          step "Building 'a'"
          createDirectoryIfMissing True (tmpDir </> "a")
          writeFileUTF8 (tmpDir </> "a" </> "daml.yaml") $ unlines
              [ "sdk-version: " <> sdkVersion
              , "version: 0.0.1"
              , "name: a"
              , "source: ."
              , "dependencies:"
              , "  - daml-prim"
              , "  - daml-stdlib"
              , "  - " <> show (tmpDir </> "lib" </> "lib.dar")
              ]
          writeFileUTF8 (tmpDir </> "a" </> "A.daml") $ unlines
              [ "module A where"
              , "import Lib"
              , "two : Int"
              , "two = inc 1"
              ]
          callProcessSilent damlc
              [ "build"
              , "--project-root", tmpDir </> "a"
              , "-o", tmpDir </> "a" </> "a.dar"
              ]
          aPackageIds <- darPackageIds (tmpDir </> "a" </> "a.dar")
          length aPackageIds @?= length libPackageIds + 1

          step "Building 'b'"
          createDirectoryIfMissing True (tmpDir </> "b")
          writeFileUTF8 (tmpDir </> "b" </> "daml.yaml") $ unlines
              [ "sdk-version: " <> sdkVersion
              , "version: 0.0.1"
              , "name: b"
              , "source: ."
              , "dependencies:"
              , "  - daml-prim"
              , "  - daml-stdlib"
              , "  - " <> show (tmpDir </> "lib" </> "lib.dar")
              , "data-dependencies: [" <> show (tmpDir </> "a" </> "a.dar") <> "]"
              ]
          writeFileUTF8 (tmpDir </> "b" </> "B.daml") $ unlines
              [ "module B where"
              , "import Lib"
              , "import A"
              , "three : Int"
              , "three = inc two"
              ]
          callProcessSilent damlc
              ["build"
              , "--project-root", tmpDir </> "b"
              , "-o", tmpDir </> "b" </> "b.dar"
              ]
          projbPackageIds <- darPackageIds (tmpDir </> "b" </> "b.dar")
          length projbPackageIds @?= length libPackageIds + 2

          step "Validating DAR"
          validate $ tmpDir </> "b" </> "b.dar"

    , simpleImportTest "Tuples"
              [ "module Lib where"
              , "data X = X (Text, Int)"
              -- ^ Check that tuples are mapped back to Daml tuples.
              ]
              [ "module Main where"
              , "import Lib"
              , "f : X -> Text"
              , "f (X (a, b)) = a <> show b"
              ]

    , simpleImportTest "Type synonyms over data-dependencies"
              [ "module Lib where"
              , "type MyInt' = Int"
              , "type MyArrow a b = a -> b"
              , "type MyUnit = ()"
              , "type MyOptional = Optional"
              , "type MyFunctor t = Functor t"
              , "type MyEither = Either"
              , "type MyPartiallyAppliedEither = Either Text"
              , "type MyShow = Show"
              , "type MyScale = 5"
              , "type MyConstScale a = 5"
              , "type MyConstUnit a = ()"
              , "class MyMultiParamClass a b where myMultiParamMethod : (a, b)"
              , "type MyMultiParamClassSynonym = MyMultiParamClass"
              , "type MyAppliedMultiParamClassSynonym = MyMultiParamClass Int"
              , "type MyOtherAppliedMultiParamClassSynonym a = MyMultiParamClass a Int"
              ]
              [ "module Main where"
              , "import DA.Numeric (pi)"
              , "import Lib"
              , "x : MyInt'"
              , "x = 10"
              , "f : MyArrow Int Int"
              , "f a = a + 1"
              , "type MyUnit = Int"
              , "g : MyUnit -> MyUnit"
                -- ^ this tests that MyUnit wasn't exported from Foo
              , "g a = a"
              , "h : MyOptional Int -> MyOptional Int"
              , "h a = a"
              , "myFmap : MyFunctor t => (a -> b) -> t a -> t b"
              , "myFmap = fmap"
              , "myLeft : MyEither Text Bool"
              , "myLeft = Left \"uh oh\""
              , "myOtherLeft : MyPartiallyAppliedEither Bool"
              , "myOtherLeft = myLeft"
              , "myShow : MyShow a => a -> Text"
              , "myShow = show"
              , "myPi : Numeric MyScale"
              , "myPi = pi"
              , "myOtherPi : Numeric (MyConstScale Int)"
              , "myOtherPi = myPi"
              , "type MyConstUnit a = ()"
                -- ^ this tests that MyConstUnit wasn't exported from Foo
              , "myConstUnit : MyConstUnit Int"
              , "myConstUnit = ()"
              , "myMultiParamMethodSynonym : MyMultiParamClassSynonym a b => (a, b)"
              , "myMultiParamMethodSynonym = myMultiParamMethod"
              , "myAppliedMultiParamMethodSynonym : MyAppliedMultiParamClassSynonym b => (Int, b)"
              , "myAppliedMultiParamMethodSynonym = myMultiParamMethod"
              , "myOtherAppliedMultiParamMethodSynonym : MyOtherAppliedMultiParamClassSynonym a => (a, Int)"
              , "myOtherAppliedMultiParamMethodSynonym = myMultiParamMethod"
              ]

    , simpleImportTest "RankNTypes"
              [ "{-# LANGUAGE AllowAmbiguousTypes #-}"
              , "module Lib where"
              , "type Lens s t a b = forall f. Functor f => (a -> f b) -> s -> f t"
              , "lensIdentity : Lens s t a b -> Lens s t a b"
              , "lensIdentity = identity"
              , "class HasInt f where"
              , "  getInt : Int"
              , "f : forall a. HasInt a => Int"
              , "f = getInt @a"
              ]
              [ "module Main where"
              , "import Lib"
              , "x : Lens s t a b -> Lens s t a b"
                -- ^ This also tests Rank N type synonyms!
              , "x = lensIdentity"
              ]

    -- regression for https://github.com/digital-asset/daml/issues/8411
    , simpleImportTest "constraints in general position"
        [ "module Lib where"
        , "grantShowInt1 : (forall t. Show t => t -> Text) -> Text"
        , "grantShowInt1 f = f 10"
        , "grantShowInt2 : (Show Int => Int -> Text) -> Text"
        , "grantShowInt2 f = f 10"
        , "class Action1 m where"
        , "    action1 : forall e t. Action e => (Action (m e) => m e t) -> m e t"
        ]
        [ "module Main where"
        , "import Lib"
        , "use1 = grantShowInt1 show"
        , "use2 = grantShowInt2 show"
        , "newtype M a b = M { unM : a b }"
        , "    deriving (Functor, Applicative, Action)"
        , "instance Action1 M where"
        , "    action1 m = m"
        , "pure1 : (Action1 m, Action e) => t -> m e t"
        , "pure1 x = action1 (pure x)"
        ]

    , testCaseSteps "Colliding package names" $ \step -> withTempDir $ \tmpDir -> do
          forM_ ["1", "2"] $ \version -> do
              step ("Building 'lib" <> version <> "'")
              let projDir = tmpDir </> "lib-" <> version
              createDirectoryIfMissing True projDir
              writeFileUTF8 (projDir </> "daml.yaml") $ unlines
                  [ "sdk-version: " <> sdkVersion
                  , "version: " <> show version
                  , "name: lib"
                  , "source: ."
                  , "dependencies: [daml-prim, daml-stdlib]"
                  ]
              writeFileUTF8 (projDir </> "Lib.daml") $ unlines
                  [ "module Lib where"
                  , "data X" <> version <> " = X"
                  ]

              callProcessSilent damlc
                  [ "build"
                  , "--project-root", projDir
                  , "-o", projDir </> "lib.dar"
                  ]

          step "Building a"
          let projDir = tmpDir </> "a"
          createDirectoryIfMissing True projDir
          writeFileUTF8 (projDir </> "daml.yaml") $ unlines
               [ "sdk-version: " <> sdkVersion
               , "version: 0.0.0"
               , "name: a"
               , "source: ."
               , "dependencies: [daml-prim, daml-stdlib]"
               , "data-dependencies:"
               , "- " <> show (tmpDir </> "lib-1" </> "lib.dar")
               ]
          writeFileUTF8 (projDir </> "A.daml") $ unlines
              [ "module A where"
              , "import Lib"
              , "data A = A X1"
              ]
          callProcessSilent damlc
              [ "build"
              , "--project-root", projDir
              , "-o", projDir </> "a.dar"
              ]

          step "Building b"
          let projDir = tmpDir </> "b"
          createDirectoryIfMissing True projDir
          writeFileUTF8 (projDir </> "daml.yaml") $ unlines
               [ "sdk-version: " <> sdkVersion
               , "version: 0.0.0"
               , "name: b"
               , "source: ."
               , "dependencies: [daml-prim, daml-stdlib]"
               , "data-dependencies:"
               , "- " <> show (tmpDir </> "lib-2" </> "lib.dar")
               , "- " <> show (tmpDir </> "a" </> "a.dar")
               ]
          writeFileUTF8 (projDir </> "B.daml") $ unlines
              [ "module B where"
              , "import Lib"
              , "import A"
              , "data B1 = B1 A"
              , "data B2 = B2 X2"
              ]
          callProcessSilent damlc
              [ "build"
              , "--project-root", projDir
              , "-o", projDir </> "b.dar"
              ]

          -- At this point b has references to both lib-1 and lib-2 in its transitive dependency closure.
          -- Now try building `c` which references `b` as a `data-dependency` and see if it
          -- manages to produce an import of `Lib` for the dummy interface of `B` that resolves correctly.
          step "Building c"
          let projDir = tmpDir </> "c"
          createDirectoryIfMissing True projDir
          writeFileUTF8 (projDir </> "daml.yaml") $ unlines
               [ "sdk-version: " <> sdkVersion
               , "version: 0.0.0"
               , "name: c"
               , "source: ."
               , "dependencies: [daml-prim, daml-stdlib]"
               , "data-dependencies:"
               , "- " <> show (tmpDir </> "b" </> "b.dar")
               , "- " <> show (tmpDir </> "lib-2" </> "lib.dar")
               ]
          writeFileUTF8 (projDir </> "C.daml") $ unlines
              [ "module C where"
              , "import B"
              , "import Lib"
              , "f : B2 -> X2"
              , "f (B2 x) = x"
              ]
          callProcessSilent damlc
              [ "build"
              , "--project-root", projDir
              , "-o", projDir </> "c.dar"
              ]
    ] <>
    [ testCase ("Dalf imports (withArchiveChoice=" <> show withArchiveChoice <> ")") $ withTempDir $ \projDir -> do
        let genSimpleDalfExe
              | isWindows = "generate-simple-dalf.exe"
              | otherwise = "generate-simple-dalf"
        genSimpleDalf <-
            locateRunfiles
            (mainWorkspace </> "compiler" </> "damlc" </> "tests" </> genSimpleDalfExe)
        writeFileUTF8 (projDir </> "daml.yaml") $ unlines
          [ "sdk-version: " <> sdkVersion
          , "name: proj"
          , "version: 0.1.0"
          , "source: ."
          , "dependencies: [daml-prim, daml-stdlib]"
          , "data-dependencies: [simple-dalf-1.0.0.dalf]"
          , "build-options: [--package=simple-dalf-1.0.0]"
          ]
        writeFileUTF8 (projDir </> "A.daml") $ unlines
            [ "module A where"
            , "import DA.Assert"
            , "import qualified \"simple-dalf\" Module"
            , "swapParties : Module.Template -> Module.Template"
            , "swapParties (Module.Template a b) = Module.Template b a"
            , "getThis : Module.Template -> Party"
            , "getThis (Module.Template this _) = this"
            , "getArg : Module.Template -> Party"
            , "getArg (Module.Template _ arg) = arg"
            , "test_methods = scenario do"
            , "  alice <- getParty \"Alice\""
            , "  bob <- getParty \"Bob\""
            , "  let t = Module.Template alice bob"
            , "  getThis (Module.Template alice bob) === alice"
            , "  getArg (Module.Template alice bob) === bob"
            , "  getThis (swapParties (Module.Template alice bob)) === bob"
            , "  getArg (swapParties (Module.Template alice bob)) === alice"
            -- Disabled until we support reusing old type classes
            -- , "  let t = newTemplate alice bob"
            -- , "  assert $ signatory t == [alice, bob]"
            -- , "  assert $ observer t == []"
            -- , "  assert $ ensure t"
            -- , "  assert $ agreement t == \"\""
            -- , "  coid <- submit alice $ createTemplate alice alice"
            -- , "  " <> (if withArchiveChoice then "submit" else "submitMustFail") <> " alice $ archive coid"
            -- , "  coid1 <- submit bob $ createTemplate bob bob"
            -- , "  t1 <- submit bob $ fetch coid1"
            -- , "  assert $ signatory t1 == [bob, bob]"
            -- , "  let anyTemplate = toAnyTemplate t1"
            -- , "  let (Some t2 : Optional Module.Template) = fromAnyTemplate anyTemplate"
            -- , "  submit bob $ exercise coid1 Module.Choice2 with choiceArg = ()"
            -- , "  pure ()"
            ]
        callProcessSilent genSimpleDalf $
            ["--with-archive-choice" | withArchiveChoice ] <>
            [projDir </> "simple-dalf-1.0.0.dalf"]
        callProcess damlc
            [ "build"
            , "--enable-scenarios=yes" -- TODO: https://github.com/digital-asset/daml/issues/11316
            , "--project-root", projDir
            , "--target=1.dev"
            , "--generated-src" ]
        let dar = projDir </> ".daml/dist/proj-0.1.0.dar"
        assertFileExists dar
        callProcessSilent damlc
            [ "test"
            , "--enable-scenarios=yes" -- TODO: https://github.com/digital-asset/daml/issues/11316
            , "--target=1.dev"
            , "--project-root"
            , projDir
            , "--generated-src" ]
    | withArchiveChoice <- [False, True]
    ] <>
    [ testCaseSteps ("Typeclasses and instances from Daml-LF " <> LF.renderVersion depLfVer <> " to " <> LF.renderVersion targetLfVer) $ \step -> withTempDir $ \tmpDir -> do
          let proja = tmpDir </> "proja"
          let projb = tmpDir </> "projb"

          step "Build proja"
          createDirectoryIfMissing True (proja </> "src")
          writeFileUTF8 (proja </> "src" </> "A.daml") $ unlines
              [ "{-# LANGUAGE KindSignatures #-}"
              , "{-# LANGUAGE UndecidableInstances #-}"
              , "{-# LANGUAGE DataKinds #-}"
              , "module A where"
              , "import DA.Record"
              , "import DA.Validation"
              -- test typeclass export
              , "class Foo t where"
              , "  foo : Int -> t"
              , "class Foo t => Bar t where"
              , "  bar : Int -> t"
              -- test constrainted function export
              , "usingFoo : Foo t => t"
              , "usingFoo = foo 0"
              -- test instance export
              , "instance Foo Int where"
              , "  foo x = x"
              , "instance Bar Int where"
              , "  bar x = x"
              -- test instance export where typeclass is from stdlib
              , "data Q = Q1 | Q2 deriving (Eq, Ord, Show)"
              -- test constrained function export where typeclass is from stdlib
              , "usingEq : Eq t => t -> t -> Bool"
              , "usingEq = (==)"
              -- test exporting of HasField instances
              , "data RR = RR { rrfoo : Int }"
              -- test exporting of template typeclass instances
              , "template P"
              , "  with"
              , "    p : Party"
              , "  where"
              , "    signatory p"
              , "data AnyWrapper = AnyWrapper { getAnyWrapper : AnyTemplate }"

              , "data FunT a b = FunT (a -> b)"

              , "instance (Foo a, Foo b) => Foo (a,b) where"
              , "  foo x = (foo x, foo x)"

              , "class ActionTrans t where"
              , "  lift : Action f => f a -> t f a"
              , "newtype OptionalT f a = OptionalT { runOptionalT : f (Maybe a) }"
              , "instance ActionTrans OptionalT where"
              , "  lift f = OptionalT (fmap Just f)"

              -- function that requires a HasField instance
              -- (i.e. this tests type-level strings across data-dependencies)
              , "usesHasField : (HasField \"a_field\" a b) => a -> b"
              , "usesHasField = getField @\"a_field\""
              , "usesHasFieldEmpty : (HasField \"\" a b) => a -> b"
              , "usesHasFieldEmpty = getField @\"\""

              -- [Issue #7256] Tests that orphan superclass instances are dependended on correctly.
              -- E.g. Applicative Validation is an orphan instance implemented in DA.Validation.
              , "instance Action (Validation e) where"
              , "  v >>= f = case v of"
              , "    Errors e-> Errors e"
              , "    Success a -> f a"

              -- Regression for issue https://github.com/digital-asset/daml/issues/9663
              -- Constraint tuple functions
              , "constraintTupleFn : (Template t, Show t) => t -> ()"
              , "constraintTupleFn = const ()"
              , "type BigConstraint a b c = (Show a, Show b, Show c, Additive c)"
              , "bigConstraintFn : BigConstraint a b c => a -> b -> c -> c -> Text"
              , "bigConstraintFn x y z w = show x <> show y <> show (z + w)"
              -- nested constraint tuples
              , "type NestedConstraintTuple a b c d = (BigConstraint a b c, Show d)"
              , "nestedConstraintTupleFn : NestedConstraintTuple a b c d => a -> b -> c -> d -> Text"
              , "nestedConstraintTupleFn x y z w = show x <> show y <> show z <> show w"
              ]
          writeFileUTF8 (proja </> "daml.yaml") $ unlines
              [ "sdk-version: " <> sdkVersion
              , "name: proja"
              , "version: 0.0.1"
              , "source: src"
              , "dependencies: [daml-prim, daml-stdlib]"
              ]
          callProcessSilent (damlcForTarget tools depLfVer)
              [ "build"
              , "--project-root", proja
              , "--target", LF.renderVersion depLfVer
              , "-o", proja </> "proja.dar"
              ]

          step "Build projb"
          createDirectoryIfMissing True (projb </> "src")
          writeFileUTF8 (projb </> "src" </> "B.daml") $ unlines
              [ "module B where"
              , "import A"
              , "import DA.Assert"
              , "import DA.Record"
              , ""
              , "data T = T Int"
              -- test instances for imported typeclass
              , "instance Foo T where"
              , "    foo = T"
              , "instance Bar T where"
              , "    bar = T"
              -- test constrained function import
              , "usingFooIndirectly : T"
              , "usingFooIndirectly = usingFoo"
              -- test imported function constrained by newer Eq class
              , "testConstrainedFn = scenario do"
              , "  usingEq 10 10 === True"
              -- test instance imports
              , "testInstanceImport = scenario do"
              , "  foo 10 === 10" -- Foo Int
              , "  bar 20 === 20" -- Bar Int
              , "  foo 10 === (10, 10)" -- Foo (a, b)
              , "  Q1 === Q1" -- (Eq Q, Show Q)
              , "  (Q1 <= Q2) === True" -- Ord Q
              -- test importing of HasField instances
              , "testHasFieldInstanceImport = scenario do"
              , "  let x = RR 100"
              , "  getField @\"rrfoo\" x === 100"
              -- test importing of template typeclass instance
              , "test = scenario do"
              , "  alice <- getParty \"Alice\""
              , "  let t = P alice"
              , "  signatory t === [alice]"
              , "  cid <- submit alice $ create t"
              , "  submit alice $ archive cid"
              -- references to DA.Internal.Any
              , "testAny = scenario do"
              , "  p <- getParty \"p\""
              , "  let t = P p"
              , "  fromAnyTemplate (AnyWrapper $ toAnyTemplate t).getAnyWrapper === Some t"
              -- reference to T
              , "foobar : FunT Int Text"
              , "foobar = FunT show"
              -- ActionTrans
              , "trans = scenario do"
              , "  runOptionalT (lift [0]) === [Just 0]"
              -- type-level string test
              , "usesHasFieldIndirectly : HasField \"a_field\" a b => a -> b"
              , "usesHasFieldIndirectly = usesHasField"
              , "usesHasFieldEmptyIndirectly : HasField \"\" a b => a -> b"
              , "usesHasFieldEmptyIndirectly = usesHasFieldEmpty"
              -- use constraint tuple fn
              , "useConstraintTupleFn : (Template t, Show t) => t -> ()"
              , "useConstraintTupleFn x = constraintTupleFn x"
              , "useBigConstraintFn : Text"
              , "useBigConstraintFn = bigConstraintFn True \"Hello\" 10 20"
              -- regression test for issue: https://github.com/digital-asset/daml/issues/9689
              -- Using constraint synonym defined in data-dependency
              , "newBigConstraintFn : BigConstraint a b c => a -> b -> c -> Text"
              , "newBigConstraintFn x y z = show x <> show y <> show z"
              , "useNewBigConstraintFn : Text"
              , "useNewBigConstraintFn = newBigConstraintFn 10 \"Hello\" 20"
              -- Using nested constraint tuple
              , "useNestedConstraintTupleFn : Text"
              , "useNestedConstraintTupleFn = nestedConstraintTupleFn 10 20 30 40"
              , "nestedConstraintTupleFn2 : NestedConstraintTuple a b c d => a -> b -> c -> d -> Text"
              , "nestedConstraintTupleFn2 x y z w = show x <> show y <> show z <> show w"
              ]
          writeFileUTF8 (projb </> "daml.yaml") $ unlines
              [ "sdk-version: " <> sdkVersion
              , "name: projb"
              , "version: 0.0.1"
              , "source: src"
              , "dependencies: [daml-prim, daml-stdlib]"
              , "data-dependencies: [" <> show (proja </> "proja.dar") <> "]"
              ]
          callProcessSilent damlc
              [ "build"
              , "--enable-scenarios=yes" -- TODO: https://github.com/digital-asset/daml/issues/11316
              , "--project-root", projb
              , "--target=" <> LF.renderVersion targetLfVer
              , "-o", projb </> "projb.dar" ]
          validate $ projb </> "projb.dar"

    | (depLfVer, targetLfVer) <- lfVersionTestPairs
    ] <>
    [ testCase "Cross-SDK typeclasses" $ withTempDir $ \tmpDir -> do
          writeFileUTF8 (tmpDir </> "daml.yaml") $ unlines
              [ "sdk-version: " <> sdkVersion
              , "name: upgrade"
              , "source: ."
              , "version: 0.1.0"
              , "dependencies: [daml-prim, daml-stdlib]"
              , "data-dependencies:"
              , "  - " <> show oldProjDar
              , "build-options:"
              , " - --target=1.dev"
              , " - --package=daml-prim"
              , " - --package=" <> unitIdString damlStdlib
              , " - --package=old-proj-0.0.1"
              ]
          writeFileUTF8 (tmpDir </> "Upgrade.daml") $ unlines
              [ "module Upgrade where"
              , "import qualified Old"

              , "template T"
              , "  with"
              , "    p : Party"
              , "  where signatory p"

              , "template Upgrade"
              , "  with"
              , "    p : Party"
              , "  where"
              , "    signatory p"
              , "    nonconsuming choice DoUpgrade : ContractId T"
              , "      with"
              , "        cid : ContractId Old.T"
              , "      controller p"
              , "      do Old.T{..} <- fetch cid"
              , "         archive cid"
              , "         create T{..}"
              ]
          callProcessSilent damlc ["build", "--project-root", tmpDir]
    , testCaseSteps "Duplicate instance reexports" $ \step -> withTempDir $ \tmpDir -> do
          -- This test checks that we handle the case where a data-dependency has (orphan) instances
          -- Functor, Applicative for a type Proxy while a dependency only has instance Functor.
          -- In this case we need to import the Functor instance or the Applicative instance will have a type error.
          step "building type project"
          createDirectoryIfMissing True (tmpDir </> "type")
          writeFileUTF8 (tmpDir </> "type" </> "daml.yaml") $ unlines
              [ "sdk-version: " <> sdkVersion
              , "name: type"
              , "source: ."
              , "version: 0.1.0"
              , "dependencies: [daml-prim, daml-stdlib]"
              , "build-options: [--target=1.dev]"
              ]
          writeFileUTF8 (tmpDir </> "type" </> "Proxy.daml") $ unlines
              [ "module Proxy where"
              , "data Proxy a = Proxy {}"
              ]
          callProcessSilent damlc
              [ "build"
              , "--project-root", tmpDir </> "type"
              , "-o",  tmpDir </> "type" </> "type.dar"]

          step "building dependency project"
          createDirectoryIfMissing True (tmpDir </> "dependency")
          writeFileUTF8 (tmpDir </> "dependency" </> "daml.yaml") $ unlines
              [ "sdk-version: " <> sdkVersion
              , "name: dependency"
              , "source: ."
              , "version: 0.1.0"
              , "dependencies: [daml-prim, daml-stdlib]"
              , "data-dependencies: [" <> show (tmpDir </> "type" </> "type.dar") <> "]"
              , "build-options: [ \"--target=1.dev\" ]"
              ]
          writeFileUTF8 (tmpDir </> "dependency" </> "Dependency.daml") $ unlines
             [ "module Dependency where"
             , "import Proxy"
             , "instance Functor Proxy where"
             , "  fmap _ Proxy = Proxy"
             ]
          callProcessSilent damlc
              [ "build"
              , "--project-root", tmpDir </> "dependency"
              , "-o", tmpDir </> "dependency" </> "dependency.dar"]

          step "building data-dependency project"
          createDirectoryIfMissing True (tmpDir </> "data-dependency")
          writeFileUTF8 (tmpDir </> "data-dependency" </> "daml.yaml") $ unlines
              [ "sdk-version: " <> sdkVersion
              , "name: data-dependency"
              , "source: ."
              , "version: 0.1.0"
              , "dependencies: [daml-prim, daml-stdlib]"
              , "data-dependencies: [" <> show (tmpDir </> "type" </> "type.dar") <> "]"
              , "build-options: [ \"--target=1.dev\" ]"
              ]
          writeFileUTF8 (tmpDir </> "data-dependency" </> "DataDependency.daml") $ unlines
             [ "module DataDependency where"
             , "import Proxy"
             , "instance Functor Proxy where"
             , "  fmap _ Proxy = Proxy"
             , "instance Applicative Proxy where"
             , "  pure _ = Proxy"
             , "  Proxy <*> Proxy = Proxy"
             ]
          callProcessSilent damlc
              [ "build"
              , "--project-root", tmpDir </> "data-dependency"
              , "-o", tmpDir </> "data-dependency" </> "data-dependency.dar"]

          step "building top-level project"
          createDirectoryIfMissing True (tmpDir </> "top")
          writeFileUTF8 (tmpDir </> "top" </> "daml.yaml") $ unlines
              [ "sdk-version: " <> sdkVersion
              , "name: top"
              , "source: ."
              , "version: 0.1.0"
              , "dependencies: [daml-prim, daml-stdlib, " <> show (tmpDir </> "dependency" </> "dependency.dar") <> ", " <> show (tmpDir </> "type/type.dar") <> "]"
              , "data-dependencies: [" <> show (tmpDir </> "data-dependency" </> "data-dependency.dar") <> "]"
              , "build-options: [--target=1.dev]"
              ]
          writeFileUTF8 (tmpDir </> "top" </> "Top.daml") $ unlines
              [ "module Top where"
              , "import DataDependency"
              , "import Proxy"
              -- Test that we can use the Applicaive instance of Proxy from the data-dependency
              , "f = pure () : Proxy ()"
              ]
          callProcessSilent damlc
              [ "build"
              , "--project-root", tmpDir </> "top"]

    , simpleImportTest "Generic variants with record constructors"
        -- This test checks that data definitions of the form
        --    data A t = B t | C { x: t, y: t }
        -- are handled correctly. This is a regression test for issue #4707.
            [ "module Lib where"
            , "data A t = B t | C { x: t, y: t }"
            ]
            [ "module Main where"
            , "import Lib"
            , "mkA : A Int"
            , "mkA = C with"
            , "  x = 10"
            , "  y = 20"
            ]

    , simpleImportTest "Empty variant constructors"
        -- This test checks that variant constructors without argument
        -- are preserved. This is a regression test for issue #7207.
            [ "module Lib where"
            , "data A = B | C Int"
            , "data D = D ()" -- single-constructor case uses explicit unit
            ]
            [ "module Main where"
            , "import Lib"
            , "mkA : A"
            , "mkA = B"
            , "matchA : A -> Int"
            , "matchA a ="
            , "  case a of"
            , "    B -> 0"
            , "    C n -> n"
            , "mkD : D"
            , "mkD = D ()"
            , "matchD : D -> ()"
            , "matchD d ="
            , "  case d of"
            , "    D () -> ()"
            ]

    , simpleImportTest "HasField across data-dependencies"
        -- This test checks that HasField instances are correctly imported via
        -- data-dependencies. This is a regression test for issue #7284.
            [ "module Lib where"
            , "data T x y"
            , "   = A with a: x"
            , "   | B with b: y"
            ]
            [ "module Main where"
            , "import Lib"
            , "getA : T x y -> x"
            , "getA t = t.a"
            ]

    , simpleImportTest "Dictionary function names match despite conflicts"
        -- This test checks that dictionary function names are recreated correctly.
        -- This is a regression test for issue #7362.
            [ "module Lib where"
            , "data T t = T {}"
            , "instance Show (T Int) where show T = \"T\""
            , "instance Show (T Bool) where show T = \"T\""
            , "instance Show (T Text) where show T = \"T\""
            , "instance Show (T (Optional Int)) where show T = \"T\""
            , "instance Show (T (Optional Bool)) where show T = \"T\""
            , "instance Show (T (Optional Text)) where show T = \"T\""
            , "instance Show (T [Int]) where show T = \"T\""
            , "instance Show (T [Bool]) where show T = \"T\""
            , "instance Show (T [Text]) where show T = \"T\""
            , "instance Show (T [Optional Int]) where show T = \"T\""
            , "instance Show (T [Optional Bool]) where show T = \"T\""
            , "instance Show (T [Optional Text]) where show T = \"T\""
            ] -- ^ These instances all have conflicting dictionary function names,
              -- so GHC numbers them 1, 2, 3, 4, ... after the first.
              --
              -- NB: It's important to have more than 10 instances here, so we can test
              -- that we handle non-lexicographically ordered conflicts correctly
              -- (i.e. instances numbered 10, 11, etc will not be in the correct order
              -- just by sorting definitions by value name, lexicographically).
            [ "module Main where"
            , "import Lib"
            , "f1 = show @(T Int)"
            , "f2 = show @(T Bool)"
            , "f3 = show @(T Text)"
            , "f4 = show @(T (Optional Int))"
            , "f5 = show @(T (Optional Bool))"
            , "f6 = show @(T (Optional Text))"
            , "f7 = show @(T [Int])"
            , "f8 = show @(T [Bool])"
            , "f9 = show @(T [Text])"
            , "f10 = show @(T [Optional Int])"
            , "f11 = show @(T [Optional Bool])"
            , "f12 = show @(T [Optional Text])"
            ]

    , simpleImportTest "Simple default methods"
        -- This test checks that simple default methods work in data-dependencies.
            [ "module Lib where"
            , "class Foo t where"
            , "    foo : t -> Int"
            , "    foo _ = 42"
            ]
            [ "module Main where"
            , "import Lib"
            , "data M = M"
            , "instance Foo M"
            , "useFoo : Int"
            , "useFoo = foo M"
            ]

    , simpleImportTest "Using default method signatures"
        -- This test checks that simple default methods work in data-dependencies.
            [ "module Lib where"
            , "class Foo t where"
            , "    foo : t -> Text"
            , "    default foo : Show t => t -> Text"
            , "    foo x = show x"

            , "    bar : Action m => t -> m Text"
            , "    default bar : (Show t, Action m) => t -> m Text"
            , "    bar x = pure (show x)"

            , "    baz : (Action m, Show y) => t -> y -> m Text"
            , "    default baz : (Show t, Action m, Show y) => t -> y -> m Text"
            , "    baz x y = pure (show x <> show y)"
            ]
            [ "module Main where"
            , "import Lib"
            , "data M = M deriving Show"
            , "instance Foo M"

            , "useFoo : Text"
            , "useFoo = foo M"

            , "useBar : Update Text"
            , "useBar = bar M"

            , "useBaz : (Action m, Show t) => t -> m Text"
            , "useBaz = baz M"
            ]

    , simpleImportTest "Non-default instance for constrained default methods"
        -- This test checks that non-default instances of a class with
        -- a constrained default method have the correct stubs.
        -- [ regression test for https://github.com/digital-asset/daml/issues/8802 ]
            [ "module Lib where"
            , "class Foo t where"
            , "    foo : t -> Text"
            , "    default foo : Show t => t -> Text"
            , "    foo = show"
            , "data Bar = Bar" -- no Show instance
            , "instance Foo Bar where"
            , "    foo Bar = \"bar\""
            ]
            [ "module Main where"
            , "import Lib"
            , "baz : Text"
            , "baz = foo Bar"
            ]

    , simpleImportTest "Data constructor operators"
        -- This test checks that we reconstruct data constructors operators properly.
        [ "module Lib where"
        , "data Expr = Lit Int | (:+:) {left: Expr, right: Expr}"
        ]
        [ "module Main where"
        , "import Lib"
        , "two = Lit 1 :+: Lit 1"
        ]

    , simpleImportTest "Using TypeOperators extension"
        -- This test checks that we reconstruct type operators properly.
        [ "{-# LANGUAGE TypeOperators #-}"
        , "module Lib (type (:+:) (..), type (+)) where"
        , "data a :+: b = (:+:){left: a, right:  b}"
        , "type a + b = a :+: b"
        ]
        [ "{-# LANGUAGE TypeOperators #-}"
        , "module Main where"
        , "import Lib (type (:+:) (..), type (+))"
        , "colonPlus: Bool :+: Int"
        , "colonPlus = True :+: 1"
        , "onlyPlus: Int + Bool"
        , "onlyPlus = 2 :+: False"
        ]

    , simpleImportTest "Using PartialTypeSignatures extension"
        -- This test checks that partial type signatures work in data-dependencies.
        [ "{-# LANGUAGE PartialTypeSignatures #-}"
        , "module Lib where"
        , "f: _ -> _"
        , "f xs = length xs + 1"
        ]
        [ "module Main where"
        , "import Lib"
        , "g: [a] -> Int"
        , "g = f"
        ]

    , simpleImportTest "Using AllowAmbiguousTypes extension"
        -- This test checks that ambiguous types work in data-dependencies.
        [ "{-# LANGUAGE AllowAmbiguousTypes #-}"
        , "module Lib where"
        , "f: forall a. Show a => Int"
        , "f = 1"
        ]
        [ "module Main where"
        , "import Lib"
        , "g: Int"
        , "g = f @Text"
        ]

    , simpleImportTest "Using InstanceSigs extension"
        -- This test checks that instance signatures work in data-dependencies.
        [ "{-# LANGUAGE InstanceSigs #-}"
        , "module Lib where"
        , "class C a where"
        , "  m: a -> a"
        , "instance C Int where"
        , "  m: Int -> Int"
        , "  m x = x"
        ]
        [ "module Main where"
        , "import Lib"
        , "f: Int -> Int"
        , "f = m"
        ]

    , simpleImportTest "Using UndecidableInstances extension"
        -- This test checks that undecidable instance work in data-dependencies.
        [ "{-# LANGUAGE UndecidableInstances #-}"
        , "module Lib where"
        , "class PartialEqual a b where"
        , "  peq: a -> b -> Bool"
        , "class Equal a where"
        , "  eq: a -> a -> Bool"
        , "instance PartialEqual a a => Equal a where"
        , "  eq x y = peq x y"
        ]
        [ "module Main where"
        , "import Lib"
        , "data X = X {f: Int}"
        , "instance PartialEqual X X where"
        , "  peq x y = x.f == y.f"
        , "eqD: X -> X -> Bool"
        , "eqD x y = eq x y"
        ]

    , simpleImportTest "Using functional dependencies"
        -- This test checks that functional dependencies are imported via data-dependencies.
        [ "module Lib where"
        , "class MyClass a b | a -> b where"
        , "   foo : a -> b"
        , "instance MyClass Int Int where foo x = x"
        , "instance MyClass Text Text where foo x = x"
        ]
        [ "module Main where"
        , "import Lib"
        , "useFooInt : Int -> Text"
        , "useFooInt x = show (foo x)"
        , "useFooText : Text -> Text"
        , "useFooText x = show (foo x)"
        -- If functional dependencies were not imported, we'd get a ton of
        -- "ambiguous type variable" errors from GHC, since type inference
        -- cannot determine the output type for 'foo'.
        ]

    , simpleImportTest "Using overlapping instances"
        [ "module Lib where"
        , "class MyShow t where myShow : t -> Text"
        , "instance MyShow t where myShow _ = \"_\""
        , "instance {-# OVERLAPPING #-} Show t => MyShow [t] where myShow = show"
        ]
        [ "module Main where"
        , "import Lib"
        , "useMyShow : [Int] -> Text"
        , "useMyShow = myShow"
          -- Without the OVERLAPPING pragma carrying over data-dependencies, this usage
          -- of myShow fails.
        ]

    , simpleImportTest "MINIMAL pragma"
        [ "module Lib where"
        , "class Dummy t where"
        , "    {-# MINIMAL #-}"
        , "    dummy : t -> Int"
        , "class MyClass t where"
        , "    {-# MINIMAL foo | (bar, baz) #-}"
        , "    foo : t -> Int"
        , "    bar : t -> Int"
        , "    baz : t -> Int"
        ]
        [ "module Main where"
        , "import Lib"
        , "data A = A"
        , "data B = B"
        , "instance Dummy A where"
        , "instance MyClass A where"
        , "    foo _ = 10"
        , "instance MyClass B where"
        , "    bar _ = 20"
        , "    baz _ = 30"
        ]

    , testCaseSteps "Implicit parameters" $ \step -> withTempDir $ \tmpDir -> do
        step "building project with implicit parameters"
        createDirectoryIfMissing True (tmpDir </> "dep")
        writeFileUTF8 (tmpDir </> "dep" </> "daml.yaml") $ unlines
            [ "sdk-version: " <> sdkVersion
            , "name: dep"
            , "source: ."
            , "version: 0.1.0"
            , "dependencies: [daml-prim, daml-stdlib]"
            ]
        writeFileUTF8 (tmpDir </> "dep" </> "Foo.daml") $ unlines
            [ "module Foo where"
            , "f : Scenario ()"
            , "f = scenario do"
            , "  p <- getParty \"p\""
            , "  submit p $ pure ()"
            , "  submit p $ pure ()"
            -- This will produce two implicit instances.
            -- GHC occasionally seems to inline those instances and I don’t understand
            -- how to reliably stop it from doing this therefore,
            -- we assert that the instance actually exists.
            ]
        callProcessSilent damlc
            [ "build"
            , "--enable-scenarios=yes" -- TODO: https://github.com/digital-asset/daml/issues/11316
            , "--project-root", tmpDir </> "dep"
            , "-o", tmpDir </> "dep" </> "dep.dar" ]
        Right Dalfs{..} <- readDalfs . Zip.toArchive <$> BSL.readFile (tmpDir </> "dep" </> "dep.dar")
        (_pkgId, pkg) <- either (fail . show) pure (LFArchive.decodeArchive LFArchive.DecodeAsMain (BSL.toStrict mainDalf))

        Just mod <- pure $ NM.lookup (LF.ModuleName ["Foo"]) (LF.packageModules pkg)
        let callStackInstances = do
                v@LF.DefValue{dvalBinder = (_, ty)} <- NM.toList (LF.moduleValues mod)
                LF.TSynApp
                  (LF.Qualified _ (LF.ModuleName ["GHC", "Classes"]) (LF.TypeSynName ["IP"]))
                  [ _
                  , LF.TCon
                      (LF.Qualified
                         _
                         (LF.ModuleName ["GHC", "Stack", "Types"])
                         (LF.TypeConName ["CallStack"])
                      )
                  ] <- pure ty
                pure v
        assertEqual "Expected two implicit CallStack" (length callStackInstances) 2

        step "building project that uses it via data-dependencies"
        createDirectoryIfMissing True (tmpDir </> "proj")
        writeFileUTF8 (tmpDir </> "proj" </> "daml.yaml") $ unlines
            [ "sdk-version: " <> sdkVersion
            , "name: proj"
            , "source: ."
            , "version: 0.1.0"
            , "dependencies: [daml-prim, daml-stdlib]"
            , "data-dependencies: "
            , "  - " <> (tmpDir </> "dep" </> "dep.dar")
            ]
        writeFileUTF8 (tmpDir </> "proj" </> "Main.daml") $ unlines
            [ "module Main where"
            , "import Foo"
            , "g = f"
            ]
        callProcessSilent damlc
            [ "build"
            , "--enable-scenarios=yes" -- TODO: https://github.com/digital-asset/daml/issues/11316
            , "--project-root"
            , tmpDir </> "proj" ]

    , dataDependenciesTest "Using orphan instances transitively"
        -- This test checks that orphan instances are imported
        -- transitively via data-dependencies.
        [
            (,) "Type.daml"
            [ "module Type where"
            , "data T = T"
            ]
        ,   (,) "OrphanInstance.daml"
            [ "{-# OPTIONS_GHC -Wno-orphans #-}"
            , "module OrphanInstance where"
            , "import Type"
            , "instance Show T where show T = \"T\""
            ]
        ,   (,) "Wrapper.daml"
            [ "module Wrapper where"
            , "import OrphanInstance ()"
            ]
        ]
        [
            (,) "Main.daml"
            [ "module Main where"
            , "import Type"
            , "import Wrapper ()"
            , "test = scenario do"
            , "  debug (show T)"
            -- If orphan instances were not imported transitively,
            -- we'd get a missing instance error from GHC.
            ]
        ]

    , dataDependenciesTest "Using reexported modules"
        -- This test checks that reexported modules are visible
        [
            (,) "Base.daml"
            [ "module Base where"
            , "data T = T"
            ]
        ,   (,) "Wrapper.daml"
            [ "module Wrapper (module Base) where"
            , "import Base"
            ]
        ]
        [
            (,) "Main.daml"
            [ "module Main where"
            , "import Wrapper"
            , "t = T"
            -- If reexported modules were not imported correctly,
            -- we'd get a missing data constructor error from GHC.
            ]
        ]

    , dataDependenciesTest "Using reexported values"
        -- This test checks that reexported values are visible
        [
            (,) "Base.daml"
            [ "module Base where"
            , "x = ()"
            ]
        ,   (,) "Wrapper.daml"
            [ "module Wrapper (x) where"
            , "import Base"
            ]
        ]
        [
            (,) "Main.daml"
            [ "module Main where"
            , "import Wrapper"
            , "y = x"
            -- If reexported values were not imported correctly,
            -- we'd get a missing variable error from GHC.
            ]
        ]

    , dataDependenciesTest "Using reexported classes"
        -- This test checks that reexported classes are visible
        [
            (,) "Base.daml"
            [ "module Base where"
            , "class C a where m : a"
            ]
        ,   (,) "Wrapper.daml"
            [ "module Wrapper (C (..)) where"
            , "import Base"
            ]
        ]
        [
            (,) "Main.daml"
            [ "module Main where"
            , "import Wrapper"
            , "f : C a => a"
            , "f = m"
            -- If reexported classes were not imported correctly,
            -- we'd get a missing class error from GHC.
            ]
        ]

    , dataDependenciesTest "Using reexported methods"
        -- This test checks that reexported methods are visible
        [
            (,) "Base.daml"
            [ "module Base where"
            , "class C a where m : a"
            , "instance C () where m = ()"
            ]
        ,   (,) "Wrapper.daml"
            [ "module Wrapper (C (..)) where"
            , "import Base"
            ]
        ]
        [
            (,) "Main.daml"
            [ "module Main where"
            , "import Wrapper"
            , "x : ()"
            , "x = m"
            -- If reexported methods were not imported correctly,
            -- we'd get a missing variable error from GHC.
            ]
        ]

    , dataDependenciesTest "Using reexported selectors"
        -- This test checks that reexported selectors are visible
        [
            (,) "Base.daml"
            [ "module Base where"
            , "data R = R with"
            , "  f : ()"
            ]
        ,   (,) "Wrapper.daml"
            [ "module Wrapper (R (..), r) where"
            , "import Base"
            , "r = R ()"
            ]
        ]
        [
            (,) "Main.daml"
            [ "module Main where"
            , "import Wrapper"
            , "x : ()"
            , "x = r.f"
            -- If reexported selectors were not imported correctly,
            -- we'd get a missing variable error from GHC.
            ]
        ]

    , dataDependenciesTest "Using reexported type operators"
        -- This test checks that we reconstruct reexported type operators properly.
        [
            (,) "Base.daml"
            [ "{-# LANGUAGE TypeOperators #-}"
            , "module Base (type (&) (..), type (+)) where"
            , "data a & b = X ()"
            , "type a + b = a & b"
            ]
        ,   (,) "Wrapper.daml"
            [ "module Wrapper (module Base) where"
            , "import Base"
            ]
        ]
        [   (,) "Main.daml"
            [ "{-# LANGUAGE TypeOperators #-}"
            , "module Main where"
            , "import Wrapper"
            , "ampersand: Bool & Int"
            , "ampersand = X ()"
            , "plus: Int + Bool"
            , "plus = X ()"
            -- If reexported type operators were not imported correctly,
            -- we'd get "missing type" or "expecting type constructor
            -- but found a variable" errors from GHC.
            ]
        ]

    , simpleImportTest "Constraint synonym context on instance"
        [ "{-# LANGUAGE UndecidableInstances #-}"
        , "module Lib where"

        , "class C a where c : a"
        , "instance C () where c = ()"

        , "class D a where d : a"
        , "instance D () where d = ()"

        , "type CD a = (C a, D a)"

        , "class E a where e : (a, a)"
        , "instance (CD a) => E a where e = (c, d)"
        ]
        [ "{-# LANGUAGE TypeOperators #-}"
        , "module Main where"
        , "import Lib"

        , "x : ((), ())"
        , "x = e"
        ]

    , simpleImportTest "Constraint synonym context on class"
        [ "module Lib where"

        , "class A x where a : x"
        , "class B x where b : x"

        , "type AB x = (A x, B x)"

        , "class AB x => C x"
        ]
        [ "module Main where"
        , "import Lib"

        , "useAfromC : C x => x"
        , "useAfromC = a"

        , "useBfromC : C x => x"
        , "useBfromC = b"
        ]

    , simpleImportTest "Fixities are preserved"
        [ "module Lib where"

        , "data Pair a b = Pair with"
        , "  fst : a"
        , "  snd : b"
        , "infixr 5 `Pair`"

        , "pair : a -> b -> Pair a b"
        , "pair = Pair"
        , "infixr 5 `pair`"

        , "class Category cat where"
        , "  id : cat a a"
        , "  (<<<) : cat b c -> cat a b -> cat a c"
        , "  infixr 1 <<<"

        , "class Category a => Arrow a where"
        , "  (&&&) : a b c -> a b c' -> a b (c,c')"
        , "  infixr 3 &&&"
        ]
        [ "{-# LANGUAGE TypeOperators #-}"
        , "module Main where"
        , "import Lib"

        -- If the fixity of the `Pair` data constructor isn't preserved, it's assumed
        -- to be infixl 9, so the type would be `Pair (Pair Bool Int) Text` instead.
        , "x : Pair Bool (Pair Int Text)"
        , "x = True `Pair` 42 `Pair` \"foo\""

        -- If the fixity of the `Pair` _type_ constructor isn't preserved, it's assumed
        -- to be infixl 9, so the type would be equivalent to `Pair (Pair Bool Int) Text` instead
        -- of the expected `Pair Bool (Pair Int Text)`
        , "x' : Bool `Pair` Int `Pair` Text"
        , "x' = x"

        -- Like `x`, but using the `pair` function instead of the `Pair` constructor.
        , "y : Pair Bool (Pair Int Text)"
        , "y = True `pair` 42 `pair` \"foo\""

        -- If the fixities of `<<<` and `&&&` are not preserved, they are both
        -- assumed to be infixl 9, so the type would be
        -- `Arrow arr => arr a b -> arr c a -> arr c (b, c)` instead.
        , "z : Arrow arr => (arr (a, b) c) -> (arr b a) -> (arr b c)"
        , "z f g = f <<< g &&& id"
        ]

    , simpleImportTestOptions "No 'inaccessible RHS' when pattern matching on interface"
        [ "--target=1.dev" ]
        [ "module Lib where"

        , "data EmptyInterfaceView = EmptyInterfaceView {}"
        , "interface I where viewtype EmptyInterfaceView"
        ]
        [ "{-# OPTIONS_GHC -Werror #-}"
        , "module Main where"
        , "import Lib"

        , "isJustI : Optional I -> Bool"
        , "isJustI mI = case mI of"
            -- If `I` lacks constructors, GHC infers the first case alternative
            -- to be inaccessible, since it's isomorphic to `Some (_ : Void)`,
            -- which can't be constructed.
        , "  Some _ -> True"
        , "  None -> False"
        ]

    , simpleImportTest "Instances of zero-method type classes are preserved"
        -- regression test for https://github.com/digital-asset/daml/issues/14585
        [ "module Lib where"

        , "class Marker a where"

        , "instance Marker Foo"

        , "data Foo = Foo"
        ]
        [ "module Main where"
        , "import Lib (Marker (..), Foo (..))"

        , "foo : Marker a => a -> ()"
        , "foo _ = ()"

        , "bar = foo Foo"
        ]

    , dataDependenciesTestOptions "Homonymous interface doesn't trigger 'ambiguous occurrence' error"
        [ "--target=1.dev" ]
        [   (,) "A.daml"
            [ "module A where"
            , "data Instrument = Instrument {}"
            ]
        ,   (,) "B.daml"
            [ "module B where"
            , "import qualified A"

            , "data EmptyInterfaceView = EmptyInterfaceView {}"
            , "interface Instrument where"
            , "  viewtype EmptyInterfaceView"
            , "  f : ()"
            , "x = A.Instrument"
            ]
        ]
        [   (,) "Main.daml"
            [ "module Main where"
            ]
        ]

    , dataDependenciesTestOptions "implement interface from data-dependency"
        [ "--target=1.dev" ]
        [   (,) "Lib.daml"
            [ "module Lib where"

            , "data EmptyInterfaceView = EmptyInterfaceView {}"
            , "interface Token where"
            , "  viewtype EmptyInterfaceView"
            , "  getOwner : Party -- ^ A method comment."
            , "  getAmount : Int"
            , "  setAmount : Int -> Token"

            , "  splitImpl : Int -> Update (ContractId Token, ContractId Token)"
            , "  transferImpl : Party -> Update (ContractId Token)"
            , "  noopImpl : () -> Update ()"

            , "  choice Split : (ContractId Token, ContractId Token) -- ^ An interface choice comment."
            , "    with"
            , "      splitAmount : Int -- ^ A choice field comment."
            , "    controller getOwner this"
            , "    do"
            , "      splitImpl this splitAmount"

            , "  choice Transfer : ContractId Token"
            , "    with"
            , "      newOwner : Party"
            , "    controller getOwner this, newOwner"
            , "    do"
            , "      transferImpl this newOwner"

            , "  nonconsuming choice Noop : ()"
            , "    with"
            , "      nothing : ()"
            , "    controller getOwner this"
            , "    do"
            , "      noopImpl this nothing"

            , "  choice GetRich : ContractId Token"
            , "    with"
            , "      byHowMuch : Int"
            , "    controller getOwner this"
            , "    do"
            , "        assert (byHowMuch > 0)"
            , "        create $ setAmount this (getAmount this + byHowMuch)"
            ]
        ]
        [
            (,) "Main.daml"
            [ "module Main where"
            , "import Lib"
            , "import DA.Assert"

            , "template Asset"
            , "  with"
            , "    issuer : Party"
            , "    owner : Party"
            , "    amount : Int"
            , "  where"
            , "    signatory issuer, owner"
            , "    interface instance Token for Asset where"
            , "      view = EmptyInterfaceView"
            , "      getOwner = owner"
            , "      getAmount = amount"
            , "      setAmount x = toInterface @Token (this with amount = x)"

            , "      splitImpl splitAmount = do"
            , "        assert (splitAmount < amount)"
            , "        cid1 <- create this with amount = splitAmount"
            , "        cid2 <- create this with amount = amount - splitAmount"
            , "        pure (toInterfaceContractId @Token cid1, toInterfaceContractId @Token cid2)"

            , "      transferImpl newOwner = do"
            , "        cid <- create this with owner = newOwner"
            , "        pure (toInterfaceContractId @Token cid)"

            , "      noopImpl nothing = do"
            , "        [1] === [1] -- make sure `mkMethod` calls are properly erased in the presence of polymorphism."
            , "        pure ()"

            , "main = scenario do"
            , "  p <- getParty \"Alice\""
            , "  p `submitMustFail` do"
            , "    create Asset with"
            , "      issuer = p"
            , "      owner = p"
            , "      amount = -1"
            , "  p `submit` do"
            , "    cidAsset1 <- create Asset with"
            , "      issuer = p"
            , "      owner = p"
            , "      amount = 15"
            , "    let cidToken1 = toInterfaceContractId @Token cidAsset1"
            , "    _ <- exercise cidToken1 (Noop ())"
            , "    (cidToken2, cidToken3) <- exercise cidToken1 (Split 10)"
            , "    token2 <- fetch cidToken2"
            , "    -- Party is duplicated because p is both observer & issuer"
            , "    signatory token2 === [p, p]"
            , "    getAmount token2 === 10"
            , "    case fromInterface token2 of"
            , "      None -> abort \"expected Asset\""
            , "      Some Asset {amount} ->"
            , "        amount === 10"
            , "    token3 <- fetch cidToken3"
            , "    getAmount token3 === 5"
            , "    case fromInterface token3 of"
            , "      None -> abort \"expected Asset\""
            , "      Some Asset {amount} ->"
            , "        amount === 5"

            , "    cidToken4 <- exercise cidToken3 (GetRich 20)"
            , "    token4 <- fetch cidToken4"
            , "    getAmount token4 === 25"
            , "    case fromInterface token4 of"
            , "      None -> abort \"expected Asset\""
            , "      Some Asset {amount} ->"
            , "        amount === 25"

            , "    pure ()"
            ]
        ]

    , dataDependenciesTestOptions "use interface from data-dependency"
        [ "--target=1.dev" ]
        [   (,) "Lib.daml"
            [ "module Lib where"
            , "import DA.Assert"

            , "data EmptyInterfaceView = EmptyInterfaceView {}"
            , "interface Token where"
            , "  viewtype EmptyInterfaceView"
            , "  getOwner : Party -- ^ A method comment."
            , "  getAmount : Int"
            , "  setAmount : Int -> Token"

            , "  splitImpl : Int -> Update (ContractId Token, ContractId Token)"
            , "  transferImpl : Party -> Update (ContractId Token)"
            , "  noopImpl : () -> Update ()"

            , "  choice Split : (ContractId Token, ContractId Token) -- ^ An interface choice comment."
            , "    with"
            , "      splitAmount : Int -- ^ A choice field comment."
            , "    controller getOwner this"
            , "    do"
            , "      splitImpl this splitAmount"

            , "  choice Transfer : ContractId Token"
            , "    with"
            , "      newOwner : Party"
            , "    controller getOwner this, newOwner"
            , "    do"
            , "      transferImpl this newOwner"

            , "  nonconsuming choice Noop : ()"
            , "    with"
            , "      nothing : ()"
            , "    controller getOwner this"
            , "    do"
            , "      noopImpl this nothing"

            , "  choice GetRich : ContractId Token"
            , "    with"
            , "      byHowMuch : Int"
            , "    controller getOwner this"
            , "    do"
            , "        assert (byHowMuch > 0)"
            , "        create $ setAmount this (getAmount this + byHowMuch)"

            , "template Asset"
            , "  with"
            , "    issuer : Party"
            , "    owner : Party"
            , "    amount : Int"
            , "  where"
            , "    signatory issuer, owner"
            , "    interface instance Token for Asset where"
            , "      view = EmptyInterfaceView"
            , "      getOwner = owner"
            , "      getAmount = amount"
            , "      setAmount x = toInterface @Token (this with amount = x)"

            , "      splitImpl splitAmount = do"
            , "        assert (splitAmount < amount)"
            , "        cid1 <- create this with amount = splitAmount"
            , "        cid2 <- create this with amount = amount - splitAmount"
            , "        pure (toInterfaceContractId @Token cid1, toInterfaceContractId @Token cid2)"

            , "      transferImpl newOwner = do"
            , "        cid <- create this with owner = newOwner"
            , "        pure (toInterfaceContractId @Token cid)"

            , "      noopImpl nothing = do"
            , "        [1] === [1] -- make sure `mkMethod` calls are properly erased in the presence of polymorphism."
            , "        pure ()"
            ]
        ]
        [
            (,) "Main.daml"
            [ "module Main where"
            , "import Lib"
            , "import DA.Assert"

            , "main = scenario do"
            , "  p <- getParty \"Alice\""
            , "  p `submitMustFail` do"
            , "    create Asset with"
            , "      issuer = p"
            , "      owner = p"
            , "      amount = -1"
            , "  p `submit` do"
            , "    cidAsset1 <- create Asset with"
            , "      issuer = p"
            , "      owner = p"
            , "      amount = 15"
            , "    let cidToken1 = toInterfaceContractId @Token cidAsset1"
            , "    _ <- exercise cidToken1 (Noop ())"
            , "    (cidToken2, cidToken3) <- exercise cidToken1 (Split 10)"
            , "    token2 <- fetch cidToken2"
            , "    -- Party is duplicated because p is both observer & issuer"
            , "    signatory token2 === [p, p]"
            , "    getAmount token2 === 10"
            , "    case fromInterface token2 of"
            , "      None -> abort \"expected Asset\""
            , "      Some Asset {amount} ->"
            , "        amount === 10"
            , "    token3 <- fetch cidToken3"
            , "    getAmount token3 === 5"
            , "    case fromInterface token3 of"
            , "      None -> abort \"expected Asset\""
            , "      Some Asset {amount} ->"
            , "        amount === 5"

            , "    cidToken4 <- exercise cidToken3 (GetRich 20)"
            , "    token4 <- fetch cidToken4"
            , "    getAmount token4 === 25"
            , "    case fromInterface token4 of"
            , "      None -> abort \"expected Asset\""
            , "      Some Asset {amount} ->"
            , "        amount === 25"

            , "    pure ()"
            ]
        ]

    , dataDependenciesTestOptions "require interface from data-dependency"
        [ "--target=1.dev" ]
        [   (,) "Lib.daml"
            [ "module Lib where"

            , "data EmptyInterfaceView = EmptyInterfaceView {}"
            , "interface Token where"
            , "  viewtype EmptyInterfaceView"
            , "  getOwner : Party -- ^ A method comment."
            , "  getAmount : Int"
            , "  setAmount : Int -> Token"

            , "  splitImpl : Int -> Update (ContractId Token, ContractId Token)"
            , "  transferImpl : Party -> Update (ContractId Token)"
            , "  noopImpl : () -> Update ()"

            , "  choice Split : (ContractId Token, ContractId Token) -- ^ An interface choice comment."
            , "    with"
            , "      splitAmount : Int -- ^ A choice field comment."
            , "    controller getOwner this"
            , "    do"
            , "      splitImpl this splitAmount"

            , "  choice Transfer : ContractId Token"
            , "    with"
            , "      newOwner : Party"
            , "    controller getOwner this, newOwner"
            , "    do"
            , "      transferImpl this newOwner"

            , "  nonconsuming choice Noop : ()"
            , "    with"
            , "      nothing : ()"
            , "    controller getOwner this"
            , "    do"
            , "      noopImpl this nothing"
            ]
        ]
        [
            (,) "Main.daml"
            [ "module Main where"
            , "import Lib"
            , "import DA.Assert"

            , "interface FancyToken requires Token where"
            , "  viewtype EmptyInterfaceView"
            , "  multiplier : Int"
            , "  choice GetRich : ContractId Token"
            , "    with"
            , "      byHowMuch : Int"
            , "    controller getOwner (toInterface @Token this)"
            , "    do"
            , "        assert (byHowMuch > 0)"
            , "        create $ setAmount"
            , "          (toInterface @Token this)"
            , "          ((getAmount (toInterface @Token this) + byHowMuch) * multiplier this)"

            , "template Asset"
            , "  with"
            , "    issuer : Party"
            , "    owner : Party"
            , "    amount : Int"
            , "  where"
            , "    signatory issuer, owner"
            , "    interface instance Token for Asset where"
            , "      view = EmptyInterfaceView"
            , "      getOwner = owner"
            , "      getAmount = amount"
            , "      setAmount x = toInterface @Token (this with amount = x)"

            , "      splitImpl splitAmount = do"
            , "        assert (splitAmount < amount)"
            , "        cid1 <- create this with amount = splitAmount"
            , "        cid2 <- create this with amount = amount - splitAmount"
            , "        pure (toInterfaceContractId @Token cid1, toInterfaceContractId @Token cid2)"

            , "      transferImpl newOwner = do"
            , "        cid <- create this with owner = newOwner"
            , "        pure (toInterfaceContractId @Token cid)"

            , "      noopImpl nothing = do"
            , "        [1] === [1] -- make sure `mkMethod` calls are properly erased in the presence of polymorphism."
            , "        pure ()"

            , "    interface instance FancyToken for Asset where"
            , "      view = EmptyInterfaceView"
            , "      multiplier = 5"

            , "main = scenario do"
            , "  p <- getParty \"Alice\""
            , "  p `submitMustFail` do"
            , "    create Asset with"
            , "      issuer = p"
            , "      owner = p"
            , "      amount = -1"
            , "  p `submit` do"
            , "    cidAsset1 <- create Asset with"
            , "      issuer = p"
            , "      owner = p"
            , "      amount = 15"
            , "    let cidToken1 = toInterfaceContractId @Token cidAsset1"
            , "    _ <- exercise cidToken1 (Noop ())"
            , "    (cidToken2, cidToken3) <- exercise cidToken1 (Split 10)"
            , "    token2 <- fetch cidToken2"
            , "    -- Party is duplicated because p is both observer & issuer"
            , "    signatory token2 === [p, p]"
            , "    getAmount token2 === 10"
            , "    case fromInterface token2 of"
            , "      None -> abort \"expected Asset\""
            , "      Some Asset {amount} ->"
            , "        amount === 10"
            , "    token3 <- fetch cidToken3"
            , "    getAmount token3 === 5"
            , "    case fromInterface token3 of"
            , "      None -> abort \"expected Asset\""
            , "      Some Asset {amount} ->"
            , "        amount === 5"

            , "    cidToken4 <- exercise (fromInterfaceContractId @FancyToken cidToken3) (GetRich 20)"
            , "    token4 <- fetch cidToken4"
            , "    getAmount token4 === 125"
            , "    case fromInterface token4 of"
            , "      None -> abort \"expected Asset\""
            , "      Some Asset {amount} ->"
            , "        amount === 125"

            , "    pure ()"
            ]
        ]

    , testCaseSteps "data-dependency doesn't leak unexported definitions from transitive dependencies" $ \step' -> withTempDir $ \tmpDir -> do
        let
          depProj = "dep"
          dataDepProj = "data-dep"
          mainProj = "main"

          path proj = tmpDir </> proj
          damlYaml proj = path proj </> "daml.yaml"
          damlMod proj mod = path proj </> mod <.> "daml"
          dar proj = path proj </> proj <.> "dar"
          step proj = step' ("building '" <> proj <> "' project")

          damlYamlBody name deps dataDeps = unlines
            [ "sdk-version: " <> sdkVersion
            , "name: " <> name
            , "build-options: [--target=1.dev]"
            , "source: ."
            , "version: 0.1.0"
            , "dependencies: [" <> intercalate ", " (["daml-prim", "daml-stdlib"] <> fmap dar deps) <> "]"
            , "data-dependencies: [" <> intercalate ", " (fmap dar dataDeps) <> "]"
            ]

        step depProj >> do
          createDirectoryIfMissing True (path depProj)
          writeFileUTF8 (damlYaml depProj) $ damlYamlBody depProj [] []
          writeFileUTF8 (damlMod depProj "Dep") $ unlines
            [ "module Dep (exported) where"

            , "exported : ()"
            , "exported = ()"

            , "unexported : ()"
            , "unexported = ()"
            ]
          callProcessSilent damlc
            [ "build"
            , "--project-root", path depProj
            , "-o", dar depProj
            ]

        step dataDepProj >> do
          createDirectoryIfMissing True (path dataDepProj)
          writeFileUTF8 (damlYaml dataDepProj) $ damlYamlBody dataDepProj [depProj] []
          writeFileUTF8 (damlMod dataDepProj "DataDep") $ unlines
            [ "module DataDep where"
            , "import Dep ()"
            ]
          callProcessSilent damlc
            [ "build"
            , "--project-root", path dataDepProj
            , "-o", dar dataDepProj
            ]

        step mainProj >> do
          createDirectoryIfMissing True (path mainProj)
          writeFileUTF8 (damlYaml mainProj) $ damlYamlBody mainProj [depProj] [dataDepProj]
          writeFileUTF8 (damlMod mainProj "Main") $ unlines
            [ "module Main where"

            , "import Dep"

            , "units : [()]"
            , "units = [exported, unexported]"
            ]

          -- This must fail since 'Main' shouldn't see 'Dep.unexported'.
          callProcessSilentError damlc
            [ "build"
            , "--project-root", path mainProj
            , "-o", dar mainProj
            ]

    , testCaseSteps "data-dependency interface hierarchy" $ \step' -> withTempDir $ \tmpDir -> do
        let
          tokenProj = "token"
          fancyTokenProj = "fancy-token"
          assetProj = "asset"
          mainProj = "main"

          path proj = tmpDir </> proj
          damlYaml proj = path proj </> "daml.yaml"
          damlMod proj mod = path proj </> mod <.> "daml"
          dar proj = path proj </> proj <.> "dar"
          step proj = step' ("building '" <> proj <> "' project")

          damlYamlBody name dataDeps = unlines
            [ "sdk-version: " <> sdkVersion
            , "name: " <> name
            , "build-options: [--target=1.dev]"
            , "source: ."
            , "version: 0.1.0"
            , "dependencies: [daml-prim, daml-stdlib]"
            , "data-dependencies: [" <> intercalate ", " (fmap dar dataDeps) <> "]"
            ]

        step tokenProj >> do
          createDirectoryIfMissing True (path tokenProj)
          writeFileUTF8 (damlYaml tokenProj) $ damlYamlBody tokenProj []
          writeFileUTF8 (damlMod tokenProj "Token") $ unlines
            [ "module Token where"

            , "data EmptyInterfaceView = EmptyInterfaceView {}"
            , "interface Token where"
            , "  viewtype EmptyInterfaceView"
            , "  getOwner : Party -- ^ A method comment."
            , "  getAmount : Int"
            , "  setAmount : Int -> Token"

            , "  splitImpl : Int -> Update (ContractId Token, ContractId Token)"
            , "  transferImpl : Party -> Update (ContractId Token)"
            , "  noopImpl : () -> Update ()"

            , "  choice Split : (ContractId Token, ContractId Token) -- ^ An interface choice comment."
            , "    with"
            , "      splitAmount : Int -- ^ A choice field comment."
            , "    controller getOwner this"
            , "    do"
            , "      splitImpl this splitAmount"

            , "  choice Transfer : ContractId Token"
            , "    with"
            , "      newOwner : Party"
            , "    controller getOwner this, newOwner"
            , "    do"
            , "      transferImpl this newOwner"

            , "  nonconsuming choice Noop : ()"
            , "    with"
            , "      nothing : ()"
            , "    controller getOwner this"
            , "    do"
            , "      noopImpl this nothing"
            ]
          callProcessSilent damlc
            [ "build"
            , "--project-root", path tokenProj
            , "-o", dar tokenProj
            ]

        step fancyTokenProj >> do
          createDirectoryIfMissing True (path fancyTokenProj)
          writeFileUTF8 (damlYaml fancyTokenProj) $ damlYamlBody fancyTokenProj
            [ tokenProj
            ]
          writeFileUTF8 (damlMod fancyTokenProj "FancyToken") $ unlines
            [ "module FancyToken where"
            , "import Token"

            , "interface FancyToken requires Token where"
            , "  viewtype EmptyInterfaceView"
            , "  multiplier : Int"
            , "  choice GetRich : ContractId Token"
            , "    with"
            , "      byHowMuch : Int"
            , "    controller getOwner (toInterface @Token this)"
            , "    do"
            , "        assert (byHowMuch > 0)"
            , "        create $ setAmount"
            , "          (toInterface @Token this)"
            , "          ((getAmount (toInterface @Token this) + byHowMuch) * multiplier this)"
            ]
          callProcessSilent damlc
            [ "build"
            , "--project-root", path fancyTokenProj
            , "-o", dar fancyTokenProj
            ]

        step assetProj >> do
          createDirectoryIfMissing True (path assetProj)
          writeFileUTF8 (damlYaml assetProj) $ damlYamlBody assetProj
            [ tokenProj
            , fancyTokenProj
            ]
          writeFileUTF8 (damlMod assetProj "Asset") $ unlines
            [ "module Asset where"
            , "import Token"
            , "import FancyToken"

            , "import DA.Assert"

            , "template Asset"
            , "  with"
            , "    issuer : Party"
            , "    owner : Party"
            , "    amount : Int"
            , "  where"
            , "    signatory issuer, owner"
            , "    interface instance Token for Asset where"
            , "      view = EmptyInterfaceView"
            , "      getOwner = owner"
            , "      getAmount = amount"
            , "      setAmount x = toInterface @Token (this with amount = x)"

            , "      splitImpl splitAmount = do"
            , "        assert (splitAmount < amount)"
            , "        cid1 <- create this with amount = splitAmount"
            , "        cid2 <- create this with amount = amount - splitAmount"
            , "        pure (toInterfaceContractId @Token cid1, toInterfaceContractId @Token cid2)"

            , "      transferImpl newOwner = do"
            , "        cid <- create this with owner = newOwner"
            , "        pure (toInterfaceContractId @Token cid)"

            , "      noopImpl nothing = do"
            , "        [1] === [1] -- make sure `mkMethod` calls are properly erased in the presence of polymorphism."
            , "        pure ()"

            , "    interface instance FancyToken for Asset where"
            , "      view = EmptyInterfaceView"
            , "      multiplier = 5"
            ]
          callProcessSilent damlc
            [ "build"
            , "--project-root", path assetProj
            , "-o", dar assetProj
            ]

        step mainProj >> do
          createDirectoryIfMissing True (path mainProj)
          writeFileUTF8 (damlYaml mainProj) $ damlYamlBody mainProj
            [ tokenProj
            , fancyTokenProj
            , assetProj
            ]
          writeFileUTF8 (damlMod mainProj "Main") $ unlines
            [ "module Main where"
            , "import Token"
            , "import FancyToken"
            , "import Asset"

            , "import DA.Assert"

            , "main = scenario do"
            , "  p <- getParty \"Alice\""
            , "  p `submitMustFail` do"
            , "    create Asset with"
            , "      issuer = p"
            , "      owner = p"
            , "      amount = -1"
            , "  p `submit` do"
            , "    cidAsset1 <- create Asset with"
            , "      issuer = p"
            , "      owner = p"
            , "      amount = 15"
            , "    let cidToken1 = toInterfaceContractId @Token cidAsset1"
            , "    _ <- exercise cidToken1 (Noop ())"
            , "    (cidToken2, cidToken3) <- exercise cidToken1 (Split 10)"
            , "    token2 <- fetch cidToken2"
            , "    -- Party is duplicated because p is both observer & issuer"
            , "    signatory token2 === [p, p]"
            , "    getAmount token2 === 10"
            , "    case fromInterface token2 of"
            , "      None -> abort \"expected Asset\""
            , "      Some Asset {amount} ->"
            , "        amount === 10"
            , "    token3 <- fetch cidToken3"
            , "    getAmount token3 === 5"
            , "    case fromInterface token3 of"
            , "      None -> abort \"expected Asset\""
            , "      Some Asset {amount} ->"
            , "        amount === 5"

            , "    cidToken4 <- exercise (fromInterfaceContractId @FancyToken cidToken3) (GetRich 20)"
            , "    token4 <- fetch cidToken4"
            , "    getAmount token4 === 125"
            , "    case fromInterface token4 of"
            , "      None -> abort \"expected Asset\""
            , "      Some Asset {amount} ->"
            , "        amount === 125"

            , "    pure ()"
            ]
          callProcessSilent damlc
            [ "build"
            , "--enable-scenarios=yes" -- TODO: https://github.com/digital-asset/daml/issues/11316
            , "--project-root", path mainProj
            ]

    , simpleImportTestOptions "retroactive interface instance of template from data-dependency"
        [ "--target=1.dev" ]
        [ "module Lib where"

        , "template T with"
        , "    p : Party"
        , "  where"
        , "    signatory p"
        ]
        [ "{-# OPTIONS_GHC -Werror #-}"
        , "module Main where"
        , "import Lib"

        , "data EmptyInterfaceView = EmptyInterfaceView {}"

        , "interface I where"
        , "  viewtype EmptyInterfaceView"
        , "  m : ()"
        , "  interface instance I for T where"
        , "    view = EmptyInterfaceView"
        , "    m = ()"
        ]

    , simpleImportTestOptions "retroactive interface instance of qualified template from data-dependency"
        [ "--target=1.dev" ]
        [ "module Lib where"

        , "template T with"
        , "    p : Party"
        , "  where"
        , "    signatory p"
        ]
        [ "{-# OPTIONS_GHC -Werror #-}"
        , "module Main where"
        , "import qualified Lib"

        , "data EmptyInterfaceView = EmptyInterfaceView {}"

        , "interface I where"
        , "  viewtype EmptyInterfaceView"
        , "  m : ()"
        , "  interface instance I for Lib.T where"
        , "    view = EmptyInterfaceView"
        , "    m = ()"
        ]

    , testCaseSteps "Cross-SDK data-dependency with daml-script" $ \step' -> withTempDir $ \tmpDir -> do
        -- regression test for https://github.com/digital-asset/daml/issues/14291
        let
          mainProj = "main"

          path proj = tmpDir </> proj
          damlYaml proj = path proj </> "daml.yaml"
          damlMod proj mod = path proj </> mod <.> "daml"
          step proj = step' ("building '" <> proj <> "' project")

        step mainProj >> do
          createDirectoryIfMissing True (path mainProj)
          writeFileUTF8 (damlYaml mainProj) $ unlines
            [ "sdk-version: " <> sdkVersion
            , "name: " <> mainProj
            , "source: ."
            , "version: 0.1.0"
            , "dependencies: [daml-prim, daml-stdlib, " <> damlScriptDar <> "]"
            , "data-dependencies: [" <> libWithScriptDar <> "]"
            ]
          writeFileUTF8 (damlMod mainProj "Main") $ unlines
            [ "module Main where"
            , "import Daml.Script"
            , "import Lib qualified"

            , "template T1"
            , "  with"
            , "    party : Party"
            , "  where"
            , "    signatory party"

            , "    nonconsuming choice C1 : Bool"
            , "     controller party"
            , "        do pure False"

            , "run1 : Lib.Script ()"
            , "run1 = Lib.run"

            , "run2 : Script ()"
            , "run2 = script do"
            , "  alice <- allocateParty \"alice\""

            , "  t <- alice `submit` createCmd Lib.T0 with party = alice"
            , "  b <- alice `submit` exerciseCmd t Lib.C0"
            , "  debug b"
            , "  alice `submit` archiveCmd t"

            , "  t <- alice `submit` createCmd T1 with party = alice"
            , "  b <- alice `submit` exerciseCmd t C1"
            , "  debug b"
            , "  alice `submit` archiveCmd t"
            ]
          callProcessSilent damlc
            [ "test"
            , "--project-root", path mainProj
            ]

    , testCaseSteps "User-defined exceptions" $ \step -> withTempDir $ \tmpDir -> do
        step "building project to be imported via data-dependencies"
        createDirectoryIfMissing True (tmpDir </> "lib")
        writeFileUTF8 (tmpDir </> "lib" </> "daml.yaml") $ unlines
            [ "sdk-version: " <> sdkVersion
            , "name: lib"
            , "source: ."
            , "version: 0.1.0"
            , "dependencies: [daml-prim, daml-stdlib]"
            ]
        writeFileUTF8 (tmpDir </> "lib" </> "Lib.daml") $ unlines
            [ "module Lib where"
            , "import DA.Exception"
            , "exception E1"
            , "  with m : Text"
            , "  where message m"
            , ""
            , "libFnThatThrowsE1 : Update ()"
            , "libFnThatThrowsE1 = throw (E1 \"throw from lib\")"
            , "libFnThatThrows : Exception e => e -> Update ()"
            , "libFnThatThrows x = throw x"
            , "libFnThatCatches : Exception e => (() -> Update ()) -> (e -> Update ()) -> Update ()"
            , "libFnThatCatches m c = try m () catch e -> c e"
            ]
        callProcessSilent damlc
            [ "build"
            , "--project-root", tmpDir </> "lib"
            , "-o", tmpDir </> "lib" </> "lib.dar"
            , "--target", LF.renderVersion (LF.featureMinVersion LF.featureExceptions) ]

        step "building project that imports it via data-dependencies"
        createDirectoryIfMissing True (tmpDir </> "main")
        writeFileUTF8 (tmpDir </> "main" </> "daml.yaml") $ unlines
            [ "sdk-version: " <> sdkVersion
            , "name: main"
            , "source: ."
            , "version: 0.1.0"
            , "dependencies: [daml-prim, daml-stdlib]"
            , "data-dependencies: "
            , "  - " <> (tmpDir </> "lib" </> "lib.dar")
            ]
        writeFileUTF8 (tmpDir </> "main" </> "Main.daml") $ unlines
            [ "module Main where"
            , "import DA.Exception"
            , "import Lib"
            , "exception E2"
            , "  with m : Text"
            , "  where message m"
            , ""
            , "mainFnThatThrowsE1 : Update ()"
            , "mainFnThatThrowsE1 = throw (E1 \"throw from main\")"
            , "mainFnThatThrowsE2 : Update ()"
            , "mainFnThatThrowsE2 = libFnThatThrows (E2 \"thrown from lib\")"
            , "mainFnThatCatchesE1 : Update ()"
            , "mainFnThatCatchesE1 = try libFnThatThrowsE1 catch E1 e -> pure ()"
            , "mainFnThatCatchesE2 : (() -> Update ()) -> Update ()"
            , "mainFnThatCatchesE2 m = libFnThatCatches m (\\ (e: E2) -> pure ())"
            ]
        callProcessSilent damlc
            [ "build"
            , "--project-root", tmpDir </> "main"
            , "--target", LF.renderVersion LF.versionDev ]

    , testCaseSteps "Package ids are stable across rebuilds" $ \step -> withTempDir $ \tmpDir -> do
        step "building lib (project to be imported via data-dependencies)"
        createDirectoryIfMissing True (tmpDir </> "lib")
        writeFileUTF8 (tmpDir </> "lib" </> "daml.yaml") $ unlines
            [ "sdk-version: " <> sdkVersion
            , "name: lib"
            , "source: ."
            , "version: 0.1.0"
            , "dependencies:"
            , "  - daml-prim"
            , "  - daml-stdlib"
            ]
        writeFileUTF8 (tmpDir </> "lib" </> "Lib.daml") $ unlines
            [ "module Lib where"
            , "data Data = Data ()"
            ]
        callProcessSilent damlc
            [ "build"
            , "--project-root", tmpDir </> "lib"
            , "-o", tmpDir </> "lib" </> "lib.dar"
            , "--target", LF.renderVersion LF.versionDev
            ]

        step "building main (project that imports lib via data-dependencies)"
        createDirectoryIfMissing True (tmpDir </> "main")
        writeFileUTF8 (tmpDir </> "main" </> "daml.yaml") $ unlines
            [ "sdk-version: " <> sdkVersion
            , "name: main"
            , "source: ."
            , "version: 0.1.0"
            , "dependencies:"
            , "  - daml-prim"
            , "  - daml-stdlib"
            , "data-dependencies:"
            , "  - " <> (tmpDir </> "lib" </> "lib.dar")
            ]
        writeFileUTF8 (tmpDir </> "main" </> "Main.daml") $ unlines
            [ "module Main where"
            , "import Lib qualified"
            , "data Data = Data Lib.Data"
            ]
        callProcessSilent damlc
            [ "build"
            , "--project-root", tmpDir </> "main"
            , "-o", tmpDir </> "main" </> "main.dar"
            , "--target", LF.renderVersion LF.versionDev
            ]

        step "building main again as main2.dar"
        callProcessSilent damlc
            [ "build"
            , "--project-root", tmpDir </> "main"
            , "-o", tmpDir </> "main" </> "main2.dar"
            , "--target", LF.renderVersion LF.versionDev
            ]

        step "compare package ids in main.dar and main2.dar"
        libPackageIds <- darPackageIds (tmpDir </> "lib" </> "lib.dar")
        mainPackageIds <- darPackageIds (tmpDir </> "main" </> "main.dar")
        main2PackageIds <- darPackageIds (tmpDir </> "main" </> "main2.dar")
        let
          mainOnlyPackageIds = mainPackageIds \\ libPackageIds
          main2OnlyPackageIds = main2PackageIds \\ libPackageIds
        main2OnlyPackageIds @?= mainOnlyPackageIds

    , testCaseSteps "Standard library exceptions" $ \step -> withTempDir $ \tmpDir -> do
        step "building project to be imported via data-dependencies"
        createDirectoryIfMissing True (tmpDir </> "lib")
        writeFileUTF8 (tmpDir </> "lib" </> "daml.yaml") $ unlines
            [ "sdk-version: " <> sdkVersion
            , "name: lib"
            , "source: ."
            , "version: 0.1.0"
            , "dependencies: [daml-prim, daml-stdlib]"
            ]
        writeFileUTF8 (tmpDir </> "lib" </> "Lib.daml") $ unlines
            [ "module Lib where"
            , "import DA.Assert"
            , "import DA.Exception"
            , "template TLib"
            , "  with"
            , "    p : Party"
            , "  where"
            , "    signatory p"
            , "    ensure False"
            , ""
            , "libFnThatThrowsGeneralError : Party -> Update ()"
            , "libFnThatThrowsGeneralError _ = error \"thrown from lib\""
            , "libFnThatThrowsArithmeticError : Party -> Update ()"
            , "libFnThatThrowsArithmeticError _ = pure (1 / 0) >> pure ()"
            , "libFnThatThrowsAssertionFailed : Party -> Update ()"
            , "libFnThatThrowsAssertionFailed _ = assert False"
            , "libFnThatThrowsPreconditionFailed : Party -> Update ()"
            , "libFnThatThrowsPreconditionFailed p = create (TLib p) >> pure ()"
            , ""
            , "libFnThatCatchesGeneralError : (() -> Update ()) -> Update ()"
            , "libFnThatCatchesGeneralError m = try m () catch (e: GeneralError) -> pure ()"
            , "libFnThatCatchesArithmeticError : (() -> Update ()) -> Update ()"
            , "libFnThatCatchesArithmeticError m = try m () catch (e: ArithmeticError) -> pure ()"
            , "libFnThatCatchesAssertionFailed : (() -> Update ()) -> Update ()"
            , "libFnThatCatchesAssertionFailed m = try m () catch (e: AssertionFailed) -> pure ()"
            , "libFnThatCatchesPreconditionFailed : (() -> Update ()) -> Update ()"
            , "libFnThatCatchesPreconditionFailed m = try m () catch (e: PreconditionFailed) -> pure ()"
            ]
        callProcessSilent damlc
            [ "build"
            , "--project-root", tmpDir </> "lib"
            , "-o", tmpDir </> "lib" </> "lib.dar"
            , "--target", LF.renderVersion (LF.featureMinVersion LF.featureExceptions) ]

        step "building project that imports it via data-dependencies"
        createDirectoryIfMissing True (tmpDir </> "main")
        writeFileUTF8 (tmpDir </> "main" </> "daml.yaml") $ unlines
            [ "sdk-version: " <> sdkVersion
            , "name: main"
            , "source: ."
            , "version: 0.1.0"
            , "dependencies: [daml-prim, daml-stdlib]"
            , "data-dependencies: "
            , "  - " <> (tmpDir </> "lib" </> "lib.dar")
            ]
        writeFileUTF8 (tmpDir </> "main" </> "Main.daml") $ unlines
            [ "module Main where"
            , "import DA.Assert"
            , "import DA.Exception"
            , "import Lib"
            , "template TMain"
            , "  with"
            , "    p : Party"
            , "  where"
            , "    signatory p"
            , "    ensure False"
            , ""
            , "mainFnThatThrowsGeneralError : Party -> Update ()"
            , "mainFnThatThrowsGeneralError _ = error \"thrown from main\""
            , "mainFnThatThrowsArithmeticError : Party -> Update ()"
            , "mainFnThatThrowsArithmeticError _ = pure (1 / 0) >> pure ()"
            , "mainFnThatThrowsAssertionFailed : Party -> Update ()"
            , "mainFnThatThrowsAssertionFailed _ = assert False"
            , "mainFnThatThrowsPreconditionFailed : Party -> Update ()"
            , "mainFnThatThrowsPreconditionFailed p = create (TMain p) >> pure ()"
            , ""
            , "mainFnThatCatchesGeneralError : (() -> Update ()) -> Update ()"
            , "mainFnThatCatchesGeneralError m = try m () catch (e: GeneralError) -> pure ()"
            , "mainFnThatCatchesArithmeticError : (() -> Update ()) -> Update ()"
            , "mainFnThatCatchesArithmeticError m = try m () catch (e: ArithmeticError) -> pure ()"
            , "mainFnThatCatchesAssertionFailed : (() -> Update ()) -> Update ()"
            , "mainFnThatCatchesAssertionFailed m = try m () catch (e: AssertionFailed) -> pure ()"
            , "mainFnThatCatchesPreconditionFailed : (() -> Update ()) -> Update ()"
            , "mainFnThatCatchesPreconditionFailed m = try m () catch (e: PreconditionFailed) -> pure ()"
            , ""
            , "mkScenario : ((() -> Update ()) -> Update ()) -> (Party -> Update ()) -> Scenario ()"
            , "mkScenario catcher thrower = scenario do"
            , "    p <- getParty \"Alice\""
            , "    submit p (catcher (\\() -> thrower p))"
            , ""
            , "libThrow1 = mkScenario mainFnThatCatchesGeneralError libFnThatThrowsGeneralError"
            , "libThrow2 = mkScenario mainFnThatCatchesArithmeticError libFnThatThrowsArithmeticError"
            , "libThrow3 = mkScenario mainFnThatCatchesAssertionFailed libFnThatThrowsAssertionFailed"
            , "libThrow4 = mkScenario mainFnThatCatchesPreconditionFailed libFnThatThrowsPreconditionFailed"
            , ""
            , "libCatch1 = mkScenario libFnThatCatchesGeneralError mainFnThatThrowsGeneralError"
            , "libCatch2 = mkScenario libFnThatCatchesArithmeticError mainFnThatThrowsArithmeticError"
            , "libCatch3 = mkScenario libFnThatCatchesAssertionFailed mainFnThatThrowsAssertionFailed"
            , "libCatch4 = mkScenario libFnThatCatchesPreconditionFailed mainFnThatThrowsPreconditionFailed"
           ]
        callProcessSilent damlc
            [ "build"
            , "--enable-scenarios=yes" -- TODO: https://github.com/digital-asset/daml/issues/11316
            , "--project-root", tmpDir </> "main"
            , "--target", LF.renderVersion LF.versionDev ]
        step "running damlc test"
        callProcessSilent damlc
            [ "test"
            , "--enable-scenarios=yes" -- TODO: https://github.com/digital-asset/daml/issues/11316
            , "--project-root", tmpDir </> "main"
            , "--target", LF.renderVersion LF.versionDev ]
    ]
  where
    Tools
      { damlc
      , validate
      , damlScriptDar
      , oldProjDar
      , libWithScriptDar
      } = tools

    simpleImportTest :: String -> [String] -> [String] -> TestTree
    simpleImportTest title = simpleImportTestOptions title []

    simpleImportTestOptions :: String -> [String] -> [String] -> [String] -> TestTree
    simpleImportTestOptions title options lib main =
        dataDependenciesTestOptions title options [("Lib.daml", lib)] [("Main.daml", main)]

    dataDependenciesTest :: String -> [(FilePath, [String])] -> [(FilePath, [String])] -> TestTree
    dataDependenciesTest title = dataDependenciesTestOptions title []

    dataDependenciesTestOptions :: String -> [String] -> [(FilePath, [String])] -> [(FilePath, [String])] -> TestTree
    dataDependenciesTestOptions title buildOptions libModules mainModules =
        testCaseSteps title $ \step -> withTempDir $ \tmpDir -> do
            step "building project to be imported via data-dependencies"
            createDirectoryIfMissing True (tmpDir </> "lib")
            writeFileUTF8 (tmpDir </> "lib" </> "daml.yaml") $ unlines
                [ "sdk-version: " <> sdkVersion
                , "name: lib"
                , "build-options: [" <> intercalate ", " buildOptions <> "]"
                , "source: ."
                , "version: 0.1.0"
                , "dependencies: [daml-prim, daml-stdlib]"
                ]
            forM_ libModules $ \(path, contents) ->
                writeFileUTF8 (tmpDir </> "lib" </> path) $ unlines contents
            callProcessSilent damlc
                [ "build"
                , "--enable-scenarios=yes" -- TODO: https://github.com/digital-asset/daml/issues/11316
                , "--project-root", tmpDir </> "lib"
                , "-o", tmpDir </> "lib" </> "lib.dar" ]

            step "building project that imports it via data-dependencies"
            createDirectoryIfMissing True (tmpDir </> "main")
            writeFileUTF8 (tmpDir </> "main" </> "daml.yaml") $ unlines
                [ "sdk-version: " <> sdkVersion
                , "name: main"
                , "build-options: [" <> intercalate ", " buildOptions <> "]"
                , "source: ."
                , "version: 0.1.0"
                , "dependencies: [daml-prim, daml-stdlib]"
                , "data-dependencies: "
                , "  - " <> (tmpDir </> "lib" </> "lib.dar")
                ]
            forM_ mainModules $ \(path, contents) ->
                writeFileUTF8 (tmpDir </> "main" </> path) $ unlines contents
            callProcessSilent damlc
                [ "build"
                , "--enable-scenarios=yes" -- TODO: https://github.com/digital-asset/daml/issues/11316
                , "--project-root"
                , tmpDir </> "main" ]
