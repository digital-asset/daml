-- Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0
module DA.Test.DataDependencies (main) where

import qualified "zip-archive" Codec.Archive.Zip as Zip
import Control.Monad.Extra
import DA.Bazel.Runfiles
import qualified DA.Daml.LF.Ast as LF
import DA.Daml.LF.Reader (readDalfs, Dalfs(..))
import qualified DA.Daml.LF.Proto3.Archive as LFArchive
import DA.Test.Process
import DA.Test.Util
import qualified Data.ByteString.Lazy as BSL
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
    setEnv "TASTY_NUM_THREADS" "1" True
    damlc <- locateRunfiles (mainWorkspace </> "compiler" </> "damlc" </> exe "damlc")
    repl <- locateRunfiles (mainWorkspace </> "daml-lf" </> "repl" </> exe "repl")
    davlDar <- locateRunfiles ("davl-v3" </> "released" </> "davl-v3.dar")
    oldProjDar <- locateRunfiles (mainWorkspace </> "compiler" </> "damlc" </> "tests" </> "dars" </> "old-proj-0.13.55-snapshot.20200309.3401.0.6f8c3ad8-1.8.dar")
    let validate dar = callProcessSilent damlc ["validate-dar", dar]
    defaultMain $ tests Tools{..}

data Tools = Tools -- and places
  { damlc :: FilePath
  , repl :: FilePath
  , validate :: FilePath -> IO ()
  , davlDar :: FilePath
  , oldProjDar :: FilePath
  }

darPackageIds :: FilePath -> IO [LF.PackageId]
darPackageIds fp = do
    archive <- Zip.toArchive <$> BSL.readFile fp
    Dalfs mainDalf dalfDeps <- either fail pure $ readDalfs archive
    Right dalfPkgIds  <- pure $ mapM (LFArchive.decodeArchivePackageId . BSL.toStrict) $ mainDalf : dalfDeps
    pure dalfPkgIds


numStablePackages :: LF.Version -> Int
numStablePackages ver
  | ver == LF.version1_6 = 15
  | ver == LF.version1_7 = 16
  | ver == LF.version1_8 = 16
  | ver == LF.versionDev = 16
  | otherwise = error $ "Unsupported LF version: " <> show ver

tests :: Tools -> TestTree
tests Tools{damlc,repl,validate,davlDar,oldProjDar} = testGroup "Data Dependencies" $
    [ testCaseSteps ("Cross DAML-LF version: " <> LF.renderVersion depLfVer <> " -> " <> LF.renderVersion targetLfVer)  $ \step -> withTempDir $ \tmpDir -> do
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
              , "data X = X" -- This should generate a DAML-LF enum

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
          withCurrentDirectory proja $ callProcessSilent damlc ["build", "--target=" <> LF.renderVersion depLfVer, "-o", proja </> "proja.dar"]
          projaPkgIds <- darPackageIds (proja </> "proja.dar")
          -- daml-stdlib, daml-prim and proja
          length projaPkgIds @?= numStablePackages depLfVer + 2 + 1

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
          withCurrentDirectory projb $ callProcessSilent damlc
            [ "build", "--target=" <> LF.renderVersion targetLfVer, "-o", projb </> "projb.dar"
            ]
          step "Validating DAR"
          validate $ projb </> "projb.dar"
          projbPkgIds <- darPackageIds (projb </> "projb.dar")
          -- daml-prim, daml-stdlib for targetLfVer, daml-prim, daml-stdlib for depLfVer if targetLfVer /= depLfVer, proja and projb
          length projbPkgIds @?= numStablePackages
            targetLfVer + 2 + (if targetLfVer /= depLfVer then 2 else 0) + 1 + 1
          length (filter (`notElem` projaPkgIds) projbPkgIds) @?=
              (numStablePackages targetLfVer - numStablePackages depLfVer) + -- new stable packages
              1 + -- projb
              (if targetLfVer /= depLfVer then 2 else 0) -- different daml-stdlib/daml-prim
    | depLfVer <- LF.supportedOutputVersions
    , targetLfVer <- LF.supportedOutputVersions
    , targetLfVer >= depLfVer
    ] <>
    [ testCaseSteps "Cross-SDK dependency on DAVL" $ \step -> withTempDir $ \tmpDir -> do
          step "Building DAR"
          writeFileUTF8 (tmpDir </> "daml.yaml") $ unlines
              [ "sdk-version: " <> sdkVersion
              , "version: 0.0.1"
              , "name: foobar"
              , "source: ."
              , "dependencies: [daml-prim, daml-stdlib]"
              , "data-dependencies: [" <> show davlDar <> "]"
              ]
          writeFileUTF8 (tmpDir </> "Main.daml") $ unlines
              [ "module Main where"

              , "import DAVL"
              , "import DA.Assert"
              , "import qualified OldStdlib.DA.Internal.Template as OldStdlib"

              -- We exploit internals of the template desugaring here
              -- until we can reconstruct typeclasses or at least functions.
              , "instance HasCreate EmployeeProposal where"
              , "  create = GHC.Types.primitive @\"UCreate\""
              , "instance HasFetch EmployeeProposal where"
              , "  fetch = GHC.Types.primitive @\"UFetch\""
              , "instance HasExercise EmployeeProposal OldStdlib.Archive () where"
              , "  exercise = GHC.Types.primitive @\"UExercise\""

              , "test = scenario do"
              , "  alice <- getParty \"Alice\""
              , "  bob <- getParty \"Bob\""
              , "  eve <- getParty \"eve\""
              , "  let role = EmployeeRole bob alice eve"
              , "  cid <- submit alice $ create (EmployeeProposal role 42)"
              , "  EmployeeProposal{employeeRole} <- submit bob $ fetch cid"
              , "  employee employeeRole === bob"
              , "  company employeeRole === alice"
              , "  () <- submit alice $ exercise cid OldStdlib.Archive"
              , "  pure ()"
              ]
          withCurrentDirectory tmpDir $ callProcessSilent damlc
            [ "build", "-o", tmpDir </> "foobar.dar"
            -- We need to use the old stdlib for the Archive type
            , "--package", "daml-stdlib-cc6d52aa624250119006cd19d51c60006762bd93ca5a6d288320a703024b33da (DA.Internal.Template as OldStdlib.DA.Internal.Template)"
            ]
          step "Validating DAR"
          validate $ tmpDir </> "foobar.dar"
          step "Testing scenario"
          callProcessSilent repl ["test", "Main:test", tmpDir </> "foobar.dar"]
    , testCaseSteps "Mixed dependencies and data-dependencies" $ \step -> withTempDir $ \tmpDir -> do
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
          withCurrentDirectory (tmpDir </> "lib") $ callProcessSilent damlc ["build", "-o", tmpDir </> "lib" </> "lib.dar"]
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
          withCurrentDirectory (tmpDir </> "a") $ callProcessSilent damlc ["build", "-o", tmpDir </> "a" </> "a.dar"]
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
          withCurrentDirectory (tmpDir </> "b") $ callProcessSilent damlc ["build", "-o", tmpDir </> "b" </> "b.dar"]
          projbPackageIds <- darPackageIds (tmpDir </> "b" </> "b.dar")
          length projbPackageIds @?= length libPackageIds + 2

          step "Validating DAR"
          validate $ tmpDir </> "b" </> "b.dar"

    , simpleImportTest "Tuples"
              [ "module Lib where"
              , "data X = X (Text, Int)"
              -- ^ Check that tuples are mapped back to DAML tuples.
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
              ]
              [ "module Main where"
              , "import Lib"
              , "x : MyInt'"
              , "x = 10"
              , "f : MyArrow Int Int"
              , "f a = a + 1"
              , "type MyUnit = Int"
              , "g : MyUnit -> MyUnit"
                -- ^ this tests that MyUnit wasn't exported from Foo
              , "g a = a"
              , "type MyOptional t = Int"
              , "h : MyOptional Int -> MyOptional Int"
                  -- ^ this tests that MyOptional wasn't exported from Foo
              , "h a = a"
              , "myFmap : MyFunctor t => (a -> b) -> t a -> t b"
              , "myFmap = fmap"
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
              withCurrentDirectory projDir $ callProcessSilent damlc ["build", "-o", projDir </> "lib.dar"]

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
          withCurrentDirectory projDir $ callProcessSilent damlc ["build", "-o", projDir </> "a.dar"]

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
          withCurrentDirectory projDir $ callProcessSilent damlc ["build", "-o", projDir </> "b.dar"]

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
          withCurrentDirectory projDir $ callProcessSilent damlc ["build", "-o", projDir </> "c.dar"]
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
          , "data-dependencies: [simple-dalf-0.0.0.dalf]"
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
        withCurrentDirectory projDir $
            callProcessSilent genSimpleDalf $
            ["--with-archive-choice" | withArchiveChoice ] <> ["simple-dalf-0.0.0.dalf"]
        withCurrentDirectory projDir $ callProcess damlc ["build", "--target=1.dev", "--generated-src"]
        let dar = projDir </> ".daml/dist/proj-0.1.0.dar"
        assertFileExists dar
        callProcessSilent damlc ["test", "--target=1.dev", "--project-root", projDir, "--generated-src"]
    | withArchiveChoice <- [False, True]
    ] <>
    [ testCaseSteps ("Typeclasses and instances from DAML-LF " <> LF.renderVersion depLfVer <> " to " <> LF.renderVersion targetLfVer) $ \step -> withTempDir $ \tmpDir -> do
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
              , "import DA.Generics"
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
              -- Test that deriving Generic doesn't blow everything up
              , "data X t = X t deriving Generic"
              -- Test that indirect references to an erased type don't
              -- stick around with a dangling reference, including via
              -- typeclass specializations.
              , "class MyGeneric t where"
              , "class MyGeneric t => YourGeneric t where"
              , "instance {-# OVERLAPPABLE #-} DA.Generics.Generic t rep => MyGeneric t"
              , "instance {-# OVERLAPPABLE #-} Generic Int (D1 ('MetaData ('MetaData0 \"\" \"\" \"\" 'True)) (K1 R ())) where"
              , "  from = error \"\""
              , "  to = error \"\""
              , "instance YourGeneric Int"
                  -- ^ tests detection of Generic reference via
                  -- specialization of MyGeneric instance

              -- [Issue #7256] Tests that orphan superclass instances are dependended on correctly.
              -- E.g. Applicative Validation is an orphan instance implemented in DA.Validation.
              , "instance Action (Validation e) where"
              , "  v >>= f = case v of"
              , "    Errors e-> Errors e"
              , "    Success a -> f a"
              ]
          writeFileUTF8 (proja </> "daml.yaml") $ unlines
              [ "sdk-version: " <> sdkVersion
              , "name: proja"
              , "version: 0.0.1"
              , "source: src"
              , "dependencies: [daml-prim, daml-stdlib]"
              ]
          withCurrentDirectory proja $ callProcessSilent damlc ["build", "--target=" <> LF.renderVersion depLfVer, "-o", proja </> "proja.dar"]

          step "Build projb"
          createDirectoryIfMissing True (projb </> "src")
          writeFileUTF8 (projb </> "src" </> "B.daml") $ unlines
              [ "module B where"
              , "import A ( Foo (foo), Bar (..), usingFoo, Q (..), usingEq, RR(RR), P(P), AnyWrapper(..), FunT(..), OptionalT(..), ActionTrans(..), usesHasField, usesHasFieldEmpty )"
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
              ]
          writeFileUTF8 (projb </> "daml.yaml") $ unlines
              [ "sdk-version: " <> sdkVersion
              , "name: projb"
              , "version: 0.0.1"
              , "source: src"
              , "dependencies: [daml-prim, daml-stdlib]"
              , "data-dependencies: [" <> show (proja </> "proja.dar") <> "]"
              ]
          withCurrentDirectory projb $ callProcessSilent damlc
            [ "build", "--target=" <> LF.renderVersion targetLfVer, "-o", projb </> "projb.dar"
            ]
          validate $ projb </> "projb.dar"

    | depLfVer <- LF.supportedOutputVersions
    , targetLfVer <- LF.supportedOutputVersions
    , targetLfVer >= depLfVer
    , LF.supports depLfVer LF.featureTypeSynonyms -- only test for new-style typeclasses
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
          withCurrentDirectory (tmpDir </> "type") $ callProcessSilent damlc ["build", "-o", "type.dar"]

          step "building dependency project"
          createDirectoryIfMissing True (tmpDir </> "dependency")
          writeFileUTF8 (tmpDir </> "dependency" </> "daml.yaml") $ unlines
              [ "sdk-version: " <> sdkVersion
              , "name: dependency"
              , "source: ."
              , "version: 0.1.0"
              , "dependencies: [daml-prim, daml-stdlib]"
              , "data-dependencies: [" <> show (tmpDir </> "type/type.dar") <> "]"
              , "build-options: [ \"--target=1.dev\" ]"
              ]
          writeFileUTF8 (tmpDir </> "dependency" </> "Dependency.daml") $ unlines
             [ "module Dependency where"
             , "import Proxy"
             , "instance Functor Proxy where"
             , "  fmap _ Proxy = Proxy"
             ]
          withCurrentDirectory (tmpDir </> "dependency") $ callProcessSilent damlc ["build", "-o", "dependency.dar"]

          step "building data-dependency project"
          createDirectoryIfMissing True (tmpDir </> "data-dependency")
          writeFileUTF8 (tmpDir </> "data-dependency" </> "daml.yaml") $ unlines
              [ "sdk-version: " <> sdkVersion
              , "name: data-dependency"
              , "source: ."
              , "version: 0.1.0"
              , "dependencies: [daml-prim, daml-stdlib]"
              , "data-dependencies: [" <> show (tmpDir </> "type/type.dar") <> "]"
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
          withCurrentDirectory (tmpDir </> "data-dependency") $ callProcessSilent damlc ["build", "-o", "data-dependency.dar"]

          step "building top-level project"
          createDirectoryIfMissing True (tmpDir </> "top" </> "data-dependency")
          writeFileUTF8 (tmpDir </> "top" </> "daml.yaml") $ unlines
              [ "sdk-version: " <> sdkVersion
              , "name: top"
              , "source: ."
              , "version: 0.1.0"
              , "dependencies: [daml-prim, daml-stdlib, " <> show (tmpDir </> "dependency/dependency.dar") <> ", " <> show (tmpDir </> "type/type.dar") <> "]"
              , "data-dependencies: [" <> show (tmpDir </> "data-dependency/data-dependency.dar") <> "]"
              , "build-options: [--target=1.dev]"
              ]
          writeFileUTF8 (tmpDir </> "top" </> "Top.daml") $ unlines
              [ "module Top where"
              , "import DataDependency"
              , "import Proxy"
              -- Test that we can use the Applicaive instance of Proxy from the data-dependency
              , "f = pure () : Proxy ()"
              ]
          withCurrentDirectory (tmpDir </> "top") $ callProcessSilent damlc ["build", "-o", "top.dar"]

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
        withCurrentDirectory (tmpDir </> "dep") $
            callProcessSilent damlc ["build", "-o", "dep.dar"]
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
        withCurrentDirectory (tmpDir </> "proj") $
            callProcessSilent damlc ["build"]
    ]
  where
    simpleImportTest :: String -> [String] -> [String] -> TestTree
    simpleImportTest title lib main =
        testCaseSteps title $ \step -> withTempDir $ \tmpDir -> do
            step "building project to be imported via data-dependencies"
            createDirectoryIfMissing True (tmpDir </> "lib")
            writeFileUTF8 (tmpDir </> "lib" </> "daml.yaml") $ unlines
                [ "sdk-version: " <> sdkVersion
                , "name: lib"
                , "source: ."
                , "version: 0.1.0"
                , "dependencies: [daml-prim, daml-stdlib]"
                ]
            writeFileUTF8 (tmpDir </> "lib" </> "Lib.daml") $ unlines lib
            withCurrentDirectory (tmpDir </> "lib") $
                callProcessSilent damlc ["build", "-o", "lib.dar"]

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
            writeFileUTF8 (tmpDir </> "main" </> "Main.daml") $ unlines main
            withCurrentDirectory (tmpDir </> "main") $
                callProcessSilent damlc ["build"]
