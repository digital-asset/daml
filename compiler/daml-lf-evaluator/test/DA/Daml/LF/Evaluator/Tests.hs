-- Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

module DA.Daml.LF.Evaluator.Tests (main) where

import Data.Int (Int64)
import System.Environment.Blank (setEnv)
import System.FilePath ((</>))
import qualified "zip-archive" Codec.Archive.Zip as ZipArchive
import qualified Data.ByteString as BS (readFile)
import qualified Data.ByteString.Lazy as BSL (fromStrict)
import qualified Data.Text as Text
import qualified Test.Tasty as Tasty (defaultMain,testGroup,TestTree)
import qualified Test.Tasty.HUnit as Tasty (assertBool,assertEqual,testCaseSteps)

import DA.Bazel.Runfiles (locateRunfiles,mainWorkspace)
import DA.Daml.LF.Evaluator (decodeDalfs,simplify,runIntProgArg,Counts(..),Throw(..))
import DA.Daml.LF.Optimize (optimize)
import DA.Daml.LF.Reader (readDalfs,Dalfs(..))
import qualified DA.Daml.LF.Ast as LF

main :: IO ()
main = do
  setEnv "TASTY_NUM_THREADS" "1" True
  run tests

tests :: [Test]
tests =
  [ mkTest 1 "dub" 1 2
  , mkTest 1 "dub_dub" 1 4
  , mkTest 1 "dub_dub_dub" 1 8

  , mkTest 1 "decrement" 10 9
  , mkTest 5 "fact" 4 24
  , mkTest 6 "fact" 5 120

  , mkTest 1 "decrement" 0 (-1)
  , mkTest 1 "thrice_decrement" 0 (-3)
  , mkTest 1 "thrice_thrice_decrement" 0 (-27)

  , mkTest 7 "length_list" 7 3 -- apps=7 because optimized code passes a lambda to FOLDL
  , mkTest 1 "sum_list" 7 24 -- apps=1 because optimized code passes the prim ADDI to FOLDL
  , mkTest 7 "run_makeDecimal" 7 789

  , mkTest 632 "nthPrime" 10 29
  , mkTest 86896 "nthPrime" 100 541

  , mkTest 13 "run_sum_myList" 9 30
  , mkTest 13 "run_sum_myList2" 99 300

  , mkTest 177 "nfib" 10 177

  , mkTest 1 "let1" 10 16
  , mkTest 1 "let2" 10 26
  , mkTest 1 "let3" 10 13

  , mkTest 1 "let4" 1 16
  , mkTest 1 "let5" 0 16

  , mkTest 1 "let6" 0 6
  , mkTest 1 "let7" 0 6

  , mkTest 1 "easy" 0 27
  , mkTest 1 "hard" 0 27

  , mkTest 1 "if1" 0 4
  , mkTest 1 "if2" 0 9
  , mkTest 1 "if3" 0 9
  , mkTest 1 "if4" 0 9
  , mkTest 1 "if5" 0 9
  , mkTest 1 "if6" 0 10

  , Test 1 "err1" 0 (Left (Throw "foobar"))
  , Test 1 "err2" 0 (Right 0)

  , mkTest 1 "dive" 9 42
  ]

-- | Like `Test`, except we always expect a non-`Throw` result
mkTest :: Int -> String -> Int64 -> Int64 -> Test
mkTest xa fn arg expected = Test xa fn arg (Right expected)

-- | Setup a testcase, for DAML function named `fn` (of type `Int64 -> Int64)
-- | Check the result is `expected` when the function is applied to Int64 `arg`
-- | (We check evaluation of both original code and the optimized code)
-- | Also, check the expected number of `application` during optimized evaluation

data Test = Test
  { expectedAppsWhenEvaluatingOptimizedCode :: Int
  , functionName :: String
  , arg :: Int64
  , expected :: Either Throw Int64
  }

run :: [Test] -> IO ()
run tests = do
  filename <- locateRunfiles (mainWorkspace </> "compiler/daml-lf-evaluator/examples.dar")
  dalfs <- readDar filename
  (pkgs,[mod]) <- decodeDalfs dalfs
  Tasty.defaultMain $ Tasty.testGroup "daml-lf-evaluator" (map (makeTasty pkgs mod) tests)

readDar :: FilePath -> IO Dalfs
readDar inFile = do
  archiveBS <- BS.readFile inFile
  either fail pure $ readDalfs $ ZipArchive.toArchive $ BSL.fromStrict archiveBS

makeTasty :: [LF.ExternalPackage] -> LF.Module -> Test -> Tasty.TestTree
makeTasty pkgs mod Test{expectedAppsWhenEvaluatingOptimizedCode=xa,functionName,arg,expected} = do

  let vn = LF.ExprValName $ Text.pack functionName
  let name = Text.unpack (LF.unExprValName vn) <> "(" <> show arg <> ")"

  Tasty.testCaseSteps name $ \_step -> do

    -- check the original program evaluates as expected
    let prog = simplify pkgs mod vn
    let (actual,countsOrig) = runIntProgArg prog arg
    Tasty.assertEqual "original" expected actual
    let Counts{apps=a1,prims=p1,projections=q1} = countsOrig

    -- check the program constructed from optimized DAML-LF has same result
    modO <- optimize pkgs mod
    let progO = simplify pkgs modO vn
    let (actualO,countsOpt) = runIntProgArg progO arg
    Tasty.assertEqual "optimized" expected actualO

    -- check the optimized program doesn't take more steps to evaluate
    -- in fact check that it has less application steps (i.e it has actually improved!)
    -- But check it executes the same number of primitive ops.
    let Counts{apps=a2,prims=p2,projections=q2} = countsOpt
    let mkName tag x y = tag <> ":" <> show x <> "-->" <> show y
    Tasty.assertBool (mkName "apps" a1 a2) (a2 <= a1)
    Tasty.assertBool (mkName "prim" p1 p2) (p2 == p1)
    Tasty.assertBool (mkName "proj" q1 q2) (q2 <= q1)
    Tasty.assertBool (mkName "apps!" xa a2) (a2 == xa)
