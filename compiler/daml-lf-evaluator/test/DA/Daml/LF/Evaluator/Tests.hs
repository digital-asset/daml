-- Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

module DA.Daml.LF.Evaluator.Tests
  ( main
  ) where

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
import DA.Daml.LF.Optimize (optimizeWorld,World)
import DA.Daml.LF.Reader (readDalfs,Dalfs(..))
import qualified DA.Daml.LF.Ast as LF
import qualified DA.Daml.LF.Evaluator as EV

main :: IO ()
main = do
  setEnv "TASTY_NUM_THREADS" "1" True
  run tests

tests :: [Test]
tests =
  [ Test 1 "decrement" 10 9
  , Test 5 "fact" 4 24
  , Test 6 "fact" 5 120

  , Test 1 "dub_dub_dub" 1 8

  , Test 1 "decrement" 0 (-1)
  , Test 1 "thrice_decrement" 0 (-3)
  , Test 1 "thrice_thrice_decrement" 0 (-27)

  , Test 7 "length_list" 7 3 -- apps=7 because optimized code passes a lambda to FOLDL
  , Test 1 "sum_list" 7 24 -- apps=1 because optimized code passes the prim ADDI to FOLDL
  , Test 7 "run_makeDecimal" 7 789

  , Test 633 "nthPrime" 10 29 -- TODO: can apps be better?
  , Test 86897 "nthPrime" 100 541

  , Test 15 "run_sum_myList" 9 30
  , Test 15 "run_sum_myList2" 99 300

  , Test 177 "nfib" 10 177

  , Test 1 "let1" 10 16
  , Test 1 "let2" 10 26
  , Test 1 "let3" 10 13

  , Test 1 "let4" 1 16
  , Test 1 "let5" 0 16

  , Test 1 "let6" 0 6
  , Test 3 "let7" 0 6 -- TODO: make NBE better here, and reduce apps to 1, same as for let6

  , Test 1 "easy" 0 27
  , Test 28 "hard" 0 27 -- TODO: make #apps same as for easy

  , Test 1 "if1" 0 4
  , Test 1 "if2" 0 9
  , Test 2 "if3" 0 9 -- TODO: have apps=1, same as if2
  , Test 1 "if4" 0 9
  , Test 2 "if5" 0 9 -- TODO: have apps=1, same as if4
  , Test 2 "if6" 0 10 -- TODO: have apps=1

  ]

-- testing for DAML functions of type: `Int -> Int`
data Test = Test
  { expectedAppsWhenEvaluatingOptimizedCode :: Int
  , functionName :: String
  , arg :: Int64
  , expected :: Int64
  }

run :: [Test] -> IO ()
run tests = do
  filename <- locateRunfiles (mainWorkspace </> "compiler/daml-lf-evaluator/examples.dar")
  dalfs <- readDar filename
  world <- EV.decodeDalfs dalfs
  Tasty.defaultMain $ Tasty.testGroup "daml-lf-evaluator" (map (makeTasty world) tests)

readDar :: FilePath -> IO Dalfs
readDar inFile = do
  archiveBS <- BS.readFile inFile
  either fail pure $ readDalfs $ ZipArchive.toArchive $ BSL.fromStrict archiveBS

makeTasty :: World -> Test -> Tasty.TestTree
makeTasty world Test{expectedAppsWhenEvaluatingOptimizedCode=xa,functionName,arg,expected} = do
  let mn = LF.ModuleName ["Examples"]
  let vn = LF.ExprValName $ Text.pack functionName
  let name = Text.unpack (LF.unExprValName vn) <> "(" <> show arg <> ")"
  Tasty.testCaseSteps name $ \_step -> do

    -- check the original program evaluates as expected
    let prog = EV.simplify world mn vn
    let (actual,countsOrig) = EV.runIntProgArg prog arg
    Tasty.assertEqual "original" expected actual
    let EV.Counts{apps=a1,prims=p1,projections=q1} = countsOrig

    -- check the program constructed from optimized DAML-LF has same result
    worldO <- optimizeWorld world
    let progO = EV.simplify worldO mn vn
    let (actualO,countsOpt) = EV.runIntProgArg progO arg
    Tasty.assertEqual "optimized" expected actualO

    -- check the optimized program doesn't take more steps to evaluate
    -- in fact check that it has less application steps (i.e it has actually improved!)
    -- But check it executes the same number of primitive ops.
    let EV.Counts{apps=a2,prims=p2,projections=q2} = countsOpt
    let mkName tag x y = tag <> ":" <> show x <> "-->" <> show y
    Tasty.assertBool (mkName "apps" a1 a2) (a2 < a1)
    Tasty.assertBool (mkName "prim" p1 p2) (p2 == p1)
    Tasty.assertBool (mkName "proj" q1 q2) (q2 <= q1)

    Tasty.assertBool (mkName "apps!" xa a2) (a2 == xa)

