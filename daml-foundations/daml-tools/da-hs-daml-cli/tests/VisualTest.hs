{-# LANGUAGE OverloadedStrings #-}
module VisualTest
   ( main
   ) where

import Test.Tasty
import Test.Tasty.HUnit
import System.Directory

main :: IO ()
main = defaultMain tests

tests :: TestTree
tests = testGroup "Tests" [unitTests]


temp :: String -> String -> Bool
temp a b = a == b

unitTests :: TestTree
unitTests = testGroup "Unit tests"
  [
  testCase "List comparison (different length)"  (assertBool "Non-empty list"  (temp "a" "a") )
   , testCase "file exists" (doesFileExist "daml-foundations/daml-tools/da-hs-daml-cli/visual-test-daml.dar" @? "missing")

  ]
