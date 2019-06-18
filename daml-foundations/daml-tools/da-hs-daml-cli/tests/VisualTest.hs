{-# LANGUAGE OverloadedStrings #-}
module DamlcTest
   ( main
   ) where

import Test.Tasty
import Test.Tasty.HUnit

main :: IO ()
main = defaultMain tests

tests :: TestTree
tests = testGroup "Tests" [unitTests]


temp :: String -> String -> Bool
temp a b = a == b

unitTests :: TestTree
unitTests = testGroup "Unit tests"
  [
  testCase "List comparison (different length)"  (assertBool "Non-empty list"  (temp "a" "b") )
  ]
