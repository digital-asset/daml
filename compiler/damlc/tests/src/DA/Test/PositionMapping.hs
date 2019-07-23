-- Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

{-# LANGUAGE OverloadedStrings #-}
module DA.Test.PositionMapping (main) where

import Test.Tasty
import Test.Tasty.HUnit

import Development.IDE.Core.PositionMapping
import Language.Haskell.LSP.Types


main :: IO ()
main = defaultMain $
    testGroup "position mapping"
        [ testGroup "toCurrent"
              [ testCase "before" $
                toCurrent
                    (Range (Position 0 1) (Position 0 3))
                    "ab"
                    (Position 0 0) @?= Just (Position 0 0)
              , testCase "after, same line, same length" $
                toCurrent
                    (Range (Position 0 1) (Position 0 3))
                    "ab"
                    (Position 0 3) @?= Just (Position 0 3)
              , testCase "after, same line, increased length" $
                toCurrent
                    (Range (Position 0 1) (Position 0 3))
                    "abc"
                    (Position 0 3) @?= Just (Position 0 4)
              , testCase "after, same line, decreased length" $
                toCurrent
                    (Range (Position 0 1) (Position 0 3))
                    "a"
                    (Position 0 3) @?= Just (Position 0 2)
              , testCase "after, next line, no newline" $
                toCurrent
                    (Range (Position 0 1) (Position 0 3))
                    "abc"
                    (Position 1 3) @?= Just (Position 1 3)
              , testCase "after, next line, newline" $
                toCurrent
                    (Range (Position 0 1) (Position 0 3))
                    "abc\ndef"
                    (Position 1 0) @?= Just (Position 2 0)
              , testCase "after, same line, newline" $
                toCurrent
                    (Range (Position 0 1) (Position 0 3))
                    "abc\nd"
                    (Position 0 4) @?= Just (Position 1 2)
              , testCase "after, same line, newline + newline at end" $
                toCurrent
                    (Range (Position 0 1) (Position 0 3))
                    "abc\nd\n"
                    (Position 0 4) @?= Just (Position 2 1)
              ]
        , testGroup "fromCurrent"
              [ testCase "before" $
                fromCurrent
                    (Range (Position 0 1) (Position 0 3))
                    "ab"
                    (Position 0 0) @?= Just (Position 0 0)
              , testCase "after, same line, same length" $
                fromCurrent
                    (Range (Position 0 1) (Position 0 3))
                    "ab"
                    (Position 0 3) @?= Just (Position 0 3)
              , testCase "after, same line, increased length" $
                fromCurrent
                    (Range (Position 0 1) (Position 0 3))
                    "abc"
                    (Position 0 4) @?= Just (Position 0 3)
              , testCase "after, same line, decreased length" $
                fromCurrent
                    (Range (Position 0 1) (Position 0 3))
                    "a"
                    (Position 0 2) @?= Just (Position 0 3)
              , testCase "after, next line, no newline" $
                fromCurrent
                    (Range (Position 0 1) (Position 0 3))
                    "abc"
                    (Position 1 3) @?= Just (Position 1 3)
              , testCase "after, next line, newline" $
                fromCurrent
                    (Range (Position 0 1) (Position 0 3))
                    "abc\ndef"
                    (Position 2 0) @?= Just (Position 1 0)
              , testCase "after, same line, newline" $
                fromCurrent
                    (Range (Position 0 1) (Position 0 3))
                    "abc\nd"
                    (Position 1 2) @?= Just (Position 0 4)
              , testCase "after, same line, newline + newline at end" $
                fromCurrent
                    (Range (Position 0 1) (Position 0 3))
                    "abc\nd\n"
                    (Position 2 1) @?= Just (Position 0 4)
              ]
        ]

