-- Copyright (c) 2019 The DAML Authors. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

module DA.Test.PositionMapping (main) where

import Test.QuickCheck
import Test.QuickCheck.Instances ()
import Test.Tasty
import Test.Tasty.HUnit
import Test.Tasty.QuickCheck

import Data.Rope.UTF16 (Rope)
import qualified Data.Rope.UTF16 as Rope
import qualified Data.Text as T
import Development.IDE.Core.PositionMapping (fromCurrent, toCurrent)
import Language.Haskell.LSP.Types
import Language.Haskell.LSP.VFS (applyChange)


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
              , testCase "after, same line, newline + newline at end" $
                toCurrent
                    (Range (Position 0 1) (Position 0 1))
                    "abc"
                    (Position 0 1) @?= Just (Position 0 4)
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
              , testCase "after, same line, newline + newline at end" $
                fromCurrent
                    (Range (Position 0 1) (Position 0 1))
                    "abc"
                    (Position 0 4) @?= Just (Position 0 1)
              ]
        , adjustOption (\(QuickCheckTests i) -> QuickCheckTests (max 1000 i)) $ testGroup "properties"
              [ testProperty "fromCurrent r t <=< toCurrent r t" $ do
                -- Note that it is important to use suchThatMap on all values at once
                -- instead of only using it on the position. Otherwise you can get
                -- into situations where there is no position that can be mapped back
                -- for the edit which will result in QuickCheck looping forever.
                let gen = do
                        rope <- genRope
                        range <- genRange rope
                        PrintableText replacement <- arbitrary
                        oldPos <- genPosition rope
                        pure (range, replacement, oldPos)
                forAll
                    (suchThatMap gen
                        (\(range, replacement, oldPos) -> (range, replacement, oldPos,) <$> toCurrent range replacement oldPos)) $
                    \(range, replacement, oldPos, newPos) ->
                    fromCurrent range replacement newPos === Just oldPos
              , testProperty "toCurrent r t <=< fromCurrent r t" $ do
                let gen = do
                        rope <- genRope
                        range <- genRange rope
                        PrintableText replacement <- arbitrary
                        let newRope = applyChange rope (TextDocumentContentChangeEvent (Just range) Nothing replacement)
                        newPos <- genPosition newRope
                        pure (range, replacement, newPos)
                forAll
                    (suchThatMap gen
                        (\(range, replacement, newPos) -> (range, replacement, newPos,) <$> fromCurrent range replacement newPos)) $
                    \(range, replacement, newPos, oldPos) ->
                    toCurrent range replacement oldPos === Just newPos
              ]
        ]

newtype PrintableText = PrintableText { getPrintableText :: T.Text }
    deriving Show

instance Arbitrary PrintableText where
    arbitrary = PrintableText . T.pack . getPrintableString <$> arbitrary


genRope :: Gen Rope
genRope = Rope.fromText . getPrintableText <$> arbitrary

genPosition :: Rope -> Gen Position
genPosition r = do
    row <- choose (0, max 0 $ rows - 1)
    let columns = Rope.columns (nthLine row r)
    column <- choose (0, max 0 $ columns - 1)
    pure $ Position row column
    where rows = Rope.rows r

genRange :: Rope -> Gen Range
genRange r = do
    startPos@(Position startLine startColumn) <- genPosition r
    let maxLineDiff = max 0 $ rows - 1 - startLine
    endLine <- choose (startLine, startLine + maxLineDiff)
    let columns = Rope.columns (nthLine endLine r)
    endColumn <-
        if startLine == endLine
            then choose (startColumn, columns)
            else choose (0, max 0 $ columns - 1)
    pure $ Range startPos (Position endLine endColumn)
    where rows = Rope.rows r

-- | Get the ith line of a rope, starting from 0. Trailing newline not included.
nthLine :: Int -> Rope -> Rope
nthLine i r
    | i < 0 = error $ "Negative line number: " <> show i
    | i == 0 && Rope.rows r == 0 = r
    | i >= Rope.rows r = error $ "Row number out of bounds: " <> show i <> "/" <> show (Rope.rows r)
    | otherwise = Rope.takeWhile (/= '\n') $ fst $ Rope.splitAtLine 1 $ snd $ Rope.splitAtLine (i - 1) r
