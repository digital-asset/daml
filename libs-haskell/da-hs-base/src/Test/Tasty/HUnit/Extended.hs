-- Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

-- | Adds functions to diff two values for testing purposes.
module Test.Tasty.HUnit.Extended
    ( module Test.Tasty.HUnit
    , DiffResult(..)
    , contextDiff
    , contextDiffShowWithString
    , contextDiffShow
    , assertEqualDiff
    , assertEqualDiffShow
    ) where

-- import qualified Data.Algorithm.DiffContext as Diff
import qualified Data.Text                  as T
import qualified Test.Tasty.HUnit           as HUnit
-- import qualified "pretty" Text.PrettyPrint.HughesPJ  as Pretty
import qualified Text.Show.Pretty           as PShow
import           Test.Tasty.HUnit


-- | The result of diffing two values.
data DiffResult
    = Same
    -- ^ The two values are the same.
    | Different String
    -- ^ A description of the difference of the two values.
    deriving (Show)


-- | Diff two 'T.Text's. Only displays the differences and leave the rest out.
contextDiff :: T.Text -> T.Text -> DiffResult
contextDiff is shouldBe
 | is == shouldBe   = Same
 | otherwise        = Different "files are different"


{- TODO(BL): This code has a space leak, probably in the prettyContextDiff function.
   Diffing two files which had > 10k different lines was using 10GB of space.

 = case length diff of
    0 -> Same
    _ -> Different
      $ Pretty.render
      $ Diff.prettyContextDiff (Pretty.text "Is:") (Pretty.text "Should Be:") text
      $ diff
  where
    shouldBeLines = T.lines shouldBe
    isLines = T.lines is
    diff = Diff.getContextDiff 5 isLines shouldBeLines
    text = Pretty.text . T.unpack
 where   
    shouldBeLines   = T.lines shouldBe
    isLines         = T.lines is
    diff            = Diff.getContextDiff 5 isLines shouldBeLines
-}


-- | 'pretty-show' a value and compare it to a string. Useful for file-based tests.
contextDiffShowWithString
    :: Show a
    => a
    -- ^ What the value actually is. Will be serialized into a string via 'pretty-show'.
    -> T.Text
    -- ^ What you want the 'pretty-show'd value to look like.
    -> DiffResult
contextDiffShowWithString is shouldBe =
    contextDiff (T.pack $ PShow.ppShow is) shouldBe


-- | 'pretty-show' two values and compare them.
contextDiffShow
    :: Show a
    => a
    -- ^ What the value actually is. Will be serialized into a string via 'pretty-show'.
    -> a
    -- ^ What the value should look like. Will be serialized into a string via 'pretty-show'.
    -> DiffResult
contextDiffShow is shouldBe =
    contextDiffShowWithString is $ T.pack $ PShow.ppShow shouldBe


assertEqualDiff
    :: String -- ^ The message prefix, will be prepended to the message in case of failure.
    -> String -- ^ The message suffix, will be appended to the message in case of failure.
    -> T.Text -- ^ The actual value
    -> T.Text -- ^ The expected value
    -> HUnit.Assertion
assertEqualDiff prefix suffix actual expected  =
    case resultDiff of
        Same -> pure ()
        Different diff ->
            HUnit.assertFailure
          $ (if null prefix then "" else prefix ++ "\n")
            ++ diff ++ (if null suffix then "" else "\n" ++ suffix)
  where
    resultDiff = contextDiff actual expected


-- | Compare two values by converting them to a pretty-printed string and diffing those.
assertEqualDiffShow
    :: Show a
    => a
    -- ^ the actual value
    -> a
    -- ^ The expected value
    -> HUnit.Assertion
assertEqualDiffShow actual expected =
    case resultDiff of
        Same -> pure ()
        Different diff ->
            HUnit.assertFailure diff
  where
    resultDiff = contextDiffShow actual expected
