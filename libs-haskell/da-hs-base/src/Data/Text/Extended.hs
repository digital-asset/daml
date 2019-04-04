-- Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

{-# LANGUAGE NoImplicitPrelude #-}
-- | An extended version of "Data.Text", which provides a slightly more
-- extensive export lists and missing orphan instances.
module Data.Text.Extended
  (
    module Data.Text

  , show
  , Data.Text.Extended.splitOn
  , startsWithLower
  , startsWithUpper

    -- ** Lenses
  , packed
  , unpacked

  , readFileUtf8
  , writeFileUtf8
  ) where

import qualified Data.ByteString as BS
import           Data.List.NonEmpty as NE

import           Data.Text.Lens (packed, unpacked)
import qualified Data.Text as T
import qualified Data.Text.Encoding as T
import qualified Data.Text.Encoding.Error as T
import           Data.Text hiding (splitOn)

import           DA.Prelude hiding (show)
import qualified DA.Prelude


-- | 'Show' a value and pack it into a strict 'Text' value.
show :: Show a => a -> Text
show = pack . DA.Prelude.show

-- | A version of `T.splitOn` with the more precise non-empty list as its result type.
splitOn :: Text
        -- ^ String to split on. If this string is empty, an error
        -- will occur.
        -> Text
        -- ^ Input text.
        -> NE.NonEmpty Text
splitOn pat src =
  case T.splitOn pat src of
    [] -> error "Data.Text.Extended.splitOn: got an empty list. This is an implementation error in 'Data.Text.splitOn'."
    x : xs -> x NE.:| xs

-- | Check if a 'T.Text' starts with a lowercase character.
startsWithLower :: T.Text -> Bool
startsWithLower = maybe False (isLower . fst) . T.uncons

-- | Check if a 'T.Text' starts with an uppercase character.
startsWithUpper :: T.Text -> Bool
startsWithUpper = maybe False (isUpper . fst) . T.uncons

-- | Read a file as UTF-8.
readFileUtf8 :: FilePath -> IO T.Text
readFileUtf8 f = T.decodeUtf8With T.lenientDecode <$> BS.readFile f

-- | Write a file as UTF-8.
writeFileUtf8 :: FilePath -> T.Text -> IO ()
writeFileUtf8 f = BS.writeFile f . T.encodeUtf8
