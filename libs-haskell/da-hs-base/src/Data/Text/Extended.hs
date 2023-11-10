-- Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

-- | An extended version of "Data.Text", which provides a slightly more
-- extensive export lists and missing orphan instances.
module Data.Text.Extended
  (
    module Data.Text

  , Data.Text.Extended.show
  , Data.Text.Extended.splitOn

  , readFileUtf8
  , writeFileUtf8
  ) where

import Data.ByteString qualified as BS
import Data.List.NonEmpty as NE

import Data.Text qualified as T
import Data.Text.Encoding qualified as T
import Data.Text.Encoding.Error qualified as T
import Data.Text hiding (splitOn)


-- | 'Show' a value and pack it into a strict 'Text' value.
show :: Show a => a -> Text
show = pack . Prelude.show

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

-- | Read a file as UTF-8.
readFileUtf8 :: FilePath -> IO T.Text
readFileUtf8 f = T.decodeUtf8With T.lenientDecode <$> BS.readFile f

-- | Write a file as UTF-8.
writeFileUtf8 :: FilePath -> T.Text -> IO ()
writeFileUtf8 f = BS.writeFile f . T.encodeUtf8
