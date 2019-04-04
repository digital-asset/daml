-- Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

{-# LANGUAGE OverloadedStrings #-}

{-# OPTIONS_GHC -Wno-orphans #-}

-- | Orphans instances for classes defined in the 'aeson' library.
module Orphans.Lib_aeson () where

import           Data.Aeson
import qualified Data.ByteString            as BS
import qualified Data.ByteString.Char8      as B8
import qualified Data.ByteString.Base16     as B16

-- bytestring

instance ToJSON BS.ByteString where
    toJSON = toJSON . B8.unpack . B16.encode

instance FromJSON BS.ByteString where
    parseJSON src = do
      str <- parseJSON src
      let (decoded, rest) = B16.decode $ B8.pack str
      if BS.null rest
        then return decoded
        else fail $ "Failed to parse hex: " ++ B8.unpack rest

instance FromJSONKey BS.ByteString where
    fromJSONKey = FromJSONKeyValue parseJSON

instance ToJSONKey BS.ByteString where
    toJSONKey = ToJSONKeyValue toJSON toEncoding
