-- Copyright (c) 2019 The DAML Authors. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

module DA.Ledger.Jwt(
  Jwt,
  tryCreateFromString,
  toString
  ) where

import Data.List.Extra (splitOn)
import Data.String.Utils (strip)

newtype Jwt = Jwt { toString :: String }

tryCreateFromString :: String -> Either String Jwt
tryCreateFromString s0 = do
  let s = strip s0
  validate3parts s
  return $ Jwt s

-- | catch easy user errors before sending the token to the ledger
validate3parts :: String -> Either String ()
validate3parts s = do
  case splitOn "." s of
    [_, _, _] -> return ()
    parts -> Left $ "Bad JWT token: The token was expected to have 3 parts, but got "
             <> show (length parts)
