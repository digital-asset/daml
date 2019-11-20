-- Copyright (c) 2019 The DAML Authors. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

module DA.Ledger.Jwt(
  Jwt,
  tryCreateFromString,
  toString
  ) where

import Data.List.Extra (splitOn)

newtype Jwt = Jwt { toString :: String }

tryCreateFromString :: String -> Either String Jwt
tryCreateFromString s = do
  validate3parts s
  return $ Jwt s

validate3parts :: String -> Either String ()
validate3parts s = do
  case length (splitOn "." s) of
    3 -> return ()
    n -> Left $ "Bad JWT token: The token was expected to have 3 parts, but got " <> show n
