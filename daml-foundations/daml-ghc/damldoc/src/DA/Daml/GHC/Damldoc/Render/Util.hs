-- Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

{-# LANGUAGE OverloadedStrings #-}

module DA.Daml.GHC.Damldoc.Render.Util
  ( adjust
  , prefix
  , indent
  , enclosedIn
  , inParens
  , wrapOp
  ) where

import qualified Data.Text as T

-- | Puts text between (round) parentheses
inParens :: T.Text -> T.Text
inParens t = "(" <> t <> ")"

-- | Surrounds text in 2nd argument by text in the 1st
enclosedIn :: T.Text -> T.Text -> T.Text
enclosedIn c t = T.concat [c, t, c]

-- | Indents all lines in Text by n spaces
indent :: Int -> T.Text -> T.Text
indent n t = prefix (T.pack $ replicate n ' ') t

-- | Prefixes all lines of the 2nd argument by the 1st
prefix :: T.Text -> T.Text -> T.Text
prefix p = T.stripEnd . T.unlines . map (p <>) . T.lines

-- | Right-adjusts all lines of the 2nd argument with spaces to be of length n
adjust :: Int -> T.Text -> T.Text
adjust n t | l < n  = t <> T.replicate (n - l) " "
           | l == n = t
           | otherwise  = t -- error?
  where l = T.length t

-- | If name is an operator, wrap it.
wrapOp :: T.Text -> T.Text
wrapOp t =
    if T.null t || isIdChar (T.head t)
        then t
        else inParens t
  where
    isIdChar :: Char -> Bool
    isIdChar c = ('A' <= c && c <= 'Z')
              || ('a' <= c && c <= 'z')
              || ('_' == c)
