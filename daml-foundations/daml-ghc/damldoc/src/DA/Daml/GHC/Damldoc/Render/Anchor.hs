-- Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

{-# LANGUAGE OverloadedStrings #-}

-- | Generate anchor names to link between Hoogle and Rst docs. These need to be
-- unique, and conform to the Rst's restrictions on anchor names (AFAICT they have to
-- satisfy the regex /[a-z0-9]+(-[a-z0-9]+)*/). It's also nice for them to be readable.
-- So we generate a human readable tag, and append a hash to guarantee uniqueness.

module DA.Daml.GHC.Damldoc.Render.Anchor
    ( Anchor
    , moduleAnchor
    , classAnchor
    , templateAnchor
    , typeAnchor
    , dataAnchor
    , constrAnchor
    , functionAnchor
    ) where

import DA.Daml.GHC.Damldoc.Types
import Data.Hashable
import qualified Data.Text as T
import qualified Data.Char as C

type Anchor = T.Text

moduleAnchor :: Modulename -> Anchor
moduleAnchor m = T.intercalate "-" ["module", convertModulename m, hashText m]

convertModulename :: Modulename -> T.Text
convertModulename = T.toLower . T.replace "." "-" . T.replace "_" ""

classAnchor    :: Modulename -> Typename  -> Anchor
templateAnchor :: Modulename -> Typename  -> Anchor
typeAnchor     :: Modulename -> Typename  -> Anchor
dataAnchor     :: Modulename -> Typename  -> Anchor
constrAnchor   :: Modulename -> Typename  -> Anchor
functionAnchor :: Modulename -> Fieldname -> Maybe Type -> Anchor

classAnchor    m n = anchor "class"    m n ()
templateAnchor m n = anchor "template" m n ()
typeAnchor     m n = anchor "type"     m n ()
dataAnchor     m n = anchor "data"     m n ()
constrAnchor   m n = anchor "constr"   m n ()
functionAnchor     = anchor "function"


anchor :: Hashable v => T.Text -> Modulename -> T.Text -> v -> Anchor
anchor k m n v = T.intercalate "-" [k, convertModulename m, expandOps n, hashText (k,m,n,v)]
  where
    expandOps :: T.Text -> T.Text
    expandOps = T.pack . replaceEmpty . concatMap expandOp . T.unpack

    -- On the off chance `concatMap expandOp` results in an empty string, p
    -- put something in here to satisfy the anchor regex.
    replaceEmpty :: String -> String
    replaceEmpty "" = "x"
    replaceEmpty x = x

    expandOp :: Char -> String
    expandOp = \case
      '.' -> "dot"
      '+' -> "plus"
      '-' -> "dash"
      '*' -> "star"
      '/' -> "slash"
      '>' -> "gt"
      '<' -> "lt"
      '=' -> "eq"
      '^' -> "hat"
      '&' -> "amp"
      '|' -> "pipe"
      '#' -> "hash"
      '?' -> "what"
      '!' -> "bang"
      '\'' -> "tick"
      ':' -> "colon"
      ',' -> "comma"
      '$' -> "dollar"
      c | '0' <= c && c <= '9' -> [c]
      c | 'a' <= c && c <= 'z' -> [c]
      c | 'A' <= c && c <= 'Z' -> [C.toLower c]
      _ -> ""

hashText :: Hashable v => v -> T.Text
hashText = T.pack . show . (`mod` 100000) . hash
