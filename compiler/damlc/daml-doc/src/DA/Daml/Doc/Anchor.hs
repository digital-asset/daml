-- Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0


-- | Generate anchor names to link between Hoogle and Rst docs. These need to be
-- unique, and conform to the Rst's restrictions on anchor names (AFAICT they have to
-- satisfy the regex /[a-z0-9]+(-[a-z0-9]+)*/). It's also nice for them to be readable.
-- So we generate a human readable tag, and append a hash to guarantee uniqueness.

module DA.Daml.Doc.Anchor
    ( Anchor
    , moduleAnchor
    , classAnchor
    , typeAnchor
    , constrAnchor
    , functionAnchor
    ) where

import DA.Daml.Doc.Types
import Data.Hashable
import qualified Data.Text as T
import qualified Data.Char as C

moduleAnchor :: Modulename -> Anchor
-- calculating a hash on String instead of Data.Text as hash output of the later is different on Windows than other OSes
moduleAnchor m = Anchor $ T.intercalate "-" ["module", convertModulename m, hashText . T.unpack . unModulename $ m]

convertModulename :: Modulename -> T.Text
convertModulename = T.toLower . T.replace "." "-" . T.replace "_" "" . unModulename

classAnchor    :: Modulename -> Typename  -> Anchor
typeAnchor     :: Modulename -> Typename  -> Anchor
constrAnchor   :: Modulename -> Typename  -> Anchor
functionAnchor :: Modulename -> Fieldname -> Anchor

classAnchor    m n = anchor "class"    m (unTypename n) ()
typeAnchor     m n = anchor "type"     m (unTypename n) ()
constrAnchor   m n = anchor "constr"   m (unTypename n) ()
functionAnchor m n = anchor "function" m (unFieldname n) ()

anchor :: Hashable v => T.Text -> Modulename -> T.Text -> v -> Anchor
-- calculating a hash on String instead of Data.Text as hash output of the later is different on Windows than other OSes
anchor k m n v = Anchor $ T.intercalate "-" [k, convertModulename m, expandOps n, hashText (T.unpack k, T.unpack (unModulename m), T.unpack n, v)]
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
