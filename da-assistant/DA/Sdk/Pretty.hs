-- Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

{-# LANGUAGE NoImplicitPrelude #-}
{-# LANGUAGE OverloadedStrings #-}

-- | Pretty printing module with "batteries included".
-- Meant to @import qualified DA.Sdk.Pretty as P@.
module DA.Sdk.Pretty
    ( Pretty (..)

      -- * Defaults
    , defaultTermWidth

      -- * Pretty printing
    , p
    , mb
    , s
    , t
    , e
    , w
    , label

      -- * Rendering
    , renderPlain
    , renderPlainWidth

    , table

      -- * Re-exports
    , module P
    ) where

import           DA.Sdk.Prelude hiding ((<>))
import qualified Data.Text                             as T
import           Data.Text.Prettyprint.Doc             as P
import           Data.Text.Prettyprint.Doc.Render.Text as P
import           Data.Text.Prettyprint.Doc.Util        as P

--------------------------------------------------------------------------------
-- Defaults
--------------------------------------------------------------------------------

defaultTermWidth :: Int
defaultTermWidth = 80

--------------------------------------------------------------------------------
-- Pretty Printing
--------------------------------------------------------------------------------

p :: FilePath -> Doc ann
p = P.pretty . pathToText

mb :: Maybe a -> (a -> Doc ann) -> Doc ann
mb m prettify = maybe mempty prettify m

s :: String -> Doc ann
s = pretty

t :: Text -> Doc ann
t = pretty

-- | Something with a label.
-- If the document doesn't fit onto the same line as the label, if will be indented.
label :: Text -> Doc ann -> Doc ann
label labelText doc = P.nest 4 $ P.sep
    [ pretty labelText <> s ":"
    , doc
    ]

-- | Pretty print an error.
e :: Doc ann -> Doc ann
e = label "Error"

-- | Pretty print a warning.
w :: Doc ann -> Doc ann
w = label "Warning"

--------------------------------------------------------------------------------
-- Rendering
--------------------------------------------------------------------------------

-- | The layout options used for the SDK assistant.
cliLayout ::
       Int
    -- ^ Rendering width of the pretty printer.
    -> LayoutOptions
cliLayout renderWidth = P.LayoutOptions
    { layoutPageWidth = AvailablePerLine renderWidth 0.9
    }

-- | Render without any syntax annotations
renderPlain :: Doc ann -> T.Text
renderPlain = P.renderStrict . P.layoutSmart (cliLayout defaultTermWidth)

-- | Render with no syntax annotations within a maximal width.
renderPlainWidth :: Int -> Doc ann -> T.Text
renderPlainWidth renderWidth = P.renderStrict . P.layoutSmart (cliLayout renderWidth)

table :: [[T.Text]] -> T.Text
table lst@(l:_ls) = T.intercalate "\n" $ map (\r -> T.intercalate "  " r) listWpadding
  where
    listWpadding = map addRowPadd lst
    addRowPadd row = map (\(n, txt) -> withPadding n txt) $ zip maxLens row
    maxLens = foldr (zipWith (\x z -> T.length x `max` z)) (replicate (length l) 0) lst
table [] = ""

withPadding :: Int -> T.Text -> T.Text
withPadding num txt = txt <> T.replicate i " "
  where
    i = num - T.length txt
