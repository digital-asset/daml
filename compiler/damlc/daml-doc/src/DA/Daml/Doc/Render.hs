-- Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

{-# LANGUAGE OverloadedStrings #-}

module DA.Daml.Doc.Render
  ( DocFormat(..)
  , renderPage
  , renderSimpleRst
  , renderSimpleMD
  , renderSimpleHtml
  , renderSimpleHoogle
  , jsonConf
  ) where

import DA.Daml.Doc.Render.Types
import DA.Daml.Doc.Render.Monoid
import DA.Daml.Doc.Render.Rst
import DA.Daml.Doc.Render.Markdown
import DA.Daml.Doc.Render.Hoogle
import DA.Daml.Doc.Types

import qualified CMarkGFM as GFM
import qualified Data.Aeson.Encode.Pretty as AP
import qualified Data.Text as T
import qualified Data.Text.Lazy as TL
import qualified Text.Blaze.Html5 as H
import qualified Text.Blaze.Html.Renderer.Text as H


-- | centralised JSON configuration for pretty-printing
jsonConf :: AP.Config
jsonConf = AP.Config (AP.Spaces 2) (AP.keyOrder ["id"]) AP.Generic True

-- | Html renderer, using cmark-gfm
renderSimpleHtml :: ModuleDoc -> T.Text
renderSimpleHtml m@ModuleDoc{..} =
  wrapHtml t $ GFM.commonmarkToHtml [] [GFM.extTable] $ renderPage $ renderSimpleMD m
  where t = "Module " <> unModulename md_name

wrapHtml :: T.Text -> T.Text -> T.Text
wrapHtml pageTitle body =
  let html = do
        H.head (H.title $ H.toHtml pageTitle)
        H.body $ H.preEscapedToHtml body
  in TL.toStrict $ H.renderHtml html
