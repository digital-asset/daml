-- Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

{-# LANGUAGE OverloadedStrings #-}

module DA.Daml.Doc.Render
  ( DocFormat(..)
  , RenderOptions(..)
  , RenderMode(..)
  , renderDocs
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

import Data.Maybe
import Data.List.Extra
import Data.Foldable
import System.Directory
import System.FilePath

import qualified CMarkGFM as GFM
import qualified Data.Aeson.Encode.Pretty as AP
import qualified Data.Text as T
import qualified Data.Text.Encoding as T
import qualified Data.ByteString as BS
import qualified Data.Map.Strict as Map
import qualified Data.Text.Lazy as TL
import qualified Text.Blaze.Html5 as H
import qualified Text.Blaze.Html.Renderer.Text as H


-- | centralised JSON configuration for pretty-printing
jsonConf :: AP.Config
jsonConf = AP.Config (AP.Spaces 2) (AP.keyOrder ["id"]) AP.Generic True

-- | Html renderer, using cmark-gfm
renderSimpleHtml :: ModuleDoc -> T.Text
renderSimpleHtml m@ModuleDoc{..} =
  wrapHtml t $ GFM.commonmarkToHtml [GFM.optUnsafe] [GFM.extTable] $ renderPage $ renderSimpleMD m
  where t = "Module " <> unModulename md_name

wrapHtml :: T.Text -> T.Text -> T.Text
wrapHtml pageTitle body =
  let html = do
        H.head (H.title $ H.toHtml pageTitle)
        H.body $ H.preEscapedToHtml body
  in TL.toStrict $ H.renderHtml html

renderDocs :: RenderOptions -> [ModuleDoc] -> IO ()
renderDocs RenderOptions{..} mods = do
    let renderModule =
            case ro_format of
                Json -> const (renderLine "") -- not implemented (yet?)
                Hoogle -> const (renderLine "") -- not implemented (yet?)
                Rst -> renderSimpleRst
                Markdown -> renderSimpleMD
                Html -> renderSimpleMD
        postProcessing =
            case ro_format of
                Html -> GFM.commonmarkToHtml [GFM.optUnsafe] [GFM.extTable]
                _ -> id
        template = fromMaybe (defaultTemplate ro_format) ro_template

    case ro_mode of
        RenderToFile path -> do
            BS.writeFile path
                . T.encodeUtf8
                . renderTemplate template
                    (fromMaybe "Package Docs" ro_title)
                . postProcessing
                . renderPage
                $ mconcatMap renderModule mods

        RenderToFolder path -> do
            let renderMap = Map.fromList
                    [(md_name mod, renderModule mod) | mod <- mods]
                outputMap = renderFolder renderMap
                extension =
                    case ro_format of
                        Json -> "json"
                        Hoogle -> "txt"
                        Markdown -> "md"
                        Rst -> "rst"
                        Html -> "html"

                outputPath mod = path </> moduleNameToFileName mod <.> extension
                pageTitle mod = T.concat
                    [ maybe "" (<> " - ") ro_title
                    , "Module "
                    , unModulename mod ]

            createDirectoryIfMissing True path
            for_ (Map.toList outputMap) $ \ (mod, renderedOutput) -> do
                BS.writeFile (outputPath mod)
                    . T.encodeUtf8
                    . renderTemplate template (pageTitle mod)
                    . postProcessing
                    $ renderedOutput


renderTemplate ::
    T.Text -- ^ template
    -> T.Text -- ^ page title
    -> T.Text -- ^ page body
    -> T.Text
renderTemplate template pageTitle pageBody
    = T.replace "__BODY__" pageBody
    . T.replace "__TITLE__" pageTitle
    $ template

defaultTemplate :: DocFormat -> T.Text
defaultTemplate = \case
    Html -> defaultTemplateHtml
    _ -> "__BODY__"

defaultTemplateHtml :: T.Text
defaultTemplateHtml = T.unlines
    [ "<html>"
    , "<head><title>__TITLE__</title><meta charset=\"utf-8\"></head>"
    , "<body>__BODY__</body>"
    , "</html>"
    ]
