-- Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0


module DA.Daml.Doc.Render
  ( RenderFormat(..)
  , RenderOptions(..)
  , RenderMode(..)
  , renderDocs
  , renderPage
  , jsonConf
  ) where

import DA.Daml.Doc.Render.Types
import DA.Daml.Doc.Render.Monoid
import DA.Daml.Doc.Render.Rst
import DA.Daml.Doc.Render.Markdown
import DA.Daml.Doc.Render.Hoogle
import DA.Daml.Doc.Render.Output
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


-- | centralised JSON configuration for pretty-printing
jsonConf :: AP.Config
jsonConf = AP.Config (AP.Spaces 2) (AP.keyOrder ["id"]) AP.Generic True

renderDocs :: RenderOptions -> [ModuleDoc] -> IO ()
renderDocs RenderOptions{..} mods = do
    let (formatter, postProcessing) =
            case ro_format of
                Rst -> (renderRst, id)
                Markdown -> (renderMd, id)
                Html -> (renderMd, GFM.commonmarkToHtml [GFM.optUnsafe] [GFM.extTable])
        template = fromMaybe (defaultTemplate ro_format) ro_template

    case ro_mode of
        RenderToFile path -> do
            BS.writeFile path
                . T.encodeUtf8
                . renderTemplate template
                    (fromMaybe "Package Docs" ro_title)
                . postProcessing
                . renderPage formatter
                $ mconcatMap renderModule mods

        RenderToFolder path -> do
            let renderMap = Map.fromList
                    [(md_name mod, renderModule mod) | mod <- mods]
                outputMap = renderFolder formatter renderMap
                extension =
                    case ro_format of
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

defaultTemplate :: RenderFormat -> T.Text
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
