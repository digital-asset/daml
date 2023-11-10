-- Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

module DA.Daml.Doc.Transform.Annotations
    ( applyAnnotations
    ) where

import DA.Daml.Doc.Types
import DA.Daml.Doc.Anchor

import Data.Text qualified as T
import Data.List.Extra
import Control.Applicative ((<|>))

-- | Apply HIDE and MOVE annotations.
applyAnnotations :: [ModuleDoc] -> [ModuleDoc]
applyAnnotations = applyMove . applyHide

-- | Apply the MOVE annotation, which moves all the docs from one
-- module to another.
applyMove :: [ModuleDoc] -> [ModuleDoc]
applyMove
    = map (foldr1 combineModules)
    . groupSortOn md_name
    . map renameModule
  where
    -- | Rename module according to its MOVE annotation, if present.
    -- If the module is renamed, we drop the rest of the module's
    -- description.
    renameModule :: ModuleDoc -> ModuleDoc
    renameModule md@ModuleDoc{..}
        | Just new <- isMove md_descr = md
            { md_name = new
            , md_anchor = Just (moduleAnchor new)
                -- Update the module anchor
            , md_descr = Nothing
                -- Drop the renamed module's description.
            }
        | otherwise = md

    -- | Combine two modules with the same name.
    combineModules :: ModuleDoc -> ModuleDoc -> ModuleDoc
    combineModules m1 m2 = ModuleDoc
        { md_anchor = md_anchor m1
        , md_name = md_name m1
        , md_descr = md_descr m1 <|> md_descr m2
            -- The renamed module's description was dropped,
            -- so in this line we always prefers the original
            -- module description.
        , md_adts = md_adts m1 ++ md_adts m2
        , md_functions = md_functions m1 ++ md_functions m2
        , md_templates = md_templates m1 ++ md_templates m2
        , md_interfaces = md_interfaces m1 ++ md_interfaces m2
        , md_classes = md_classes m1 ++ md_classes m2
        , md_instances = md_instances m1 ++ md_instances m2
        }

-- | Apply the HIDE annotation, which removes the current subtree from
-- the docs. This can be applied to an entire module, or to a specific
-- type, a constructor, a field, a class, a method, or a function.
applyHide :: [ModuleDoc] -> [ModuleDoc]
applyHide = concatMap onModule
    where
        onModule md@ModuleDoc{..}
            | isHide md_descr = []
            | otherwise = pure md
                    {md_templates = concatMap onTemplate md_templates
                    ,md_adts = concatMap onADT md_adts
                    ,md_functions = concatMap onFunction md_functions
                    ,md_classes = concatMap onClass md_classes
                    }

        -- be careful, we don't support hiding arbitrary bits within a data type or template
        -- as that would be a security risk
        onTemplate x = [x | not $ isHide $ td_descr x]
        onFunction x = [x | not $ isHide $ fct_descr x]
        onMethod x = [x | not $ isHide $ cm_descr x]
        onClass x
            | isHide $ cl_descr x = []
            | ClassDoc {..} <- x = [x { cl_methods = concatMap onMethod cl_methods }]

        onADT x
            | isHide $ ad_descr x = []
            | ADTDoc{..} <- x, all (isHide . ac_descr) ad_constrs = pure x{ad_constrs = []}
            | otherwise = [x]


getAnn :: Maybe DocText -> [T.Text]
getAnn = maybe [] (T.words . unDocText)

isHide :: Maybe DocText -> Bool
isHide x = ["HIDE"] `isPrefixOf` getAnn x

isMove :: Maybe DocText -> Maybe Modulename
isMove x = case getAnn x of
    "MOVE":y:_ -> Just (Modulename y)
    _ -> Nothing
