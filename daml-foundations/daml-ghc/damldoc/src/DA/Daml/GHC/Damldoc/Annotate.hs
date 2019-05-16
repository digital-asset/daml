-- Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

{-# LANGUAGE OverloadedStrings #-}

module DA.Daml.GHC.Damldoc.Annotate(applyAnnotations) where

import           DA.Daml.GHC.Damldoc.Types
import qualified Data.Text as T
import           Data.List.Extra


-- | Apply the annotation HIDE to hide either modules or declarations
applyAnnotations :: [ModuleDoc] -> [ModuleDoc]
applyAnnotations = applyMove . applyHide

applyMove :: [ModuleDoc] -> [ModuleDoc]
applyMove = map (foldr1 g) . groupSortOn (modulePriorityKey . md_name) . map f
    where
        f md@ModuleDoc{..}
            | Just new <- isMove md_descr = md{md_name = new, md_descr = Nothing}
            | otherwise = md

        g m1 m2 = m1{md_adts = md_adts m1 ++ md_adts m2
                    ,md_functions = md_functions m1 ++ md_functions m2
                    ,md_templates = md_templates m2 ++ md_templates m2
                    ,md_classes = md_classes m1 ++ md_classes m2
                    }

        -- Bring Prelude module to the front.
        modulePriorityKey :: Modulename -> (Int,Modulename)
        modulePriorityKey m = (if m == "Prelude" then 0 else 1, m)

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
        onClass x
            | isHide $ cl_descr x = []
            | ClassDoc {..} <- x = [x { cl_functions = concatMap onFunction cl_functions }]

        onADT x
            | isHide $ ad_descr x = []
            | ADTDoc{..} <- x, all (isHide . ac_descr) ad_constrs = pure x{ad_constrs = []}
            | otherwise = [x]


getAnn :: Maybe Markdown -> [T.Text]
getAnn = maybe [] T.words

isHide :: Maybe Markdown -> Bool
isHide x = ["HIDE"] `isPrefixOf` getAnn x

isMove :: Maybe Markdown -> Maybe T.Text
isMove x = case getAnn x of
    "MOVE":y:_ -> Just y
    _ -> Nothing
