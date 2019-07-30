-- Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

{-# LANGUAGE DerivingStrategies #-}

module DA.Daml.Doc.Render.Output
  ( renderModule
  ) where

import DA.Daml.Doc.Types
import DA.Daml.Doc.Render.Util (wrapOp)
import DA.Daml.Doc.Render.Monoid

import Data.List.Extra
import Data.Maybe
import qualified Data.Text as T

renderModule :: ModuleDoc -> RenderOut
renderModule = renderDoc

class RenderDoc t where
    renderDoc :: t -> RenderOut

instance RenderDoc Anchor where renderDoc = RenderAnchor
instance RenderDoc DocText where renderDoc = RenderDocs
instance RenderDoc t => RenderDoc (Maybe t) where
    renderDoc = maybe mempty renderDoc
instance RenderDoc t => RenderDoc [t] where
    renderDoc = mconcatMap renderDoc

isModuleEmpty :: ModuleDoc -> Bool
isModuleEmpty ModuleDoc{..} =
    null md_templates && null md_classes &&
    null md_adts && null md_functions &&
    isNothing md_descr

instance RenderDoc ModuleDoc where
    renderDoc m | isModuleEmpty m = mempty
    renderDoc ModuleDoc{..} = mconcat
        [ renderDoc md_anchor
        , RenderModuleHeader ("Module " <> unModulename md_name)
        , renderDoc md_descr
        , section "Templates" md_templates
        , section "Template Instances" md_templateInstances
        , section "Typeclasses" md_classes
        , section "Data types" md_adts
        , section "Functions" md_functions
        ]
      where
        section :: RenderDoc t => T.Text -> [t] -> RenderOut
        section _ [] = mempty
        section title xs = mconcat
            [ RenderSectionHeader title
            , renderDoc xs
            ]


maybeAnchorLink :: Maybe Anchor -> T.Text -> RenderText
maybeAnchorLink = maybe RenderPlain RenderLink

instance RenderDoc TemplateDoc where
    renderDoc TemplateDoc{..} = mconcat
        [ renderDoc td_anchor
        , RenderPara . RenderUnwords . concat $
            [ [RenderStrong "template "]
            , renderContext td_super
            , [maybeAnchorLink td_anchor (unTypename td_name)]
            , map RenderPlain td_args
            ]
        , RenderBlock $ mconcat
            [ renderDoc td_descr
            , RenderList (map renderDoc td_choices)
            ]
        ]

instance RenderDoc ChoiceDoc where
    renderDoc ChoiceDoc{..} = mconcat
        [ RenderPara $ RenderStrong ("Choice " <> unTypename cd_name)
        , renderDoc cd_descr
        , fieldTable cd_fields
        ]

instance RenderDoc TemplateInstanceDoc where
    renderDoc TemplateInstanceDoc{..} = mconcat
        [ renderDoc ti_anchor
        , RenderPara . RenderUnwords $
            [ RenderStrong "template instance"
            , maybeAnchorLink ti_anchor (unTypename ti_name)
            ]
        , RenderBlock $ mconcat
            [ RenderPara $ RenderUnwords
                [ RenderPlain "="
                , renderType ti_rhs
                ]
            , renderDoc ti_descr
            ]
        ]

instance RenderDoc ClassDoc where
    renderDoc ClassDoc{..} = mconcat
        [ renderDoc cl_anchor
        , RenderPara . RenderUnwords . concat $
            [ [RenderStrong "class"]
            , renderContext cl_super
            , [maybeAnchorLink cl_anchor (unTypename cl_name)]
            , map RenderPlain cl_args
            , [RenderStrong "where"]
            ]
        , RenderBlock $ mconcat
            [ renderDoc cl_descr
            , renderDoc cl_functions
            ]
        ]

instance RenderDoc ADTDoc where
    renderDoc TypeSynDoc{..} = mconcat
        [ renderDoc ad_anchor
        , RenderPara . RenderUnwords
            $ RenderStrong "type"
            : maybeAnchorLink ad_anchor (unTypename ad_name)
            : map RenderPlain ad_args
        , RenderBlock $ mconcat
            [ RenderPara $ RenderUnwords
                [ RenderPlain "="
                , renderType ad_rhs
                ]
            , renderDoc ad_descr
            ]
        ]

    renderDoc ADTDoc{..} = mconcat
        [ renderDoc ad_anchor
        , RenderPara . RenderUnwords
            $ RenderStrong "data"
            : maybeAnchorLink ad_anchor (unTypename ad_name)
            : map RenderPlain ad_args
        , RenderBlock $ mconcat
            [ renderDoc ad_descr
            , renderDoc ad_constrs
            ]
        ]

instance RenderDoc ADTConstr where
    renderDoc PrefixC{..} = mconcat
        [ renderDoc ac_anchor
        , RenderPara . RenderUnwords
            $ maybeAnchorLink ac_anchor (wrapOp (unTypename ac_name))
            : map (renderTypePrec 2) ac_args
        , RenderBlock (renderDoc ac_descr)
        ]

    renderDoc RecordC{..} = mconcat
        [ renderDoc ac_anchor
        , RenderPara
            $ maybeAnchorLink ac_anchor (unTypename ac_name)
        , RenderBlock $ mconcat
            [ renderDoc ac_descr
            , fieldTable ac_fields
            ]
        ]

instance RenderDoc FunctionDoc where
    renderDoc FunctionDoc{..} = mconcat
        [ renderDoc fct_anchor
        , RenderPara
            $ maybeAnchorLink fct_anchor
                (wrapOp (unFieldname fct_name))
        , RenderBlock $ mconcat
            [ RenderPara . RenderUnwords . concat $
                [ [RenderPlain ":"]
                , renderContext fct_context
                , [renderType fct_type]
                ]
            , renderDoc fct_descr
            ]
        ]

fieldTable :: [FieldDoc] -> RenderOut
fieldTable fields = RenderFields
    [ ( RenderPlain (unFieldname fd_name)
      , renderType fd_type
      , maybe mempty RenderDocsInline fd_descr
      )
    | FieldDoc{..} <- fields
    ]

-- | Render type at precedence level 0.
renderType :: Type -> RenderText
renderType = renderTypePrec  0

-- | Render a type at a given precedence level. The precedence
-- level controls whether parentheses will be placed around the
-- type or not. Thus:
--
-- * precedence 0: no brackets
-- * precedence 1: brackets around function types
-- * precedence 2: brackets around function types and type application
renderTypePrec :: Int -> Type -> RenderText
renderTypePrec prec = \case
    TypeApp anchorM (Typename typename) args ->
        (if prec >= 2 && notNull args then inParens else id)
            . RenderUnwords
            $ maybeAnchorLink anchorM typename
            : map (renderTypePrec 2) args
    TypeFun ts ->
        (if prec >= 1 then inParens else id)
            . RenderIntercalate " -> "
            $ map (renderTypePrec 1) ts
    TypeList t ->
        enclose "[" "]" (renderTypePrec 0 t)
    TypeTuple [t] ->
        renderTypePrec prec t
    TypeTuple ts ->
        inParens
            . RenderIntercalate ", "
            $ map (renderTypePrec 0) ts

-- | Render type context as a list of words. Nothing is rendered as [],
-- and Just t is rendered as [render t, "=>"].
renderContext :: Maybe Type -> [RenderText]
renderContext = maybe [] (\x -> [renderTypePrec 1 x, RenderPlain "=>"])

inParens :: RenderText -> RenderText
inParens = enclose "(" ")"

enclose :: T.Text -> T.Text -> RenderText -> RenderText
enclose lp rp t = RenderConcat
    [ RenderPlain lp
    , t
    , RenderPlain rp
    ]
