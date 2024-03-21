-- Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
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
        , section "Interfaces" md_interfaces
        , section "Typeclasses" md_classes
        , section "Orphan Typeclass Instances" (filter id_isOrphan md_instances)
        , section "Data Types" md_adts
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
maybeAnchorLink = maybe RenderPlain (RenderLink . Reference Nothing)

maybeReferenceLink :: Maybe Reference -> T.Text -> RenderText
maybeReferenceLink = maybe RenderPlain RenderLink

instance RenderDoc TemplateDoc where
    renderDoc TemplateDoc{..} = mconcat
        [ renderDoc td_anchor
        , RenderParagraph . renderUnwords $
            [ RenderStrong "template"
            , maybeAnchorLink td_anchor (unTypename td_name)
            ]
        , RenderBlock $ mconcat
            [ renderDoc td_descr
            , fieldTable td_payload
            , RenderList (map renderDoc td_choices)
            ]
        , if null td_impls
          then mempty
          else RenderBlock $ mconcat
                [ RenderList (map renderDoc td_impls)
                ]
        ]

instance RenderDoc InterfaceDoc where
    renderDoc InterfaceDoc{..} = mconcat
        [ renderDoc if_anchor
        , RenderParagraph . renderUnwords . concat $
            [ [RenderStrong "interface"]
            , [maybeAnchorLink if_anchor (unTypename if_name)]
            ]
        , RenderBlock $ mconcat
            [ renderDoc if_descr
            , RenderList (map renderDoc if_choices)
            , RenderList (map renderDoc if_methods)
            ]
        ]

instance RenderDoc ImplDoc where
    renderDoc ImplDoc {..} =
        RenderParagraph $
            renderUnwords [ RenderStrong "implements", renderType impl_iface ]

instance RenderDoc MethodDoc where
    renderDoc MethodDoc {..} = mconcat
      [ RenderParagraph $ RenderStrong ("Method " <> unTypename mtd_name <> " : ") <> renderType mtd_type
      , renderDoc mtd_descr
      ]

instance RenderDoc ChoiceDoc where
    renderDoc ChoiceDoc{..} = mconcat
        [ RenderParagraph $ RenderStrong ("Choice " <> unTypename cd_name)
        , renderDoc cd_descr
        , fieldTable cd_fields
        ]

instance RenderDoc ClassDoc where
    renderDoc ClassDoc{..} = mconcat
        [ renderDoc cl_anchor
        , RenderParagraph . renderUnwords . concat $
            [ [RenderStrong "class"]
            , renderContext cl_super
            , [maybeAnchorLink cl_anchor (unTypename cl_name)]
            , map RenderPlain cl_args
            , [RenderStrong "where"]
            ]
        , RenderBlock $ mconcat
            [ renderDoc cl_descr
            , renderDoc cl_methods
            , renderDoc cl_instances
            ]
        ]

instance RenderDoc ClassMethodDoc where
    renderDoc ClassMethodDoc{..} = mconcat
        [ renderDoc cm_anchor
        , RenderParagraph . renderUnwords . concat $
            [ [ RenderStrong "default" | cm_isDefault ]
            , [ maybeAnchorLink cm_anchor (wrapOp (unFieldname cm_name)) ]
            ]
        , RenderBlock $ mconcat
            [ RenderParagraph . renderUnwords . concat $
                [ [RenderPlain ":"]
                , renderContext cm_localContext
                    -- TODO: use localContext only when rendering inside ClassDoc,
                    -- otherwise use globalContext
                , [renderType cm_type]
                ]
            , renderDoc cm_descr
            ]
        ]

instance RenderDoc ADTDoc where
    renderDoc TypeSynDoc{..} = mconcat
        [ renderDoc ad_anchor
        , RenderParagraph . renderUnwords
            $ RenderStrong "type"
            : maybeAnchorLink ad_anchor (unTypename ad_name)
            : map RenderPlain ad_args
        , RenderBlock $ mconcat
            [ RenderParagraph $ renderUnwords
                [ RenderPlain "="
                , renderType ad_rhs
                ]
            , renderDoc ad_descr
            , renderDoc ad_instances
            ]
        ]

    renderDoc ADTDoc{..} = mconcat
        [ renderDoc ad_anchor
        , RenderParagraph . renderUnwords
            $ RenderStrong "data"
            : maybeAnchorLink ad_anchor (unTypename ad_name)
            : map RenderPlain ad_args
        , RenderBlock $ mconcat
            [ renderDoc ad_descr
            , renderDoc ad_constrs
            , renderDoc ad_instances
            ]
        ]

instance RenderDoc ADTConstr where
    renderDoc PrefixC{..} = mconcat
        [ renderDoc ac_anchor
        , RenderParagraph . renderUnwords
            $ maybeAnchorLink ac_anchor (wrapOp (unTypename ac_name))
            : map (renderTypePrec 2) ac_args
        , RenderBlock (renderDoc ac_descr)
        ]

    renderDoc RecordC{..} = mconcat
        [ renderDoc ac_anchor
        , RenderParagraph
            $ maybeAnchorLink ac_anchor (unTypename ac_name)
        , RenderBlock $ mconcat
            [ renderDoc ac_descr
            , fieldTable ac_fields
            ]
        ]

instance RenderDoc FunctionDoc where
    renderDoc FunctionDoc{..} = mconcat
        [ renderDoc fct_anchor
        , RenderParagraph
            $ maybeAnchorLink fct_anchor
                (wrapOp (unFieldname fct_name))
        , RenderBlock $ mconcat
            [ RenderParagraph . renderUnwords . concat $
                [ [RenderPlain ":"]
                , renderContext fct_context
                , [renderType fct_type]
                ]
            , renderDoc fct_descr
            ]
        ]

instance RenderDoc InstanceDoc where
    renderDoc InstanceDoc{..} =
        RenderParagraph . renderUnwords . concat $
            [ [RenderStrong "instance"]
            , renderContext id_context
            , [renderType id_type]
            ]

fieldTable :: [FieldDoc] -> RenderOut
fieldTable fields = RenderRecordFields
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
    TypeApp referenceM (Typename typename) args ->
        (if prec >= 2 && notNull args then renderInParens else id)
            . renderUnwords
            $ maybeReferenceLink referenceM (wrapOp typename)
            : map (renderTypePrec 2) args
    TypeFun ts ->
        (if prec >= 1 then renderInParens else id)
            . renderIntercalate " -> "
            $ map (renderTypePrec 1) ts
    TypeList t ->
        renderEnclose "[" "]" (renderTypePrec 0 t)
    TypeTuple ts ->
        renderTypeTuplePrec prec ts
    TypeLit lit ->
        RenderPlain lit

renderTypeTuplePrec :: Int -> [Type] -> RenderText
renderTypeTuplePrec prec = \case
    [t] -> renderTypePrec prec t
    ts -> renderInParens
            . renderIntercalate ", "
            $ map (renderTypePrec 0) ts

-- | Render type context as a list of words. Nothing is rendered as [],
-- and Just t is rendered as [render t, "=>"].
renderContext :: Context -> [RenderText]
renderContext = \case
    Context [] -> []
    Context ts -> [renderTypeTuplePrec 1 ts, RenderPlain "=>"]

renderInParens :: RenderText -> RenderText
renderInParens = renderEnclose "(" ")"

renderEnclose :: T.Text -> T.Text -> RenderText -> RenderText
renderEnclose lp rp t = RenderConcat
    [ RenderPlain lp
    , t
    , RenderPlain rp
    ]
