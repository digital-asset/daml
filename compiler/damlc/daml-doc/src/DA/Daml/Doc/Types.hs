-- Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

{-# LANGUAGE DerivingStrategies #-}

module DA.Daml.Doc.Types(
    module DA.Daml.Doc.Types
    ) where

import Data.Aeson
import Data.Text (Text)
import qualified Data.Text as T
import Data.Hashable
import GHC.Generics
import Data.String
import qualified Data.HashMap.Strict as HMS

-- | Doc text type, presumably Markdown format.
newtype DocText = DocText { unDocText :: Text }
    deriving newtype (Eq, Ord, Show, ToJSON, FromJSON, IsString)

-- | Field name, starting with lowercase
newtype Fieldname = Fieldname { unFieldname :: Text }
    deriving newtype (Eq, Ord, Show, ToJSON, FromJSON, IsString)

-- | Type name starting with uppercase
newtype Typename = Typename { unTypename :: Text }
    deriving newtype (Eq, Ord, Show, ToJSON, FromJSON, IsString)

-- | Module name, starting with uppercase, may have dots.
newtype Modulename = Modulename { unModulename :: Text }
    deriving newtype (Eq, Show, ToJSON, FromJSON, IsString)

-- | Ord instance for Modulename. We want to always sort "Prelude" in front of
-- other modules, then "DA.*" modules, and then any remaining modules.
instance Ord Modulename where
    compare a b = compare (moduleKey a, unModulename a) (moduleKey b, unModulename b)
        where
            moduleKey :: Modulename -> Int
            moduleKey = \case
                Modulename "Prelude" -> 0
                Modulename m
                    | "DA." `T.isPrefixOf` m -> 1
                    | otherwise -> 2

-- | Name of daml package, e.g. "daml-prim", "daml-stdlib"
newtype Packagename = Packagename { unPackagename :: Text }
    deriving newtype (Eq, Ord, Show, ToJSON, FromJSON, IsString)

-- | Type expression, possibly a (nested) type application
data Type = TypeApp !(Maybe Reference) !Typename [Type] -- ^ Type application
          | TypeFun [Type] -- ^ Function type
          | TypeList Type   -- ^ List syntax
          | TypeTuple [Type] -- ^ Tuple syntax
          | TypeLit Text -- ^ a literal (e.g. "foo") appearing at the type level
  deriving (Eq, Ord, Show, Generic)

-- | Function/class context (appears on left-hand-side of "=>").
newtype Context = Context [Type]
  deriving (Eq, Ord, Show, Generic)

getTypeAppAnchor :: Type -> Maybe Anchor
getTypeAppAnchor = \case
    TypeApp refM _ _ -> referenceAnchor <$> refM
    _ -> Nothing

getTypeAppName :: Type -> Maybe Typename
getTypeAppName = \case
    TypeApp _ n _ -> Just n
    _ -> Nothing

getTypeAppArgs :: Type -> Maybe [Type]
getTypeAppArgs = \case
    TypeApp _ _ a -> Just a
    _ -> Nothing

instance Hashable Type where
  hashWithSalt salt = hashWithSalt salt . show

-- | A docs reference, possibly external (i.e. in another package).
data Reference = Reference
    { referencePackage :: Maybe Packagename
    , referenceAnchor :: Anchor
    } deriving (Eq, Ord, Show, Generic)

-- | Anchors are URL-safe (and RST-safe!) ids into the docs.
newtype Anchor = Anchor { unAnchor :: Text }
    deriving newtype (Eq, Ord, Show, ToJSON, ToJSONKey, FromJSON, FromJSONKey, IsString, Hashable)

-- | A database of anchors to URL strings.
newtype AnchorMap = AnchorMap { unAnchorMap :: HMS.HashMap Anchor String }
    deriving newtype (Eq, Ord, Show, FromJSON)

------------------------------------------------------------
-- | Documentation data for a module
data ModuleDoc = ModuleDoc
  { md_anchor    :: Maybe Anchor
  , md_name      :: Modulename
  , md_descr     :: Maybe DocText
  , md_templates :: [TemplateDoc]
  , md_interfaces :: [InterfaceDoc]
  , md_adts      :: [ADTDoc]
  , md_functions :: [FunctionDoc]
  , md_classes   :: [ClassDoc]
  -- TODO will later be refactored to contain "documentation sections" with an
  -- optional header, containing groups of templates and ADTs. This can be done
  -- storing just linkIDs for them, the renderer would then search the lists.
  , md_instances :: [InstanceDoc]
  }
  deriving (Eq, Show, Generic)


-- | Documentation data for a template
data TemplateDoc = TemplateDoc
  { td_anchor  :: Maybe Anchor
  , td_name    :: Typename
  , td_descr   :: Maybe DocText
  , td_payload :: [FieldDoc]
  , td_choices :: [ChoiceDoc]
  , td_interfaceInstances :: [InterfaceInstanceDoc]
  }
  deriving (Eq, Show, Generic)

data InterfaceDoc = InterfaceDoc
  { if_anchor :: Maybe Anchor
  , if_name :: Typename
  , if_choices :: [ChoiceDoc]
  , if_methods :: [MethodDoc]
  , if_descr :: Maybe DocText
  , if_interfaceInstances :: [InterfaceInstanceDoc]
  , if_viewtype :: Maybe InterfaceViewtypeDoc
  }
  deriving (Eq, Show, Generic)

newtype InterfaceViewtypeDoc = InterfaceViewtypeDoc Type
  deriving (Eq, Ord, Show, Generic)

data InterfaceInstanceDoc = InterfaceInstanceDoc
  { ii_interface :: Type
  , ii_template :: Type
  }
  deriving (Eq, Ord, Show, Generic)

data ClassDoc = ClassDoc
  { cl_anchor :: Maybe Anchor
  , cl_name :: Typename
  , cl_descr :: Maybe DocText
  , cl_super :: Context
  , cl_args :: [Text]
  , cl_methods :: [ClassMethodDoc]
  , cl_instances :: Maybe [InstanceDoc] -- relevant instances
  }
  deriving (Eq, Show, Generic)

-- | Documentation data for typeclass methods.
data ClassMethodDoc = ClassMethodDoc
    { cm_anchor :: Maybe Anchor
    , cm_name :: Fieldname
    , cm_isDefault :: Bool
        -- ^ Is this a default implementation, associated with a
        -- separate default type signature? (These are marked with
        -- "default" in the source code and docs. For example, in
        -- the typeclass:
        --
        -- @
        --     class MyShow t where
        --         myShow :: t -> String
        --         default myShow :: Show t => t -> String
        -- @
        --
        -- The former method would have 'cm_isDefault' set to 'False',
        -- the latter would have 'cm_isDefault' set to 'True'.
    , cm_localContext :: Context
        -- ^ Context of class method inside typeclass declaration.
        -- For example, 'fold' from @'Foldable' t@:
        --
        -- @
        --     class Foldable t where
        --         fold :: Monoid m => t m -> m
        --         ...
        -- @
        --
        -- Would have the 'cm_localContext' of @('Monoid' m)@.
    , cm_globalContext :: Context
        -- ^ Context of class method outside typeclass declaration.
        -- Following the previous example, 'fold' from @'Foldable' t@
        -- would have the 'cm_globalContext' of @('Foldable' t, 'Monoid' m)@.
        --
        -- In other words, the difference between 'cm_globalContext' and
        -- 'cm_localContext' is that the former has the containing
        -- typeclass in the context, but the latter does not.
    , cm_type :: Type
    , cm_descr :: Maybe DocText
    } deriving (Eq, Show, Generic)

-- | Documentation data for an ADT or type synonym
data ADTDoc = ADTDoc
  { ad_anchor :: Maybe Anchor
  , ad_name   :: Typename
  , ad_descr  :: Maybe DocText
  , ad_args   :: [Text] -- retain names of type var.s
  , ad_constrs :: [ADTConstr]  -- allowed to be empty
  , ad_instances :: Maybe [InstanceDoc] -- relevant instances
  }
  | TypeSynDoc
  { ad_anchor :: Maybe Anchor
  , ad_name   :: Typename
  , ad_descr  :: Maybe DocText
  , ad_args   :: [Text] -- retain names of type var.s
  , ad_rhs    :: Type
  , ad_instances :: Maybe [InstanceDoc] -- relevant instances
  }
  deriving (Eq, Show, Generic)


-- | Constructors (Record or Prefix)
data ADTConstr =
    PrefixC { ac_anchor :: Maybe Anchor
            , ac_name :: Typename
            , ac_descr :: Maybe DocText
            , ac_args :: [Type]   -- use retained var.names
            }
  | RecordC { ac_anchor :: Maybe Anchor
            , ac_name :: Typename
            , ac_descr :: Maybe DocText
            , ac_fields :: [FieldDoc]
            }
  deriving (Eq, Show, Generic)


-- | Choices are like ADTs: name, link, optional description, but the
-- associated type always has exactly one record constructor with the same name
-- as the choice.
data ChoiceDoc = ChoiceDoc
  { cd_name   :: Typename
  , cd_descr  :: Maybe DocText
  , cd_fields :: [FieldDoc]
  }
  deriving (Eq, Show, Generic)

data MethodDoc = MethodDoc
  { mtd_name :: Typename
  , mtd_type :: Type
  , mtd_descr :: Maybe DocText
  }
  deriving (Eq, Show, Generic)

-- | Documentation data for a field in a record
data FieldDoc = FieldDoc
  { fd_anchor :: Maybe Anchor
  , fd_name  :: Fieldname
  , fd_type  :: Type
    -- TODO align with GHC data structure. The type representation must use FQ
    -- names in components to enable links, and it Can use bound type var.s.
  , fd_descr :: Maybe DocText
  }
  deriving (Eq, Show, Generic)


-- | Documentation data for functions (top level only, type optional)
data FunctionDoc = FunctionDoc
  { fct_anchor :: Maybe Anchor
  , fct_name  :: Fieldname
  , fct_context :: Context
  , fct_type  :: Type
  , fct_descr :: Maybe DocText
  } deriving (Eq, Show, Generic)

-- | Documentation on a typeclass instance.
data InstanceDoc = InstanceDoc
    { id_type :: Type
    , id_module :: Modulename
    , id_context :: Context
    , id_isOrphan :: Bool
    , id_descr :: Maybe DocText
    } deriving (Eq, Ord, Show, Generic)

-----------------------------------------------------
-- generate JSON instances

instance ToJSON Reference where
    toJSON = genericToJSON aesonOptions

instance FromJSON Reference where
    parseJSON = genericParseJSON aesonOptions

instance ToJSON Type where
    toJSON = genericToJSON aesonOptions

instance FromJSON Type where
    parseJSON = genericParseJSON aesonOptions

instance ToJSON Context where
    toJSON = genericToJSON aesonOptions

instance FromJSON Context where
    parseJSON = genericParseJSON aesonOptions

instance ToJSON FunctionDoc where
    toJSON = genericToJSON aesonOptions

instance FromJSON FunctionDoc where
    parseJSON = genericParseJSON aesonOptions

instance ToJSON ClassDoc where
    toJSON = genericToJSON aesonOptions

instance FromJSON ClassDoc where
    parseJSON = genericParseJSON aesonOptions

instance ToJSON ClassMethodDoc where
    toJSON = genericToJSON aesonOptions

instance FromJSON ClassMethodDoc where
    parseJSON = genericParseJSON aesonOptions

instance ToJSON FieldDoc where
    toJSON = genericToJSON aesonOptions

instance FromJSON FieldDoc where
    parseJSON = genericParseJSON aesonOptions

instance ToJSON ADTDoc where
    toJSON = genericToJSON aesonOptions

instance FromJSON ADTDoc where
    parseJSON = genericParseJSON aesonOptions

instance ToJSON ADTConstr where
    toJSON = genericToJSON aesonOptions

instance FromJSON ADTConstr where
    parseJSON = genericParseJSON aesonOptions

instance ToJSON ChoiceDoc where
    toJSON = genericToJSON aesonOptions

instance FromJSON ChoiceDoc where
    parseJSON = genericParseJSON aesonOptions

instance ToJSON InterfaceInstanceDoc where
    toJSON = genericToJSON aesonOptions

instance FromJSON InterfaceInstanceDoc where
    parseJSON = genericParseJSON aesonOptions

instance ToJSON TemplateDoc where
    toJSON = genericToJSON aesonOptions

instance FromJSON TemplateDoc where
    parseJSON = genericParseJSON aesonOptions

instance ToJSON InterfaceDoc where
    toJSON = genericToJSON aesonOptions

instance FromJSON InterfaceDoc where
    parseJSON = genericParseJSON aesonOptions

instance ToJSON InterfaceViewtypeDoc where
    toJSON = genericToJSON aesonOptions

instance FromJSON InterfaceViewtypeDoc where
    parseJSON = genericParseJSON aesonOptions

instance ToJSON MethodDoc where
    toJSON = genericToJSON aesonOptions

instance FromJSON MethodDoc where
    parseJSON = genericParseJSON aesonOptions

instance ToJSON InstanceDoc where
    toJSON = genericToJSON aesonOptions

instance FromJSON InstanceDoc where
    parseJSON = genericParseJSON aesonOptions

instance ToJSON ModuleDoc where
    toJSON = genericToJSON aesonOptions

instance FromJSON ModuleDoc where
    parseJSON = genericParseJSON aesonOptions

aesonOptions :: Options
aesonOptions = defaultOptions
    { sumEncoding = ObjectWithSingleField
    , omitNothingFields = True
    }
