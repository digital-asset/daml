-- | This module provides types and functions to generate .proto files.

{-# LANGUAGE GeneralizedNewtypeDeriving #-}
{-# LANGUAGE OverloadedLists            #-}
{-# LANGUAGE RecordWildCards            #-}
{-# LANGUAGE ScopedTypeVariables        #-}
{-# LANGUAGE PackageImports             #-}
{-# OPTIONS_GHC -fno-warn-orphans       #-}

module Proto3.Suite.DotProto.Rendering
  ( renderDotProto
  , defRenderingOptions
  , defSelectorName
  , defEnumMemberName
  , packageFromDefs
  , toProtoFile
  , toProtoFileDef
  , RenderingOptions(..)
  ) where

import           Data.Char
import qualified Data.Text                       as T
import           Filesystem.Path.CurrentOS       (toText)
import           Proto3.Suite.DotProto.AST
import           Proto3.Wire.Types               (FieldNumber (..))

import           "pretty" Text.PrettyPrint                (($$), (<+>))
import qualified "pretty" Text.PrettyPrint                as PP
import           "pretty" Text.PrettyPrint.HughesPJClass  (Pretty(..))

-- | Options for rendering a @.proto@ file.
data RenderingOptions = RenderingOptions
  { roSelectorName   :: DotProtoIdentifier -> DotProtoIdentifier -> FieldNumber -> PP.Doc
  -- ^ This function will be applied to each
  -- record selector name to turn it into a protobuf
  -- field name (default: uses the selector name, unchanged).
  , roEnumMemberName :: DotProtoIdentifier -> DotProtoIdentifier -> PP.Doc
  -- ^ This function will be applied to each
  -- enum member name to turn it into a protobuf
  -- field name (default: uses the field name, unchanged).
  }

-- | Default rendering options.
defRenderingOptions :: RenderingOptions
defRenderingOptions =
    RenderingOptions { roSelectorName   = defSelectorName
                     , roEnumMemberName = defEnumMemberName
                     }

-- | The default choice of field name for a selector.
defSelectorName :: DotProtoIdentifier -> DotProtoIdentifier -> FieldNumber -> PP.Doc
defSelectorName _ fieldName _ = pPrint fieldName

-- | The default choice of enum member name for an enum
defEnumMemberName :: DotProtoIdentifier -> DotProtoIdentifier -> PP.Doc
defEnumMemberName = const pPrint

-- | Traverses a DotProto AST and generates a .proto file from it
renderDotProto :: RenderingOptions -> DotProto -> PP.Doc
renderDotProto opts DotProto{..}
  = PP.text "syntax = \"proto3\";"
 $$ pPrint protoPackage
 $$ (PP.vcat $ pPrint    <$> protoImports)
 $$ (PP.vcat $ topOption <$> protoOptions)
 $$ (PP.vcat $ prettyPrintProtoDefinition opts <$> protoDefinitions)

instance Pretty DotProtoPackageSpec where
  pPrint (DotProtoPackageSpec p) = PP.text "package" <+> pPrint p PP.<> PP.text ";"
  pPrint (DotProtoNoPackage)     = PP.empty

instance Pretty DotProtoImport where
  pPrint (DotProtoImport q i) =
    PP.text "import" <+> pPrint q <+> PP.text fp PP.<> PP.text ";"
    where
      fp = case T.unpack . either id id . toText $ i of
             [] -> show ("" :: String)
             x  -> x

instance Pretty DotProtoImportQualifier where
  pPrint DotProtoImportDefault = PP.empty
  pPrint DotProtoImportPublic  = PP.text "public"
  pPrint DotProtoImportWeak    = PP.text "weak"

optionAnnotation :: [DotProtoOption] -> PP.Doc
optionAnnotation [] = PP.empty
optionAnnotation os = PP.brackets
                    . PP.hcat
                    . PP.punctuate (PP.text ", ")
                    $ pPrint <$> os

topOption :: DotProtoOption -> PP.Doc
topOption o = PP.text "option" <+> pPrint o PP.<> PP.text ";"

instance Pretty DotProtoOption where
  pPrint (DotProtoOption key value) = pPrint key <+> PP.text "=" <+> pPrint value

prettyPrintProtoDefinition :: RenderingOptions -> DotProtoDefinition -> PP.Doc
prettyPrintProtoDefinition opts = defn where
  defn :: DotProtoDefinition -> PP.Doc
  defn (DotProtoMessage name parts) = PP.text "message" <+> pPrint name <+> (braces $ PP.vcat $ msgPart name <$> parts)
  defn (DotProtoEnum    name parts) = PP.text "enum"    <+> pPrint name <+> (PP.braces $ PP.vcat $ enumPart name <$> parts)
  defn (DotProtoService name parts) = PP.text "service" <+> pPrint name <+> (PP.braces $ PP.vcat $ pPrint <$> parts)

  -- Put the final closing brace on the next line.
  -- This is important, since the final field might have a comment, and
  -- the brace cannot be part of the comment.
  -- We could use block comments instead, once the parser/lexer supports them.
  braces :: PP.Doc -> PP.Doc
  braces = ($$ PP.text "}") . (PP.text "{" <+>)

  msgPart :: DotProtoIdentifier -> DotProtoMessagePart -> PP.Doc
  msgPart msgName (DotProtoMessageField f)           = field msgName f
  msgPart _       (DotProtoMessageDefinition definition) = defn definition
  msgPart _       (DotProtoMessageReserved reservations)
    =   PP.text "reserved"
    <+> (PP.hcat . PP.punctuate (PP.text ", ") $ pPrint <$> reservations)
    PP.<>  PP.text ";"
  msgPart msgName (DotProtoMessageOneOf name fields)     = PP.text "oneof" <+> pPrint name <+> (PP.braces $ PP.vcat $ field msgName <$> fields)

  field :: DotProtoIdentifier -> DotProtoField -> PP.Doc
  field msgName (DotProtoField number mtype name options comments)
    =   pPrint mtype
    <+> roSelectorName opts msgName name number
    <+> PP.text "="
    <+> pPrint number
    <+> optionAnnotation options
    PP.<>  PP.text ";"
    PP.<>  maybe PP.empty (PP.text . (" // " ++)) comments
  field _ DotProtoEmptyField = PP.empty

  enumPart :: DotProtoIdentifier -> DotProtoEnumPart -> PP.Doc
  enumPart msgName (DotProtoEnumField name value options)
    = roEnumMemberName opts msgName name
    <+> PP.text "="
    <+> pPrint value
    <+> optionAnnotation options
    PP.<> PP.text ";"
  enumPart _       (DotProtoEnumOption opt)
    = PP.text "option" <+> pPrint opt PP.<> PP.text ";"
  enumPart _       DotProtoEnumEmpty
    = PP.empty

instance Pretty DotProtoServicePart where
  pPrint (DotProtoServiceRPC name (callname, callstrm) (retname, retstrm) options)
    =   PP.text "rpc"
    <+> pPrint name
    <+> PP.parens (pPrint callstrm <+> pPrint callname)
    <+> PP.text "returns"
    <+> PP.parens (pPrint retstrm <+> pPrint retname)
    <+> case options of
          [] -> PP.text ";"
          _  -> PP.braces . PP.vcat $ topOption <$> options
  pPrint (DotProtoServiceOption option) = topOption option
  pPrint DotProtoServiceEmpty           = PP.empty

instance Pretty Streaming where
  pPrint Streaming    = PP.text "stream"
  pPrint NonStreaming = PP.empty

instance Pretty DotProtoIdentifier where
  pPrint (Single name)                    = PP.text name
  pPrint (Dots (Path names))              = PP.hcat . PP.punctuate (PP.text ".") $ PP.text <$> names
  pPrint (Qualified qualifier identifier) = PP.parens (pPrint qualifier) PP.<> PP.text "." PP.<> pPrint identifier
  pPrint Anonymous                        = PP.empty

instance Pretty DotProtoValue where
  pPrint (Identifier value) = pPrint value
  pPrint (StringLit  value) = PP.text $ show value
  pPrint (IntLit     value) = PP.text $ show value
  pPrint (FloatLit   value) = PP.text $ show value
  pPrint (BoolLit    value) = PP.text $ toLower <$> show value

instance Pretty DotProtoType where
  pPrint (Prim           ty) = pPrint ty
  pPrint (Optional       ty) = pPrint ty
  pPrint (Repeated       ty) = PP.text "repeated" <+> pPrint ty
  pPrint (NestedRepeated ty) = PP.text "repeated" <+> pPrint ty
  pPrint (Map keyty valuety) = PP.text "<" PP.<> pPrint keyty PP.<> PP.text ", " PP.<> pPrint valuety PP.<> PP.text ">"

instance Pretty DotProtoPrimType where
  pPrint (Named i)  = pPrint i
  pPrint Int32      = PP.text "int32"
  pPrint Int64      = PP.text "int64"
  pPrint SInt32     = PP.text "sint32"
  pPrint SInt64     = PP.text "sint64"
  pPrint UInt32     = PP.text "uint32"
  pPrint UInt64     = PP.text "uint64"
  pPrint Fixed32    = PP.text "fixed32"
  pPrint Fixed64    = PP.text "fixed64"
  pPrint SFixed32   = PP.text "sfixed32"
  pPrint SFixed64   = PP.text "sfixed64"
  pPrint String     = PP.text "string"
  pPrint Bytes      = PP.text "bytes"
  pPrint Bool       = PP.text "bool"
  pPrint Float      = PP.text "float"
  pPrint Double     = PP.text "double"

instance Pretty FieldNumber where
  pPrint = PP.text . show . getFieldNumber

instance Pretty DotProtoReservedField where
  pPrint (SingleField num)      = PP.text $ show num
  pPrint (FieldRange start end) = (PP.text $ show start) <+> PP.text "to" <+> (PP.text $ show end)
  pPrint (ReservedIdentifier i) = PP.text $ show i

-- | Render protobufs metadata as a .proto file stringy
toProtoFile :: RenderingOptions -> DotProto -> String
toProtoFile opts = PP.render . renderDotProto opts

-- | Render protobufs metadata as a .proto file string,
-- using the default rendering options.

toProtoFileDef :: DotProto -> String
toProtoFileDef = toProtoFile defRenderingOptions

packageFromDefs :: String -> [DotProtoDefinition] -> DotProto
packageFromDefs package defs =
  DotProto [] [] (DotProtoPackageSpec $ Single package) defs (DotProtoMeta $ Path [])
