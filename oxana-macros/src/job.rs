use darling::{Error, FromDeriveInput, FromMeta, util::Flag};
use heck::{
    ToKebabCase, ToLowerCamelCase, ToShoutyKebabCase, ToShoutySnakeCase, ToSnakeCase,
    ToUpperCamelCase,
};
use proc_macro_error2::abort;
use proc_macro2::TokenStream;
use quote::quote;
use syn::{
    Attribute, Data, DeriveInput, Expr, Fields, GenericArgument, Ident, LitStr, Meta, Path,
    PathArguments, Token, Type, punctuated::Punctuated,
};

#[derive(Debug, FromDeriveInput)]
#[darling(attributes(oxana), supports(struct_any))]
struct OxanaJobArgs {
    unique_id: Option<UniqueIdSpec>,
    on_conflict: Option<Ident>,
    resurrect: Option<bool>,
    resume: Option<bool>,
    throttle_cost: Option<ThrottleCost>,
    on_demand: Flag,
}

#[derive(Debug)]
enum UniqueIdSpec {
    /// #[unique_id = "job_{id}"]
    Shorthand(LitStr),

    /// #[unique_id(fmt = "...", name = expr, ...)]
    NamedFormatter {
        fmt: LitStr,
        args: Vec<(syn::Ident, Expr)>,
    },

    /// #[unique_id = Self::unique_id]
    CustomFunc(Path),
}

#[derive(Debug)]
enum ThrottleCost {
    /// #[throttle_cost = 2]
    Value(u64),
    /// #[throttle_cost = Self::throttle_cost]
    CustomFunc(Path),
}

impl FromMeta for UniqueIdSpec {
    fn from_meta(meta: &Meta) -> darling::Result<Self> {
        match meta {
            Meta::NameValue(nv) => match &nv.value {
                Expr::Lit(expr_lit) => {
                    if let syn::Lit::Str(s) = &expr_lit.lit {
                        Ok(UniqueIdSpec::Shorthand(s.clone()))
                    } else {
                        Err(Error::custom("unique_id must be a string literal"))
                    }
                }
                Expr::Path(expr_path) => Ok(UniqueIdSpec::CustomFunc(expr_path.path.clone())),
                _ => Err(Error::custom("Expected string literal or type path.")),
            },
            Meta::List(list) => {
                let mut fmt = None;
                let mut args = Vec::new();

                let metas =
                    list.parse_args_with(Punctuated::<Meta, Token![,]>::parse_terminated)?;

                for meta in metas {
                    match meta {
                        Meta::NameValue(nv) if nv.path.is_ident("fmt") => {
                            #[allow(clippy::collapsible_if)] // requires 1.88
                            if let syn::Expr::Lit(expr_lit) = nv.value {
                                if let syn::Lit::Str(s) = expr_lit.lit {
                                    fmt = Some(s);
                                    continue;
                                }
                            }
                            return Err(Error::custom("fmt must be a string literal"));
                        }

                        Meta::NameValue(nv) => {
                            let ident = nv
                                .path
                                .get_ident()
                                .ok_or_else(|| Error::custom("expected identifier"))?
                                .clone();
                            args.push((ident, nv.value));
                        }

                        _ => return Err(Error::custom("Unsupported unique_id syntax")),
                    }
                }

                let fmt = fmt.ok_or_else(|| Error::custom("missing fmt = \"...\""))?;
                Ok(UniqueIdSpec::NamedFormatter { fmt, args })
            }
            _ => Err(Error::custom("Invalid unique_id attribute")),
        }
    }
}

impl FromMeta for ThrottleCost {
    fn from_meta(meta: &Meta) -> darling::Result<Self> {
        match meta {
            Meta::NameValue(nv) => match &nv.value {
                Expr::Lit(syn::ExprLit {
                    lit: syn::Lit::Int(lit),
                    ..
                }) => {
                    let value = lit.base10_parse::<u64>()?;
                    Ok(Self::Value(value))
                }
                Expr::Path(expr_path) => Ok(Self::CustomFunc(expr_path.path.clone())),
                other => Err(Error::custom(format!(
                    "Unsupported throttle_cost value: {other:?}",
                ))),
            },
            _ => Err(Error::custom(
                "throttle_cost must be a name-value attribute",
            )),
        }
    }
}

pub fn expand_derive_job(input: DeriveInput) -> TokenStream {
    let args = match OxanaJobArgs::from_derive_input(&input) {
        Ok(v) => v,
        Err(e) => {
            abort!(input.ident, "{}", e);
        }
    };

    let struct_ident = &input.ident;
    let unique_id = match &args.unique_id {
        Some(unique_id) => expand_unique_id(unique_id),
        None => quote!(),
    };

    let on_conflict = match &args.on_conflict {
        Some(on_conflict) => {
            let replaces_on_conflict = on_conflict == "Replace";
            quote! {
                const REPLACES_ON_CONFLICT: bool = #replaces_on_conflict;

                fn on_conflict(&self) -> oxana::JobConflictStrategy {
                    oxana::JobConflictStrategy::#on_conflict
                }
            }
        }
        None => quote!(),
    };

    let resurrect = match args.resurrect {
        Some(value) => quote! {
            fn should_resurrect() -> bool
            where
                Self: Sized,
            {
                #value
            }
        },
        None => quote!(),
    };

    let resume = match args.resume {
        Some(value) => quote! {
            fn should_resume() -> bool
            where
                Self: Sized,
            {
                #value
            }
        },
        None => quote!(),
    };

    let throttle_cost = match &args.throttle_cost {
        Some(throttle_cost) => expand_throttle_cost(throttle_cost),
        None => quote!(),
    };

    let on_demand_args_template =
        expand_on_demand_args_template(&input, args.on_demand.is_present());

    quote! {
        #[automatically_derived]
        impl oxana::Job for #struct_ident {
            #unique_id

            #on_conflict

            #resurrect

            #resume

            #throttle_cost

            #on_demand_args_template
        }
    }
}

fn extract_format_placeholders(fmt_str: &str) -> Vec<syn::Ident> {
    let mut seen = std::collections::HashSet::new();
    let mut result = Vec::new();
    let mut chars = fmt_str.chars().peekable();
    while let Some(ch) = chars.next() {
        if ch == '{' {
            let mut name = String::new();
            for inner in chars.by_ref() {
                if inner == '}' {
                    break;
                }
                name.push(inner);
            }
            if !name.is_empty()
                && name.chars().all(|c| c.is_alphanumeric() || c == '_')
                && !name.starts_with(|c: char| c.is_ascii_digit())
                && seen.insert(name.clone())
            {
                result.push(syn::Ident::new(&name, proc_macro2::Span::call_site()));
            }
        }
    }
    result
}

fn expand_unique_id(spec: &UniqueIdSpec) -> TokenStream {
    let formatter = match spec {
        UniqueIdSpec::Shorthand(fmt) => {
            let fmt_str = fmt.value();
            let placeholders = extract_format_placeholders(&fmt_str);
            let args = placeholders.iter().map(|name| quote!(#name = self.#name));

            quote! {
                Some(format!(
                    #fmt,
                    #(#args),*
                ))
            }
        }

        UniqueIdSpec::NamedFormatter { fmt, args } => {
            let args = args.iter().map(|(name, expr)| quote!(#name = #expr));

            quote! {
                Some(format!(
                    #fmt,
                    #(#args),*
                ))
            }
        }

        UniqueIdSpec::CustomFunc(func) => quote!(#func(self)),
    };

    quote! {
        fn unique_id(&self) -> Option<String> {
            #formatter
        }
    }
}

fn expand_throttle_cost(throttle_cost: &ThrottleCost) -> TokenStream {
    match throttle_cost {
        ThrottleCost::Value(value) => {
            quote! {
                fn throttle_cost(&self) -> Option<u64> {
                    Some(#value)
                }
            }
        }
        ThrottleCost::CustomFunc(func) => {
            quote! {
                fn throttle_cost(&self) -> Option<u64> {
                    #func(self)
                }
            }
        }
    }
}

fn expand_on_demand_args_template(input: &DeriveInput, on_demand: bool) -> TokenStream {
    if !on_demand {
        return quote!();
    }

    let template = match &input.data {
        Data::Struct(data) => match &data.fields {
            Fields::Named(fields) => {
                let rename_all = serde_rename_all(&input.attrs);
                let entries = fields.named.iter().filter_map(|field| {
                    let serde = serde_field_attrs(&field.attrs);
                    if serde.skip {
                        return None;
                    }

                    let ident = field.ident.as_ref().expect("named field has ident");
                    let name = serde
                        .rename
                        .unwrap_or_else(|| rename_field(&ident.to_string(), rename_all));
                    let name = LitStr::new(&name, ident.span());
                    let value = json_template_for_type(&field.ty);
                    Some(quote!(#name: #value))
                });
                quote!({ #(#entries),* })
            }
            Fields::Unnamed(fields) => {
                let entries = fields
                    .unnamed
                    .iter()
                    .map(|field| json_template_for_type(&field.ty));
                quote!([ #(#entries),* ])
            }
            Fields::Unit => quote!(null),
        },
        _ => abort!(input.ident, "Job must be a struct."),
    };

    quote! {
        fn on_demand_args_template() -> Option<serde_json::Value>
        where
            Self: Sized,
        {
            Some(serde_json::json!(#template))
        }
    }
}

#[derive(Clone, Copy)]
enum RenameRule {
    Lowercase,
    Uppercase,
    PascalCase,
    CamelCase,
    SnakeCase,
    ScreamingSnakeCase,
    KebabCase,
    ScreamingKebabCase,
}

impl RenameRule {
    fn from_str(value: &str) -> Option<Self> {
        match value {
            "lowercase" => Some(Self::Lowercase),
            "UPPERCASE" => Some(Self::Uppercase),
            "PascalCase" => Some(Self::PascalCase),
            "camelCase" => Some(Self::CamelCase),
            "snake_case" => Some(Self::SnakeCase),
            "SCREAMING_SNAKE_CASE" => Some(Self::ScreamingSnakeCase),
            "kebab-case" => Some(Self::KebabCase),
            "SCREAMING-KEBAB-CASE" => Some(Self::ScreamingKebabCase),
            _ => None,
        }
    }
}

fn rename_field(name: &str, rule: Option<RenameRule>) -> String {
    match rule {
        Some(RenameRule::Lowercase) => name.to_lowercase(),
        Some(RenameRule::Uppercase) => name.to_uppercase(),
        Some(RenameRule::PascalCase) => name.to_upper_camel_case(),
        Some(RenameRule::CamelCase) => name.to_lower_camel_case(),
        Some(RenameRule::SnakeCase) => name.to_snake_case(),
        Some(RenameRule::ScreamingSnakeCase) => name.to_shouty_snake_case(),
        Some(RenameRule::KebabCase) => name.to_kebab_case(),
        Some(RenameRule::ScreamingKebabCase) => name.to_shouty_kebab_case(),
        None => name.to_string(),
    }
}

fn serde_rename_all(attrs: &[Attribute]) -> Option<RenameRule> {
    let mut rename_all = None;

    for attr in attrs.iter().filter(|attr| attr.path().is_ident("serde")) {
        let _ = attr.parse_nested_meta(|meta| {
            if meta.path.is_ident("rename_all") {
                let value: LitStr = meta.value()?.parse()?;
                rename_all = RenameRule::from_str(&value.value());
            }
            Ok(())
        });
    }

    rename_all
}

struct SerdeFieldAttrs {
    rename: Option<String>,
    skip: bool,
}

fn serde_field_attrs(attrs: &[Attribute]) -> SerdeFieldAttrs {
    let mut result = SerdeFieldAttrs {
        rename: None,
        skip: false,
    };

    for attr in attrs.iter().filter(|attr| attr.path().is_ident("serde")) {
        let _ = attr.parse_nested_meta(|meta| {
            if meta.path.is_ident("rename") {
                let value: LitStr = meta.value()?.parse()?;
                result.rename = Some(value.value());
            } else if meta.path.is_ident("skip") || meta.path.is_ident("skip_deserializing") {
                result.skip = true;
            }
            Ok(())
        });
    }

    result
}

fn json_template_for_type(ty: &Type) -> TokenStream {
    match ty {
        Type::Array(_) => quote!([]),
        Type::Group(group) => json_template_for_type(&group.elem),
        Type::Paren(paren) => json_template_for_type(&paren.elem),
        Type::Reference(reference) => json_template_for_type(&reference.elem),
        Type::Tuple(tuple) => {
            let entries = tuple.elems.iter().map(json_template_for_type);
            quote!([ #(#entries),* ])
        }
        Type::Path(path) => json_template_for_path(path),
        _ => quote!({}),
    }
}

fn json_template_for_path(path: &syn::TypePath) -> TokenStream {
    let Some(segment) = path.path.segments.last() else {
        return quote!({});
    };

    let ident = segment.ident.to_string();

    match ident.as_str() {
        "Option" => quote!(null),
        "String" | "str" => quote!(""),
        "bool" => quote!(false),
        "f32" | "f64" => quote!(0.0),
        "i8" | "i16" | "i32" | "i64" | "i128" | "isize" | "u8" | "u16" | "u32" | "u64" | "u128"
        | "usize" => quote!(0),
        _ if looks_like_numeric_id_newtype(&ident) => quote!(0),
        "Vec" | "VecDeque" | "LinkedList" | "HashSet" | "BTreeSet" => quote!([]),
        "HashMap" | "BTreeMap" => quote!({}),
        "Box" => {
            generic_type(&segment.arguments).map_or_else(|| quote!({}), json_template_for_type)
        }
        _ => quote!({}),
    }
}

fn looks_like_numeric_id_newtype(ident: &str) -> bool {
    ident.ends_with("Id") || ident.ends_with("ID")
}

fn generic_type(arguments: &PathArguments) -> Option<&Type> {
    let PathArguments::AngleBracketed(args) = arguments else {
        return None;
    };

    args.args.iter().find_map(|argument| match argument {
        GenericArgument::Type(ty) => Some(ty),
        _ => None,
    })
}
