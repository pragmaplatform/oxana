use proc_macro_error2::abort;
use proc_macro2::TokenStream;
use quote::quote;
use syn::{Data, DeriveInput, Fields};

pub fn expand_derive_worker_group(input: DeriveInput) -> TokenStream {
    let Data::Struct(data) = &input.data else {
        abort!(input.ident, "WorkerGroup must be a unit struct.");
    };

    if !matches!(data.fields, Fields::Unit) {
        abort!(input.ident, "WorkerGroup must be a unit struct.");
    }

    let ident = &input.ident;

    quote! {
        #[automatically_derived]
        impl oxana::WorkerGroup for #ident {}
    }
}
