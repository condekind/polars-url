#![allow(clippy::unused_unit)]
use crate::expressions::field::field_getter;
use polars::prelude::arity::binary_elementwise;
use polars::prelude::*;
use pyo3_polars::derive::polars_expr;
use serde::Deserialize;
use std::fmt::Write;
use url::Url;

#[derive(Deserialize)]
struct ExtractFieldKwarg {
    field: String,
}

mod field {
    use url::Url;

    pub fn nothing(_url: Url) -> String {
        String::from("")
    }

    pub fn scheme(url: Url) -> String {
        String::from(url.scheme())
    }

    pub fn username(url: Url) -> String {
        String::from(url.username())
    }

    pub fn password(url: Url) -> String {
        String::from(url.password().unwrap_or(""))
    }

    pub fn host(url: Url) -> String {
        String::from(url.host_str().unwrap_or(""))
    }

    pub fn port(url: Url) -> String {
        if let Some(port) = url.port() {
            port.to_string()
        } else {
            String::from("")
        }
    }

    pub fn path(url: Url) -> String {
        String::from(url.path())
    }

    pub fn query(url: Url) -> String {
        String::from(url.query().unwrap_or(""))
    }

    pub fn fragment(url: Url) -> String {
        String::from(url.fragment().unwrap_or(""))
    }

    pub fn field_getter(field: &str) -> fn(url::Url) -> std::string::String {
        match field {
            "scheme" => scheme,
            "username" => username,
            "password" => password,
            "host" => host,
            "port" => port,
            "path" => path,
            "query" => query,
            "fragment" => fragment,
            _ => nothing,
        }
    }
}

#[polars_expr(output_type=String)]
fn parse_url(inputs: &[Series], kwargs: ExtractFieldKwarg) -> PolarsResult<Series> {
    let ca = inputs[0].str()?;

    // This holds a function that extracts the desired field from a parsed Url
    let get_field = field_getter(kwargs.field.as_str());

    let out = ca.apply_to_buffer(|url_str: &str, output: &mut String| {
        if let Ok(url) = Url::parse(url_str) {
            let _ = write!(output, "{}", get_field(url));
        } else {
            let _ = write!(output, "");
        };
    });
    Ok(out.into_series())
}

#[polars_expr(output_type=String)]
fn extract_field_from_series(inputs: &[Series]) -> PolarsResult<Series> {
    let ca = inputs[0].str()?;
    let fields = inputs[1].str()?;

    let out: StringChunked =
        binary_elementwise(ca, fields, |url_str: Option<&str>, field: Option<&str>| {
            if let (Some(url_str), Some(field)) = (url_str, field) {
                let get_field = field_getter(field);
                if let Ok(url) = Url::parse(url_str) {
                    get_field(url)
                } else {
                    String::from("")
                }
            } else {
                String::from("")
            }
        });

    Ok(out.into_series())
}
