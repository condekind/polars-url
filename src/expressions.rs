#![allow(clippy::unused_unit)]
use crate::expressions::field::field_getter;
use polars::prelude::arity::{binary_elementwise, broadcast_binary_elementwise};
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

    pub fn nothing(_url: &Url) -> String {
        String::from("")
    }

    pub fn scheme(url: &Url) -> String {
        String::from(url.scheme())
    }

    pub fn username(url: &Url) -> String {
        String::from(url.username())
    }

    pub fn password(url: &Url) -> String {
        String::from(url.password().unwrap_or(""))
    }

    pub fn host(url: &Url) -> String {
        String::from(url.host_str().unwrap_or(""))
    }

    pub fn port(url: &Url) -> String {
        if let Some(port) = url.port() {
            port.to_string()
        } else {
            String::from("")
        }
    }

    pub fn path(url: &Url) -> String {
        String::from(url.path())
    }

    pub fn query(url: &Url) -> String {
        String::from(url.query().unwrap_or(""))
    }

    pub fn fragment(url: &Url) -> String {
        String::from(url.fragment().unwrap_or(""))
    }

    pub fn field_getter(field: &str) -> fn(&url::Url) -> std::string::String {
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
            let _ = write!(output, "{}", get_field(&url));
        } else {
            let _ = write!(output, "");
        };
    });
    Ok(out.into_series())
}

/// Takes two Series: one with URL strings, another with the name of Url fields
/// to be extracted.
#[polars_expr(output_type=String)]
fn extract_field_from_series(inputs: &[Series]) -> PolarsResult<Series> {
    let ca: &StringChunked = inputs[0].str()?;
    let fields: &StringChunked = inputs[1].str()?;

    let out: StringChunked = match (ca.len(), fields.len()) {
        (_, 1) => {
            let get_field = field_getter(fields.get(0).unwrap());
            ca.apply_to_buffer(|url_str, output| {
                if let Ok(url) = Url::parse(url_str) {
                    let _ = write!(output, "{}", get_field(&url));
                }
            })
        }
        (1, fields_len) => {
            if let Ok(url) = Url::parse(ca.get(0).unwrap()) {
                fields.apply_to_buffer(|field, output| {
                    let get_field = field_getter(field);
                    let _ = write!(output, "{}", get_field(&url));
                })
            } else {
                StringChunked::full_null(ca.name(), fields_len)
            }
        }
        _ => binary_elementwise(ca, fields, |url_str: Option<&str>, field: Option<&str>| {
            if let (Some(url_str), Some(field)) = (url_str, field) {
                let get_field = field_getter(field);
                if let Ok(url) = Url::parse(url_str) {
                    get_field(&url)
                } else {
                    String::from("")
                }
            } else {
                String::from("")
            }
        }),
    };

    Ok(out.into_series())
}

#[polars_expr(output_type=String)]
fn extract_field_from_series_noopt(inputs: &[Series]) -> PolarsResult<Series> {
    let ca: &StringChunked = inputs[0].str()?;
    let fields: &StringChunked = inputs[1].str()?;

    let out: StringChunked =
        broadcast_binary_elementwise(ca, fields, |url_str: Option<&str>, field: Option<&str>| {
            if let (Some(url_str), Some(field)) = (url_str, field) {
                let get_field = field_getter(field);
                if let Ok(url) = Url::parse(url_str) {
                    get_field(&url)
                } else {
                    String::from("")
                }
            } else {
                String::from("")
            }
        });

    Ok(out.into_series())
}
