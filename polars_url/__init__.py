from __future__ import annotations

from pathlib import Path
from typing import TYPE_CHECKING

import polars as pl

from polars_url.utils import register_plugin

if TYPE_CHECKING:
    from polars.type_aliases import IntoExpr

lib = Path(__file__).parent


def add_suffix(expr: IntoExpr, *, suffix: str) -> pl.Expr:
    return register_plugin(
        args=[expr],
        lib=lib,
        symbol="add_suffix",
        is_elementwise=True,
        kwargs={"suffix": suffix},
    )


def parse_url(expr: IntoExpr, *, field: str) -> pl.Expr:
    return register_plugin(
        args=[expr],
        symbol="parse_url",
        is_elementwise=True,
        lib=lib,
        kwargs={"field": field},
    )


def extract_field_from_series(urls: IntoExpr, fields: IntoExpr) -> pl.Expr:
    return register_plugin(
        args=[urls, fields],
        symbol="extract_field_from_series",
        is_elementwise=True,
        lib=lib,
    )
