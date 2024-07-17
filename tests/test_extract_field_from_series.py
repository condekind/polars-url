import polars as pl
from polars.testing import assert_frame_equal
from polars_url import extract_field_from_series


def test_extract_field_from_series():
    df = pl.DataFrame(
        {
            "URLs": [
                "https://web.whatsapp.com/",
                "https://www.example.com:8080/path?query=example#fragment",
                "https://[2001:db8::1]:666/api/v1?query=value",
                "ftp://user:pass@192.168.1.1:21/file/download",
                "http://example.com:80/path/to/resource?key=value#section1",
                "http://example.com:80/path/to/resource?key=value#section1",
            ],
            "extract": [
                "scheme",
                "host",
                "port",
                "path",
                "query",
                "fragment",
            ],
        }
    )

    result = df.select(fields=extract_field_from_series("URLs", "extract"))

    expected_df = pl.DataFrame(
        {
            "fields": [
                "https",
                "www.example.com",
                "666",
                "/file/download",
                "key=value",
                "section1",
            ],
        }
    )

    assert_frame_equal(result, expected_df)

    result = df.with_columns(fields=extract_field_from_series("URLs", "extract"))

    expected_df = pl.DataFrame(
        {
            "URLs": [
                "https://web.whatsapp.com/",
                "https://www.example.com:8080/path?query=example#fragment",
                "https://[2001:db8::1]:666/api/v1?query=value",
                "ftp://user:pass@192.168.1.1:21/file/download",
                "http://example.com:80/path/to/resource?key=value#section1",
                "http://example.com:80/path/to/resource?key=value#section1",
            ],
            "extract": [
                "scheme",
                "host",
                "port",
                "path",
                "query",
                "fragment",
            ],
            "fields": [
                "https",
                "www.example.com",
                "666",
                "/file/download",
                "key=value",
                "section1",
            ],
        }
    )

    assert_frame_equal(result, expected_df)


def test_effs_literal_series():
    df = pl.DataFrame(
        {
            "URLs": [
                "https://web.whatsapp.com/",
                "https://www.example.com:8080/path?query=example#fragment",
                "https://[2001:db8::1]:666/api/v1?query=value",
                "ftp://user:pass@192.168.1.1:21/file/download",
                "http://example.com:80/path/to/resource?key=value#section1",
                "http://example.com:80/path/to/resource?key=value#section1",
            ],
            "extract": [
                "scheme",
                "host",
                "port",
                "path",
                "query",
                "fragment",
            ],
        }
    )

    result = df.select(
        fields=extract_field_from_series(
            pl.lit("ftp://localhost:456/index.html#section2"), "extract"
        )
    )

    expected_df = pl.DataFrame(
        {
            "fields": [
                "ftp",
                "localhost",
                "456",
                "/index.html",
                "",
                "section2",
            ],
        }
    )

    assert_frame_equal(result, expected_df)

    result = df.with_columns(
        fields=extract_field_from_series(
            pl.lit("ftp://localhost:456/index.html#section2"), "extract"
        )
    )

    expected_df = pl.DataFrame(
        {
            "URLs": [
                "https://web.whatsapp.com/",
                "https://www.example.com:8080/path?query=example#fragment",
                "https://[2001:db8::1]:666/api/v1?query=value",
                "ftp://user:pass@192.168.1.1:21/file/download",
                "http://example.com:80/path/to/resource?key=value#section1",
                "http://example.com:80/path/to/resource?key=value#section1",
            ],
            "extract": [
                "scheme",
                "host",
                "port",
                "path",
                "query",
                "fragment",
            ],
            "fields": [
                "ftp",
                "localhost",
                "456",
                "/index.html",
                "",
                "section2",
            ],
        }
    )

    assert_frame_equal(result, expected_df)


def test_effs_series_literal():
    df = pl.DataFrame(
        {
            "URLs": [
                "https://web.whatsapp.com/",
                "https://www.example.com:8080/path?query=example#fragment",
                "https://[2001:db8::1]:666/api/v1?query=value",
                "ftp://user:pass@192.168.1.1:21/file/download",
                "http://example.com:80/path/to/resource?key=value#section1",
                "http://example.com:80/path/to/resource?key=value#section1",
            ],
            "extract": [
                "scheme",
                "host",
                "port",
                "path",
                "query",
                "fragment",
            ],
        }
    )

    result = df.select(fields=extract_field_from_series("URLs", pl.lit("host")))

    expected_df = pl.DataFrame(
        {
            "fields": [
                "web.whatsapp.com",
                "www.example.com",
                "[2001:db8::1]",
                "192.168.1.1",
                "example.com",
                "example.com",
            ],
        }
    )

    assert_frame_equal(result, expected_df)

    result = df.with_columns(fields=extract_field_from_series("URLs", pl.lit("host")))

    expected_df = pl.DataFrame(
        {
            "URLs": [
                "https://web.whatsapp.com/",
                "https://www.example.com:8080/path?query=example#fragment",
                "https://[2001:db8::1]:666/api/v1?query=value",
                "ftp://user:pass@192.168.1.1:21/file/download",
                "http://example.com:80/path/to/resource?key=value#section1",
                "http://example.com:80/path/to/resource?key=value#section1",
            ],
            "extract": [
                "scheme",
                "host",
                "port",
                "path",
                "query",
                "fragment",
            ],
            "fields": [
                "web.whatsapp.com",
                "www.example.com",
                "[2001:db8::1]",
                "192.168.1.1",
                "example.com",
                "example.com",
            ],
        }
    )

    assert_frame_equal(result, expected_df)
