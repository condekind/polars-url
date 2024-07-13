import polars as pl
from polars.testing import assert_frame_equal
from polars_url import parse_url


def test_parse_url():
    df = pl.DataFrame(
        {
            "URLs": [
                "https://web.whatsapp.com/",
                "https://www.example.com:8080/path?query=example#fragment",
                "https://[2001:db8::1]:443/api/v1?query=value",
                "ftp://user:pass@192.168.1.1:21/file/download",
                "http://example.com:80/path/to/resource?key=value#section1",
                "good afternoon, good evening and good night\\sj@20.,[",
            ],
        }
    )

    result = (
        df.with_columns(scheme=parse_url("URLs", field="scheme"))
        .with_columns(username=parse_url("URLs", field="username"))
        .with_columns(host=parse_url("URLs", field="host"))
        .with_columns(port=parse_url("URLs", field="port"))
        .with_columns(path=parse_url("URLs", field="path"))
        .with_columns(query=parse_url("URLs", field="query"))
        .with_columns(fragment=parse_url("URLs", field="fragment"))
    )

    expected_df = pl.DataFrame(
        {
            "URLs": [
                "https://web.whatsapp.com/",
                "https://www.example.com:8080/path?query=example#fragment",
                "https://[2001:db8::1]:443/api/v1?query=value",
                "ftp://user:pass@192.168.1.1:21/file/download",
                "http://example.com:80/path/to/resource?key=value#section1",
                "good afternoon, good evening and good night\\sj@20.,[",
            ],
            "scheme": ["https", "https", "https", "ftp", "http", ""],
            "username": ["", "", "", "user", "", ""],
            "host": [
                "web.whatsapp.com",
                "www.example.com",
                "[2001:db8::1]",
                "192.168.1.1",
                "example.com",
                "",
            ],
            "port": ["", "8080", "", "", "", ""],
            "path": [
                "/",
                "/path",
                "/api/v1",
                "/file/download",
                "/path/to/resource",
                "",
            ],
            "query": ["", "query=example", "query=value", "", "key=value", ""],
            "fragment": ["", "fragment", "", "", "section1", ""],
        }
    )

    assert_frame_equal(result, expected_df)
