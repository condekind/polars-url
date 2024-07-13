import polars as pl
from polars_url import extract_field_from_series, parse_url


with pl.Config(tbl_rows=-1, tbl_cols=-1, fmt_str_lengths=80):
    df = pl.DataFrame({
        'URLs': [
            'https://web.whatsapp.com/',
            'https://www.example.com:8080/path?query=example#fragment',
            'https://[2001:db8::1]:443/api/v1?query=value',
            'ftp://user:pass@192.168.1.1:21/file/download',
            'http://example.com:80/path/to/resource?key=value#section1',
            'in case I dont see ya\\asd; foo',
            'good afternoon, good evening and good night\\sj@20.,[',
        ],
    })
    result = (df
        .with_columns(scheme=parse_url('URLs', field="scheme"))
        .with_columns(username=parse_url('URLs', field="username"))
        .with_columns(host=parse_url('URLs', field="host"))
        .with_columns(port=parse_url('URLs', field="port"))
        .with_columns(path=parse_url('URLs', field="path"))
        .with_columns(query=parse_url('URLs', field="query"))
        .with_columns(fragment=parse_url('URLs', field="fragment"))
    )
    print(result)




with pl.Config(tbl_rows=-1, tbl_cols=-1, fmt_str_lengths=80):
    df = pl.DataFrame({
        'URLs': [
            'https://web.whatsapp.com/',
            'https://www.example.com:8080/path?query=example#fragment',
            'https://[2001:db8::1]:666/api/v1?query=value',
            'ftp://user:pass@192.168.1.1:21/file/download',
            'http://example.com:80/path/to/resource?key=value#section1',
            'http://example.com:80/path/to/resource?key=value#section1',
        ],
        'extract': [
            'scheme',
            'host',
            'port',
            'path',
            'query',
            'fragment',
        ]
    })
    result = (df
        .with_columns(fields=extract_field_from_series('URLs', 'extract'))
    )
    print(result)

