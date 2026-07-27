# Vendored dependency (CI only)

This tree is a copy of the three `python_tools_and_shortcuts` modules that
`etl/`, `flows/`, and `eda/` import at module scope
(`aws.secrets_manager.get_secret`, `databases.influxdb.InfluxDbTool`,
`time_series_essentials.time_conversions`). That package lives in the
private `badass-data-science/python-tools-and-shortcuts` repo and isn't
published anywhere pip can reach, so CI can't `pip install` it directly.

It's added to `PYTHONPATH` only inside the GitHub Actions workflow
(`.github/workflows/ci.yml`) — local development is unaffected and continues
to use the real package via the developer's own `PYTHONPATH`.

If `get_secret`, `InfluxDbTool`, or `time_conversions` change upstream, copy
the updated file(s) here to keep CI green.
