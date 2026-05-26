# API reference

The API reference is generated from docstrings with `pdoc` as part of the docs
build.

<a href="../../api/">Open the generated API reference</a>

Local build:

```bash
uv run mkdocs build --strict --site-dir site
uv run pdoc ttasks --output-directory site/api
```
