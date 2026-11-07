"""Embed data.json into the page template.

Usage: python scripts/assemble.py src/template.html data.json site/index.html
"""
import sys

template = open(sys.argv[1], encoding="utf-8").read()
data = open(sys.argv[2], encoding="utf-8").read().replace("</", r"<\/")  # keep </script> out of the JSON
open(sys.argv[3], "w", encoding="utf-8").write(template.replace("__DATA__", data))
