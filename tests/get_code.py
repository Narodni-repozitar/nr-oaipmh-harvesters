import click
from pathlib import Path
import gzip
import yaml
import json
import re


@click.command()
@click.argument("dir")
@click.argument("code")
@click.option("--exclude", multiple=True)
def get_code(dir, code, exclude):
    rg = re.compile(f"^{code}.*")
    exclude = [re.compile(f"^{e}.*") for e in exclude]
    for fn in sorted(Path(dir).glob("*.yaml.gz")):
        with gzip.open(fn, "rt") as f:
            data = yaml.safe_load(f)
            for ent in data:
                for k, v in ent["entry"].items():
                    if rg.match(k):
                        excluded = False
                        for e in exclude:
                            if e.match(k):
                                excluded = True
                                break
                        if not excluded:
                            print(f"{k:10s} - {v}")


if __name__ == "__main__":
    get_code()
