import logging
import pathlib as plb
import re

import typer

REGEX_PATTERN = "(__version__|version).*(?=\n)"

logger = logging.getLogger("bump_version")
handler = logging.StreamHandler()
format = logging.Formatter("%(name)s - %(levelname)s - %(message)s")
handler.setFormatter(format)
logger.addHandler(handler)
logger.setLevel(logging.INFO)

app = typer.Typer()


def _has_version(inp: str) -> bool:
    return re.search(REGEX_PATTERN, inp) is not None


def _replace_version(inp: str, version: str) -> str:
    if "__version__" in inp:
        return re.sub(REGEX_PATTERN, f'__version__ = "{version}"', inp)
    else:
        return re.sub(REGEX_PATTERN, f'version = "{version}"', inp)


def _get_version_files(root: plb.Path, glob: str) -> list[plb.Path]:
    paths_ = []
    for path in glob.split(","):
        paths_ += [f for f in root.glob(path)]
    return paths_


def _read_content(path: plb.Path) -> str:
    with path.open("r") as f:
        return f.read()


def _write_content(path: plb.Path, content: str):
    with path.open("w") as f:
        f.write(content)


def _set_version_string(root: plb.Path, glob: str, version: str, dry_run: bool = False):
    for init in _get_version_files(root=root, glob=glob):
        c = _read_content(init)
        if not _has_version(c):
            logger.debug(f"No version string found in {str(init)}")
            continue
        logger.info(f"Setting version string in {str(init)} to version={version}")
        c_ = _replace_version(c, version)
        if not dry_run:
            logger.debug(f"Writing content to {str(init)}")
            _write_content(init, c_)


@app.command()
def bump_version(
    version: str = typer.Argument(
        ...,
        help="Version to set",
    ),
    root: str = typer.Argument(
        ...,
        help="Path to root of project",
    ),
    glob: str = typer.Argument(
        ...,
        help="Glob pattern for files to change",
    ),
    dry_run: bool = typer.Option(
        False,
        help="Dry run, do not make changes",
    ),
    debug: bool = typer.Option(
        False,
        help="Debug mode",
    ),
):
    if debug:
        logger.setLevel(logging.DEBUG)
    if dry_run:
        logger.info("This is a dry run, not making changes")
    root_ = plb.Path(root).resolve()
    logger.info(f"Base directory {str(root_)}")
    _set_version_string(root=root_, glob=glob, version=version, dry_run=dry_run)


if __name__ == "__main__":
    app()
