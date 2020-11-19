import sys
from pathlib import Path

import click

sys.path.append(str(Path(__file__).resolve().parents[1]))
from support import settings
from support import data_tools as dtools

@click.command()
@click.option('--outfile', '-o', default=settings.UNIQUE_FILES_TABLE, show_default=True)
@click.option('--nrows', '-n', default=None)
def main(outfile, nrows):

    if outfile == settings.UNIQUE_FILES_TABLE:
        if not click.confirm(f"Overwrite the existing table at {outfile} ?"):
            return

    if nrows:
        nrows = int(nrows)

    dtools.generate_unique_filepaths(outfile, to_file=True, n=nrows)
    print(f"File output to {Path(outfile).resolve()}")

if __name__ == '__main__':
    main()
