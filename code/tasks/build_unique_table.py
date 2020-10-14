import sys
from pathlib import Path

import click

sys.path.append(str(Path(__file__).resolve().parents[1]))
from support import settings
from support import fjc
from support import data_tools as dt

@click.command()
@click.option('--infile', '-i', default=settings.UNIQUE_FILES_LIST, show_default=True)
@click.option('--outfile', '-o', default=settings.UNIQUE_FILES_TABLE, show_default=True)
@click.option('--nrows', '-n', default=None)
def main(infile, outfile, nrows):

    if outfile == settings.UNIQUE_FILES_TABLE:
        if not click.confirm(f"Overwrite the existing table at {outfile} ?"):
            return

    if nrows:
        nrows = int(nrows)

    dt.convert_filepaths_list(infile, outfile, to_file=True, nrows=nrows)
    print(f"File output to {Path(outfile).resolve()}")

if __name__ == '__main__':
    main()
