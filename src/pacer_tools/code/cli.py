import click
from pacer_tools.code.downloader.scrapers import scraper
from pacer_tools.code.parsers.parse_pacer import parser


@click.group()
def main():
    pass

main.add_command(scraper)
main.add_command(parser)


if __name__ == '__main__':
    main()