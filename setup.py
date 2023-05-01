from setuptools import setup, find_packages
from glob import glob

from pathlib import Path
base_dir = Path(__file__).parent
long_description = (base_dir / "README.md").read_text()

setup(
	name='pacer-tools',
	version='0.1.8',
    long_description=long_description,
    long_description_content_type='text/markdown',
	package_dir={'': 'src'},
	packages=find_packages('src'),
	install_requires=[
        'async-generator', 'attrs', 'beautifulsoup4', 'bs4', 
        'cchardet', 'cffi', 'chardet', 'charset-normalizer', 
        'click', 'configuration-maker', 'cryptography', 
        'cssselect', 'feedparser', 'filelock', 'future', 
        'geonamescache', 'h11', 'html5lib', 'idna',
        'lxml', 'numpy', 'outcome', 'pandas', 'pathlib', 
        'probableparsing', 'pycparser', 'pyOpenSSL', 'PySocks', 
        'python-crfsuite', 'python-dateutil', 'python-dotenv', 
        'pytz', 'requests', 'requests-file', 'scipy', 'selenium', 
        'selenium-requests', 'sgmllib3k', 'six', 'sniffio', 
        'sortedcontainers', 'soupsieve', 'tldextract', 
        'tqdm', 'trio', 'trio-websocket', 'urllib3', 
        'urllib3-secure-extra', 'usaddress', 'webencodings', 
        'wsproto', 'xmltodict'
    ],
	entry_points={
		'console_scripts': [
			'pacer-tools = pacer_tools:cli',
		],
	},
    data_files=[
        ('pacer_tools', glob('src/pacer_tools/code/support/core_data/*')),
        ('pacer_tools', glob('src/pacer_tools/data/annotation/*'))
    ],
    include_package_data = True,
)
