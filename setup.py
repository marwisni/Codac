"""Setup file for application."""
from setuptools import setup, find_packages

setup(
    name='Codac',
    version='0.3',
    author='Mariusz Wiśniowski',
    author_email='mariusz.wisniowski@capgemini.com',
    description='Codac PySpark Assignment. Upskilling task.',    
    package_dir={"": "src"},
    packages=find_packages(where='src'),
    )
