"""Docstring"""
from setuptools import setup, find_packages

setup(
    name='Codac',
    version='0.1',
    author='Mariusz Wiśniowski',
    author_email='mariusz.wisniowski@capgemini.com',
    description='Codac assignment',
    long_description='TODO long_description',
    package_dir={"": "src"},
    packages=find_packages(where='src'),
    )
