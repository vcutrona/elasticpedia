#!/usr/bin/env python

"""The setup script."""

from setuptools import setup, find_packages

with open('README.rst') as readme_file:
    readme = readme_file.read()

with open('HISTORY.rst') as history_file:
    history = history_file.read()

requirements = ['Click>=7.0', 'pyspark==2.4.4']

setup_requirements = []

test_requirements = []

setup(
    author="Vincenzo Cutrona",
    author_email='v.cutrona1@gmail.com',
    python_requires='>=3.6, <3.8',
    classifiers=[
        'Development Status :: 5 - Production/Stable',
        'Intended Audience :: Developers',
        'License :: OSI Approved :: Apache Software License',
        'Natural Language :: English',
        'Programming Language :: Python :: 3',
        'Programming Language :: Python :: 3.6',
        'Programming Language :: Python :: 3.7',
    ],
    description="Indexing DBpedia with Elasticsearch.",
    entry_points={
        'console_scripts': [
            'elasticpedia=elasticpedia.cli:main',
        ],
    },
    install_requires=requirements,
    license="Apache Software License 2.0",
    long_description=readme + '\n\n' + history,
    include_package_data=True,
    keywords='elasticpedia',
    name='elasticpedia',
    packages=find_packages(include=['elasticpedia', 'elasticpedia.*']),
    setup_requires=setup_requirements,
    test_suite='tests',
    tests_require=test_requirements,
    url='https://github.com/vcutrona/elasticpedia',
    version='1.0.0',
    zip_safe=False,
)
