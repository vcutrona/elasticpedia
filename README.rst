============
Elasticpedia
============


.. |pypi|   image:: https://img.shields.io/pypi/v/elasticpedia.svg
            :target: https://pypi.python.org/pypi/elasticpedia

.. |build|  image:: https://img.shields.io/travis/vcutrona/elasticpedia.svg
            :target: https://travis-ci.org/vcutrona/elasticpedia

.. |docs|   image:: https://readthedocs.org/projects/elasticpedia/badge/?version=latest
            :target: https://elasticpedia.readthedocs.io/en/latest/?badge=latest
            :alt: Documentation Status

.. |DOI|    image:: https://zenodo.org/badge/233836358.svg
            :target: https://zenodo.org/badge/latestdoi/233836358

|pypi| |build| |docs| |DOI|


Indexing DBpedia with Elasticsearch.


* Free software: Apache Software License 2.0
* Documentation: https://elasticpedia.readthedocs.io.


Features
--------

* Create an ElasticSearch index with DBpedia entities.
* Redirected entities can be filtered out by providing them separately.
* Current version supports only .ttl files.

Credits
-------
If you use this package for your research work, please cite:

    Cutrona, Vincenzo. (2020, March 30). Elasticpedia v1.0.0 (Version v1.0.0). Zenodo. |DOI|

This package was derived from the DBpedia indexer provided by the `DBpedia Lookup project`_.

This package was created with Cookiecutter_ and the `audreyr/cookiecutter-pypackage`_ project template.

.. _DBpedia Lookup project: https://github.com/dbpedia/lookup
.. _Cookiecutter: https://github.com/audreyr/cookiecutter
.. _`audreyr/cookiecutter-pypackage`: https://github.com/audreyr/cookiecutter-pypackage
