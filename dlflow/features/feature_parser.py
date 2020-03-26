from dlflow.mgr.errors import ParserNotExists
from dlflow.utils.locale import i18n


class Parser(object):
    """
    Factory for parser.
    """

    def __init__(self, parser_name):
        self._parser = None
        self._normalizer = None

        initializer = {
            "spark": self._spark_parser_init
        }.get(parser_name.lower(), None)

        if initializer is None:
            raise ParserNotExists(
                i18n("Unknown feature parser '{}'").format(parser_name))

        initializer()

    def _spark_parser_init(self):
        from dlflow.features.parser_spark import SparkFeParser
        from dlflow.features.parser_spark import SparkNormalizer

        self._parser = SparkFeParser
        self._normalizer = SparkNormalizer

    def get_parser(self):
        return self._parser

    def get_normalizer(self):
        return self._normalizer
