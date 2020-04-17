from dlflow.utils.utilclass import SingletonMeta
from dlflow.utils import DLFLOW_LOCALE

import gettext


class LazyI18nMsg(object):

    def __init__(self, msg, translator=None):
        self._msg = msg
        self._translator = translator

        self._fmt_args = ()
        self._fmt_kwargs = {}

        self._join_list = []

    def __str__(self):
        msg = self._translator(self._msg) if self._translator else self._msg

        if self._fmt_args or self._fmt_kwargs:
            msg = msg.format(*self._fmt_args, **self._fmt_kwargs)

        for sep, strings in self._join_list:
            joins = [msg]
            for s in strings:
                if isinstance(s, str):
                    joins.append(s)
                else:
                    joins.append(str(s))

            msg = sep.join(joins)

        return msg

    def __repr__(self):
        return self.__str__()

    def __add__(self, s):
        self._join("", s)
        return self

    def join(self, sep, *args):
        self._join(sep, *args)
        return self

    def _join(self, sep, *args):
        self._join_list.append((sep, args))

    def format(self, *args, **kwargs):
        self._fmt_args = args
        self._fmt_kwargs = kwargs

        return self


class I18N(metaclass=SingletonMeta):

    def __init__(self):

        self._lang = None
        self._LazyMsg = None
        self._translator = None

    def __call__(self, msg):
        return LazyI18nMsg(msg, self._translator)

    def initialize(self, languages=None):
        if languages is None:
            languages = ["en"]

        if isinstance(languages, str):
            languages = [languages]

        if not isinstance(languages, (list, tuple, set)):
            raise TypeError("")

        self._lang = gettext.translation("msg",
                                         DLFLOW_LOCALE,
                                         languages=languages)
        self._lang.install()
        self._translator = _


i18n = I18N()
