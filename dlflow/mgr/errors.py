
class BaseError(Exception):

    def __init__(self, err_info=""):
        super(BaseError, self).__init__(self)
        self.err_info = err_info

    def __str__(self):
        return str(self.err_info)


class InstantiateNotAllowed(BaseError):
    pass


class FileTypeNotSupport(BaseError):
    pass


class ParameterError(BaseError):
    pass


class RegisterKeyDuplicate(BaseError):
    pass


class NotInitializeError(BaseError):
    pass


class FmapNotExists(BaseError):
    pass


class CircularReferences(BaseError):
    pass


class ParserNotExists(BaseError):
    pass


class FeatureEncodeError(BaseError):
    pass


class FeatureNotBind(BaseError):
    pass
