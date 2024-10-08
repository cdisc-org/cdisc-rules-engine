import logging
from cdisc_rules_engine.interfaces import ConfigInterface, LoggerInterface
import traceback
import inspect


class ConsoleLogger(LoggerInterface):
    @classmethod
    def get_instance(cls, config: ConfigInterface):
        logger = logging.getLogger()
        return cls(logger, config)

    def __init__(self, logger, config: ConfigInterface):
        self._logger = logger
        self._config = config
        self._exception = Exception()

    @property
    def disabled(self) -> bool:
        return self._logger.disabled

    @disabled.setter
    def disabled(self, value: bool):
        self._logger.disabled = value

    def setLevel(self, level: str):
        levels = {
            "info": logging.INFO,
            "debug": logging.DEBUG,
            "error": logging.ERROR,
            "critical": logging.CRITICAL,
            "warn": logging.WARNING,
            "verbose": logging.CRITICAL + 1,
        }
        if isinstance(level, str):
            level = levels.get(level.lower(), logging.ERROR)
        self._logger.setLevel(level)

    def debug(self, msg: str, *args, **kwargs):
        self._logger.debug(msg, *args, **kwargs)

    def info(self, msg: str, *args, **kwargs):
        self._logger.info(msg, *args, **kwargs)

    def warning(self, msg: str, *args, **kwargs):
        self._logger.warning(msg, *args, **kwargs)

    def error(self, msg: str = None, *args, **kwargs):
        self._logger.error(msg, *args, **kwargs)

    def exception(self, msg: str, *args, **kwargs):
        self._logger.exception(msg, *args, **kwargs)

    def critical(self, msg: str, *args, **kwargs):
        self._logger.critical(msg, *args, **kwargs)

    def log(self, msg: str, *args, **kwargs):
        self._logger.log(logging.CRITICAL + 1, msg, *args, **kwargs)

    def trace(self, exc: Exception, msg: str, *args, **kwargs):
        current_level = self._logger.getEffectiveLevel()
        if current_level > 50:
            self.display_trace(exc, inspect.currentframe())

    def display_trace(self, e: Exception = None, f=None):
        if e is None:
            e = self._exception
        if f is None:
            f = inspect.currentframe()
        # print out trace information
        current_line = f.f_lineno
        print(f"\n Current: {__name__}, line {current_line}")

        frame = f.f_back
        c_function = inspect.getframeinfo(frame).function
        c_lineno = frame.f_lineno
        c_filename = frame.f_code.co_filename
        print(f"  Caller: in {c_filename}: line {c_lineno} in {c_function}")

        line_number = None
        caller_function = None
        traceback_info = traceback.extract_tb(e.__traceback__)
        if traceback_info:
            line_number = traceback_info[-1].lineno
            caller_function = traceback_info[-1].name
        print(f"   Error: {str(e)}, line {line_number} in {caller_function}")
        i = 0
        for call_func in traceback_info:
            i += 1
            print(f"Trace({i}): {call_func}")
