import logging
import structlog
import sys
import logging
from pythonjsonlogger import jsonlogger

def get_struct_logger():
    # Configure structlog processors
    processors = [
        structlog.stdlib.add_log_level,
        structlog.stdlib.add_logger_name,
        structlog.stdlib.PositionalArgumentsFormatter(),
        structlog.processors.StackInfoRenderer(),
        structlog.processors.format_exc_info,
        structlog.processors.TimeStamper(fmt="iso"),
        structlog.processors.JSONRenderer(),  # For JSON output
    ]

    # Configure console formatting
    console_processors = [
        structlog.stdlib.add_log_level,
        structlog.stdlib.add_logger_name,
        structlog.stdlib.PositionalArgumentsFormatter(),
        structlog.processors.StackInfoRenderer(),
        structlog.processors.format_exc_info,
        structlog.processors.TimeStamper(fmt="iso"),
        structlog.dev.ConsoleRenderer(),  # For console output
    ]

    # Configure logging handlers
    logging.basicConfig(stream=sys.stdout, level=logging.INFO)

    # Create a logger for JSON output
    json_handler = logging.StreamHandler(sys.stdout)
    json_formatter = jsonlogger.JsonFormatter()
    json_handler.setFormatter(json_formatter)

    # Create a logger for console output
    console_handler = logging.StreamHandler(sys.stderr) # or sys.stdout depending on preference
    console_formatter = structlog.stdlib.ProcessorFormatter(
        processor=structlog.dev.ConsoleRenderer(),
        foreign_pre_chain=console_processors,
    )
    console_handler.setFormatter(console_formatter)

    # Configure structlog to use the handlers
    structlog.configure(
        processors=processors,
        logger_factory=structlog.stdlib.LoggerFactory(),
        wrapper_class=structlog.stdlib.BoundLogger,
        #cache_logger_on_first_access=True,
    )

    # Get a logger instance
    return structlog.get_logger()