import logging
import sys


def setup_logger(name: str = "weather_pipeline") -> logging.Logger:
    """
    Configure and return the application logger.

    The logger writes structured, timestamped logs to stdout so they can be
    captured by orchestrators (Docker, Kubernetes, Airflow, cron redirect, etc).
    File logging is intentionally avoided — stdout is the canonical log sink and
    external systems handle persistence and rotation.

    The function is idempotent: calling it multiple times will not duplicate handlers.

    Args:
        name: Logical logger name used across the application modules.

    Returns:
        Configured Logger instance ready for use.

    Logging Design Decisions:
        - INFO level default: operational visibility without excessive noise
        - stdout handler: container-friendly and platform-agnostic
        - ISO-like timestamps: machine parsable and human readable
        - No global basicConfig(): avoids interfering with libraries
    """
    logger = logging.getLogger(name)

    # Set level every time to guarantee deterministic behavior even if another module configured logging earlier
    logger.setLevel(logging.INFO)

    # Prevent duplicate handlers when modules import setup_logger multiple times. Without this, each import would multiply log lines
    if not logger.handlers:
        handler = logging.StreamHandler(sys.stdout)

        # Format intentionally simple, parse this reliably without custom rules
        formatter = logging.Formatter(
            fmt="%(asctime)s - %(levelname)s - %(message)s",
            datefmt="%Y-%m-%d %H:%M:%S"
        )

        handler.setFormatter(formatter)
        logger.addHandler(handler)

        # Avoid propagation to root logger → prevents duplicate logs if the runtime environment configures logging globally
        logger.propagate = False

    return logger
