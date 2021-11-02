#!/usr/bin/env python
# -*- coding: utf-8 -*-

import logging
from utils import (
    get_logger,
    start_logging
)

MODULE_NAME = "Testing"

logger = get_logger(MODULE_NAME)
start_logging("log")

logger.info("This is info")
logger.warn("a warning")
logger.error("some error")
logger.debug("This is debug")
logger.verbose("verbose log")


