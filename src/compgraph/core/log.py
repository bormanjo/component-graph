import logging

base_logger = logging.getLogger("component-graph")
core_logger = base_logger.getChild("core")
dependency_logger = core_logger.getChild("dependency")
