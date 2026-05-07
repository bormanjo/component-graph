import logging

base_logger = logging.getLogger("modulon")
graph_logger = base_logger.getChild("graph")
core_logger = base_logger.getChild("core")
dependency_logger = core_logger.getChild("dependency")
