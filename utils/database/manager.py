import logging
from .adapter import DatabaseManager as DatabaseAdapter

logger = logging.getLogger(__name__)

# Backward compatibility: use the shared adapter by default
DatabaseManager = DatabaseAdapter
