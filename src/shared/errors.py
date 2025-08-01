class AggregatorS3Error(Exception):
    """Errors related to accessing files in S3"""


class AggregatorFilterError(Exception):
    """Errors related to accessing files in S3"""


class AggregatorStudyProcessingError(Exception):
    """Errors related to running studies while others are in process"""
