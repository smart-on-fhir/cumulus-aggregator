class AggregatorAthenaError(Exception):
    """Errors related to accessing data in Athena"""


class AggregatorS3Error(Exception):
    """Errors related to accessing files in S3"""


class AggregatorFilterError(Exception):
    """Errors related to SQL filters"""


class AggregatorStudyProcessingError(Exception):
    """Errors related to running studies while others are in process"""


class S3UploadError(Exception):
    """Errors related to malformed uploads"""
