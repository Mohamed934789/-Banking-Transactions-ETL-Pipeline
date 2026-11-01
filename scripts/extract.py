"""Compatibility entry point for the validated WebHDFS extractor."""
try:
    from .webhdfs_extract import extract
except ImportError:
    from webhdfs_extract import extract

if __name__ == "__main__":
    extract()
