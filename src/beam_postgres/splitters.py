from abc import ABCMeta, abstractmethod

from apache_beam.io import iobase
from apache_beam.io.range_trackers import (
    LexicographicKeyRangeTracker,
    OffsetRangeTracker,
    UnsplittableRangeTracker,
)


class BaseSplitter(metaclass=ABCMeta):
    """Abstract class of splitter."""

    def build_source(self, source):
        """Build source on runtime."""
        self.source = source

    @abstractmethod
    def estimate_size(self):
        """Wrap :class:`~apache_beam.io.iobase.BoundedSource.estimate_size`"""
        raise NotImplementedError()

    @abstractmethod
    def get_range_tracker(self, start_position, stop_position):
        """Wrap :class:`~apache_beam.io.iobase.BoundedSource.get_range_tracker`"""
        raise NotImplementedError()

    @abstractmethod
    def read(self, range_tracker):
        """Wrap :class:`~apache_beam.io.iobase.BoundedSource.read`"""
        raise NotImplementedError()

    def split(self, desired_bundle_size, start_position=None, stop_position=None):
        """Wrap :class:`~apache_beam.io.iobase.BoundedSource.split`"""
        raise NotImplementedError()


class NoSplitter(BaseSplitter):
    """No split bounded source so not work parallel."""

    def estimate_size(self):
        return self.source.client.rough_counts_estimator(self.source.query)

    def get_range_tracker(self, start_position, stop_position):
        if start_position is None:
            start_position = 0
        if stop_position is None:
            stop_position = OffsetRangeTracker.OFFSET_INFINITY

        return UnsplittableRangeTracker(
            OffsetRangeTracker(start_position, stop_position)
        )

    def read(self, range_tracker):
        for record in self.source.client.record_generator(self.source.query):
            yield record

    def split(self, desired_bundle_size, start_position=None, stop_position=None):
        if start_position is None:
            start_position = 0
        if stop_position is None:
            stop_position = OffsetRangeTracker.OFFSET_INFINITY

        yield iobase.SourceBundle(
            weight=desired_bundle_size,
            source=self.source,
            start_position=start_position,
            stop_position=stop_position,
        )


class QuerySplitter(BaseSplitter):
    """Split bounded source by where clause."""

    def __init__(self, sep: str = "--sep--"):
        self._sep = sep

    def estimate_size(self):
        self._validate_query()
        query = self.source.query
        query = query.replace(";", "")
        union_query = " union all ".join(query.split(self._sep))
        return self.source.client.rough_counts_estimator(union_query)

    def get_range_tracker(self, start_position, stop_position):
        self._validate_query()
        return LexicographicKeyRangeTracker(start_position, stop_position)

    def read(self, range_tracker):
        if range_tracker.start_position() is None:
            query = self.source.query
        else:
            query, _ = (
                range_tracker.start_position(),
                range_tracker.stop_position(),
            )
        for record in self.source.client.record_generator(query):
            yield record

    def split(self, desired_bundle_size, start_position=None, stop_position=None):
        self._validate_query()

        for query in self.source.query.split(self._sep):
            yield iobase.SourceBundle(
                weight=desired_bundle_size,
                source=self.source,
                start_position=query,
                stop_position=None,
            )

    def _validate_query(self):
        if self._sep not in self.source.query:
            raise ValueError(
                f"separator not found in query.: {self.source.query}, sep: '{self._sep}'"
            )

        queries = self.source.query.replace(";", "").split(self._sep)
        for query in queries:
            if "where" not in query.lower():
                raise ValueError(f"Require 'where' phrase on query: {query}")
