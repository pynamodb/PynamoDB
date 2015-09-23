"""
PynamoDB Throttling (Experimental)
"""
import time
import logging

log = logging.getLogger(__name__)


class ThrottleBase(object):
    """
    A class to provide a throttling API to the user
    """

    def __init__(self, capacity, window=1200, initial_sleep=None):
        self.capacity = float(capacity)
        self.window = window
        self.records = []
        self.sleep_interval = initial_sleep if initial_sleep else 0.1

    def add_record(self, record):
        """
        Adds a ConsumedCapacity record to the dataset over `window`
        """
        if record is None:
            return
        self._slice_records()
        self.records.append({"time": time.time(), "record": record})

    def _slice_records(self):
        idx = 0
        now = time.time()
        for record in self.records:
            if now - record['time'] < self.window:
                break
            else:
                idx += 1
        self.records = self.records[idx:]

    def throttle(self):
        """
        Sleeps for the appropriate period of time, based on the current data
        """
        return


class NoThrottle(ThrottleBase):
    """
    The default throttle class, does nothing
    """

    def __init__(self):
        pass

    def add_record(self, record):
        pass


class Throttle(ThrottleBase):
    """
    The default throttling

    This class will aggressively throttle API calls if the throughput for a given window is over
    the desired capacity.

    If the throughput is under the desired capacity, then API throttling will be reduced cautiously.
    """

    def throttle(self):
        """
        This uses a method similar to additive increase, multiplicative decrease

        # http://en.wikipedia.org/wiki/Additive_increase/multiplicative_decrease
        """
        if not len(self.records) >= 2:
            return
        throughput = sum([value['record'] for value in self.records]) / float(time.time() - self.records[0]['time'])

        # Over capacity
        if throughput > self.capacity:
            self.sleep_interval *= 2
        # Under capacity
        elif throughput < (.9 * self.capacity) and self.sleep_interval > 0.1:
            self.sleep_interval -= self.sleep_interval * .10
        log.debug("Sleeping for %ss, current throughput is %s and desired throughput is %s",
                  self.sleep_interval, throughput, self.capacity)
        time.sleep(self.sleep_interval)
