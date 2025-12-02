# Source - https://stackoverflow.com/questions/16261902/any-way-to-get-one-process-to-have-a-write-lock-and-others-to-just-read-on-paral
# Posted by sgarg
# Retrieved 2025-12-02, License - CC BY-SA 4.0
#
# Originally from O'Reilly Python Cookbook by David Ascher, Alex Martelli
# with changes to cover the starvation situation where a continuous
# stream of readers may starve a writer, Lock Promotion and Context Managers
#
# Adapted for our use case (multiprocessing instead of threading)

import multiprocessing


class ReadWriteLock:
    """ A lock object that allows many simultaneous "read locks", but
    only one "write lock." """

    def __init__(self, withPromotion=False):
        self._read_ready = multiprocessing.Condition(multiprocessing.RLock())
        self._readers = 0
        self._writers = 0
        self._promote = withPromotion
        self._readerList = []  # List of Reader thread IDs
        self._writerList = []  # List of Writer thread IDs

    def acquire_read(self):
        """ Acquire a read lock. Blocks only if a thread has
        acquired the write lock. """
        self._read_ready.acquire()
        try:
            while self._writers > 0:
                self._read_ready.wait()
            self._readers += 1
        finally:
            self._readerList.append(multiprocessing.current_process()._identity)
            self._read_ready.release()

    def release_read(self):
        """ Release a read lock. """
        self._read_ready.acquire()
        try:
            self._readers -= 1
            if not self._readers:
                self._read_ready.notify_all()
        finally:
            self._readerList.remove(multiprocessing.current_process()._identity)
            self._read_ready.release()

    def acquire_write(self):
        """ Acquire a write lock. Blocks until there are no
        acquired read or write locks. """
        self._read_ready.acquire()  # A re-entrant lock lets a thread re-acquire the lock
        self._writers += 1
        self._writerList.append(multiprocessing.current_process()._identity)
        while self._readers > 0:
            # promote to write lock, only if all the readers are trying to promote to writer
            # If there are other reader threads, then wait till they complete reading
            if self._promote and multiprocessing.current_process()._identity in self._readerList and set(self._readerList).issubset(set(self._writerList)):
                break
            else:
                self._read_ready.wait()

    def release_write(self):
        """ Release a write lock. """
        self._writers -= 1
        self._writerList.remove(multiprocessing.current_process()._identity)
        self._read_ready.notify_all()
        self._read_ready.release()


# ----------------------------------------------------------------------------------------------------------

class ReadRWLock:
    # Context Manager class for ReadWriteLock
    def __init__(self, rwLock):
        self.rwLock = rwLock

    def __enter__(self):
        self.rwLock.acquire_read()
        return self  # Not mandatory, but returning to be safe

    def __exit__(self, exc_type, exc_value, traceback):
        self.rwLock.release_read()
        return False  # Raise the exception, if exited due to an exception


# ----------------------------------------------------------------------------------------------------------

class WriteRWLock:
    # Context Manager class for ReadWriteLock
    def __init__(self, rwLock):
        self.rwLock = rwLock

    def __enter__(self):
        self.rwLock.acquire_write()
        return self  # Not mandatory, but returning to be safe

    def __exit__(self, exc_type, exc_value, traceback):
        self.rwLock.release_write()
        return False  # Raise the exception, if exited due to an exception

# ----------------------------------------------------------------------------------------------------------
