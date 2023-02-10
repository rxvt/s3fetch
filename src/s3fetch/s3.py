from queue import Queue
from .exceptions import S3FetchQueueEmpty


class S3FetchQueue:
    def __init__(self):
        self.queue = Queue()

    def put(self, key: str) -> None:
        self.queue.put_nowait(key)

    # TODO: Remove this after refactor, should only use self.put()
    def put_nowait(self, key: str) -> None:
        self.queue.put_nowait(key)

    def get(self, block: bool = False) -> str:
        key = self.queue.get(block=block)
        if key is None:
            raise S3FetchQueueEmpty
        return key

    def close(self) -> None:
        self.queue.put_nowait(None)


def get_download_queue() -> S3FetchQueue:
    queue = S3FetchQueue()
    return queue

