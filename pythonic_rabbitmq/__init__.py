import logging
import os
import pickle
import queue
import random
import threading
import time
from typing import Any, Callable, Optional, Union

import blobfile as bf
import pika

rnd = random.Random()

# make really sure each process has a different seed. mix in other sources to be sure
rnd.seed(rnd.randint(0, 2**31) + os.getpid() + int.from_bytes(os.urandom(16), "big"))


threadLocal = threading.local()


def get_pika_connection(url):
    import pika
    import pika.exceptions

    logging.getLogger("pika").setLevel(logging.WARNING)  # reduce logspam

    if url in threadLocal._pika_connections:
        return threadLocal._pika_connections[url]

    print("making pika connection", threading.current_thread(), os.getpid())
    connection = pika.BlockingConnection(pika.ConnectionParameters(host=url))
    threadLocal._pika_connections[url] = connection

    return connection


def get_pika_channel(url):
    import pika
    import pika.exceptions

    if not hasattr(threadLocal, "_pika_connections"):
        threadLocal._pika_connections = {}
        threadLocal._pika_channels = {}

    if url in threadLocal._pika_channels:
        return threadLocal._pika_channels[url]
    connection = get_pika_connection(url)
    try:
        channel = connection.channel()
    except (pika.exceptions.ConnectionWrongStateError, pika.exceptions.ConnectionClosedByBroker):
        threadLocal._pika_connections.pop(url, None)
        connection = get_pika_connection(url)
        channel = connection.channel()

    threadLocal._pika_channels[url] = channel
    return channel


class RMQBase:
    def __init__(self, url: str, queue_name: Optional[str] = None):
        self.url = url
        self.queue_name = queue_name or f"queue-{rnd.randbytes(8).hex()}"
        self._is_setup = False

    def _setup_rmq(self, force_recreate=False) -> None:
        if self._is_setup and not force_recreate:
            return

        if force_recreate:
            threadLocal._pika_channels.pop(self.url, None)

        channel = get_pika_channel(self.url)

        channel.queue_declare(
            queue=self.queue_name,
        )

        self._channel = channel
        self.queue_name = self.queue_name
        self._is_setup = True

    def _channel_method(self, method, *args, **kwargs):
        from pika.exceptions import (
            ChannelClosedByBroker,
            ChannelWrongStateError,
            ConnectionWrongStateError,
            StreamLostError,
        )

        try:
            self._setup_rmq()
            return getattr(self._channel, method)(*args, **kwargs)
        except (
            ChannelWrongStateError,
            StreamLostError,
            ConnectionResetError,
            ConnectionWrongStateError,
            ChannelClosedByBroker,
        ):
            print(f"channel [{self.queue_name}] wrong state, retrying...")
            self._is_setup = False
            self._setup_rmq(force_recreate=True)
            return self._channel_method(method, *args, **kwargs)


class RemoteQueue(RMQBase):
    def __init__(self, url: str, queue_name: Optional[str] = None, maxsize: Optional[int] = None):
        super().__init__(url, queue_name)
        self.maxsize = maxsize

    def __getstate__(self) -> dict:
        return {
            "url": self.url,
            "queue_name": self.queue_name,
            "maxsize": self.maxsize,
        }

    def __setstate__(self, state: dict) -> None:
        self.url = state["url"]
        self.queue_name = state["queue_name"]
        self.maxsize = state["maxsize"]
        self._is_setup = False

    def qsize(self) -> int:
        self._setup_rmq()
        return self._channel.queue_declare(queue=self.queue_name, passive=True).method.message_count

    def put(
        self,
        data: Any,
        block: bool = True,
        timeout: Optional[float] = None,
        ttl: Optional[int] = None,
    ) -> None:  # TODO implement timeout and make sure semantics are right
        import pika

        assert timeout is None, "timeout not implemented yet"

        if self.maxsize is not None:
            while self.qsize() >= self.maxsize:
                if not block:
                    raise queue.Full()
                time.sleep(0.1)

        import cloudpickle

        self._channel_method(
            "basic_publish",
            exchange="",
            routing_key=self.queue_name,
            body=cloudpickle.dumps(data),
            properties=pika.BasicProperties(
                expiration=str(ttl * 1000) if ttl is not None else None
            ),
        )

    def get(self, block: bool = True, timeout: Optional[float] = None) -> Any:
        import cloudpickle

        start = time.time()
        while timeout is None or time.time() - start < timeout:
            method_frame, header_frame, body = self._channel_method(
                "basic_get", queue=self.queue_name, auto_ack=True
            )
            if body is None:
                if not block:
                    raise queue.Empty()

                time.sleep(0.1)
                continue

            ob = cloudpickle.loads(body)

            return ob

        raise queue.Empty()

    def empty(self) -> bool:
        return self.qsize() == 0

    def full(self) -> bool:
        if self.maxsize is None:
            return False
        return self.qsize() >= self.maxsize


class RPCServer(RMQBase):
    def __init__(self, url: str, queue_name: Optional[str] = None):
        super().__init__(url, queue_name)
        self.registered_methods: dict[str, Callable] = {}

    def _get_request(self) -> Union[tuple[Any, str], tuple[None, None]]:
        method_frame, header_frame, body = self._channel_method(
            "basic_get", queue=self.queue_name, auto_ack=True
        )

        if body is None:
            return None, None

        reply_to = header_frame.reply_to

        import cloudpickle

        ob = cloudpickle.loads(body)

        return ob, reply_to

    def _handle_request(self, method: str, args: list[Any], kwargs: dict[str, Any]) -> Any:
        if method in self.registered_methods:
            assert not hasattr(self, method)
            return self.registered_methods[method](*args, **kwargs)

        return getattr(self, method)(*args, **kwargs)

    def serve(self, self_destruct: Optional[int] = None) -> None:
        last_request = time.time()
        while True:
            request, reply_to = self._get_request()
            if request is None:
                if self_destruct is not None and time.time() - last_request > self_destruct:
                    return

                time.sleep(0.1)
                continue

            last_request = time.time()

            method, args, kwargs = request

            res: dict[str, Any] = {
                "result": None,
                "error": None,
            }

            try:
                res["result"] = self._handle_request(method, args, kwargs)
            except Exception as err:
                res["error"] = err

            import cloudpickle

            print(
                self._channel_method(
                    "basic_publish",
                    exchange="",
                    routing_key=reply_to,
                    body=cloudpickle.dumps(res),
                )
            )

    def register(self, func: Callable) -> Callable:
        self.registered_methods[func.__name__] = func
        return func


class RPCClient(RMQBase):
    def __init__(self, url: str, queue_name: Optional[str] = None):
        super().__init__(url, queue_name)
        self._return_value = None
        self._lock = threading.Lock()  # only one thread should be able to use the client at a time.

    def call(self, method: str, *args: Any, **kwargs: Any):
        with self._lock:
            import cloudpickle

            def on_client_rx_reply_from_server(ch, method_frame, properties, body):
                try:
                    self._return_value = cloudpickle.loads(body)
                except (AttributeError, ImportError, TypeError) as err:
                    self._return_value = {"result": None, "error": err}

                self._channel_method("stop_consuming")

            self._channel_method(
                "basic_consume",
                "amq.rabbitmq.reply-to",
                on_client_rx_reply_from_server,
                auto_ack=True,
            )

            self._channel_method(
                "basic_publish",
                exchange="",
                routing_key=self.queue_name,
                body=cloudpickle.dumps((method, args, kwargs)),
                properties=pika.BasicProperties(
                    reply_to="amq.rabbitmq.reply-to",
                ),
            )

            self._channel_method("start_consuming")

            assert self._return_value is not None
            ret: dict = self._return_value
            self._return_value = None

        if ret["error"] is not None:
            raise ret["error"]  # type: ignore

        return ret["result"]
