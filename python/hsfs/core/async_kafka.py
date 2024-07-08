#
#   Copyright 2024 Hopsworks AB
#
#   Licensed under the Apache License, Version 2.0 (the "License");
#   you may not use this file except in compliance with the License.
#   You may obtain a copy of the License at
#
#       http://www.apache.org/licenses/LICENSE-2.0
#
#   Unless required by applicable law or agreed to in writing, software
#   distributed under the License is distributed on an "AS IS" BASIS,
#   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#   See the License for the specific language governing permissions and
#   limitations under the License.
#
from __future__ import annotations

import asyncio
import functools
from threading import Thread
from typing import Any, Callable, Dict, Optional

from hsfs.core.constants import HAS_CONFLUENT_KAFKA
from tqdm.auto import tqdm


if HAS_CONFLUENT_KAFKA:
    from confluent_kafka import KafkaError, KafkaException, Producer


def async_acked(
    err: Exception,
    msg: str,
    debug_kafka: bool,
    progress_bar: tqdm,
    is_multi_part_insert: bool,
    result: asyncio.Future,
    loop: asyncio.AbstractEventLoop,
) -> None:
    if err is not None:
        if debug_kafka:
            loop.call_soon_threadsafe(result.set_exception, KafkaException(err))
            print("Failed to deliver message: %s: %s" % (str(msg), str(err)))
        if err.code() in [
            KafkaError.TOPIC_AUTHORIZATION_FAILED,
            KafkaError._MSG_TIMED_OUT,
        ]:
            progress_bar.colour = "RED"
            raise err  # Stop producing and show error
    # update progress bar for each msg
    if not is_multi_part_insert:
        loop.call_soon_threadsafe(result.set_result, msg)
        progress_bar.update()


class AsyncKafkaProducer:
    def __init__(
        self, configs: Dict[str, Any], loop: Optional[asyncio.AbstractEventLoop] = None
    ):
        self._loop = loop or asyncio.get_event_loop()
        self._producer = Producer(configs)
        self._cancelled = False
        self._poll_thread = Thread(target=self._poll_loop, name="kafka-poll-thread")
        self._poll_thread.start()

    def _poll_loop(self) -> None:
        while not self._cancelled:
            _n_rows_delivered = self._producer.poll(0.1)

    def close(self) -> None:
        self._cancelled = True
        self._poll_thread.join()

    def produce(
        self,
        key: str,
        encoded_row: bytes,
        topic_name: str,
        headers: Dict[str, Any],
        acked: Callable,
        debug_kafka: bool,
    ) -> asyncio.Future[Any]:
        try:
            result = self._loop.create_future()

            self._producer.produce(
                topic=topic_name,
                key=key,
                value=encoded_row,
                callback=functools.partial(acked, result=result, loop=self._loop),
                headers=headers,
            )
        except BufferError as e:
            if debug_kafka:
                print(f"Caught BufferError: {str(e)}")
            # backoff for 0.2 seconds
            self._producer.poll(0.2)
