# copied from https://github.com/tulir/mautrix-telegram/blob/master/mautrix_telegram/util/parallel_file_transfer.py
# Copyright (C) 2021 Tulir Asokan
import asyncio
import hashlib
import inspect
import logging
import math
import os
from collections import defaultdict
from typing import Optional, List, AsyncGenerator, Union, Awaitable, DefaultDict, Tuple, BinaryIO

from telethon import utils, helpers, TelegramClient
from telethon.crypto import AuthKey
from telethon.network import MTProtoSender
from telethon.tl.alltlobjects import LAYER
from telethon.tl.functions import InvokeWithLayerRequest
from telethon.tl.functions.auth import ExportAuthorizationRequest, ImportAuthorizationRequest
from telethon.tl.functions.upload import (GetFileRequest, SaveFilePartRequest,
                                          SaveBigFilePartRequest)
from telethon.tl.types import (Document, InputFileLocation, InputDocumentFileLocation,
                               InputPhotoFileLocation, InputPeerPhotoFileLocation, TypeInputFile,
                               InputFileBig, InputFile)

try:
    from mautrix.crypto.attachments import async_encrypt_attachment
except ImportError:
    async_encrypt_attachment = None

log: logging.Logger = logging.getLogger("telethon")

TypeLocation = Union[Document, InputDocumentFileLocation, InputPeerPhotoFileLocation,
                     InputFileLocation, InputPhotoFileLocation]


class DownloadSender:
    client: TelegramClient
    sender: MTProtoSender
    request: GetFileRequest
    remaining: int
    stride: int

    def __init__(self, client: TelegramClient, sender: MTProtoSender, file: TypeLocation, offset: int, limit: int,
                 stride: int, count: int) -> None:
        self.sender = sender
        self.client = client
        self.request = GetFileRequest(file, offset=offset, limit=limit)
        self.stride = stride
        self.remaining = count

    async def next(self) -> Optional[bytes]:
        if not self.remaining:
            return None
        max_retries = 3
        for attempt in range(max_retries):
            try:
                result = await self.client._call(self.sender, self.request)
                self.remaining -= 1
                self.request.offset += self.stride
                return result.bytes
            except Exception as e:
                error_str = str(e).lower()
                if 'flood' in error_str or '420' in str(type(e).__name__):
                    import re
                    wait_match = re.search(r'(\d+)', str(e))
                    wait_time = int(wait_match.group(1)) if wait_match else 5
                    wait_time = min(wait_time, 30)
                    log.warning(f"FLOOD_WAIT detected, waiting {wait_time}s (attempt {attempt+1}/{max_retries})")
                    await asyncio.sleep(wait_time)
                    if attempt == max_retries - 1:
                        raise
                else:
                    raise
        return None

    def disconnect(self) -> Awaitable[None]:
        return self.sender.disconnect()


class UploadSender:
    client: TelegramClient
    sender: MTProtoSender
    request: Union[SaveFilePartRequest, SaveBigFilePartRequest]
    part_count: int
    stride: int
    previous: Optional[asyncio.Task]
    loop: asyncio.AbstractEventLoop

    def __init__(self, client: TelegramClient, sender: MTProtoSender, file_id: int, part_count: int, big: bool,
                 index: int,
                 stride: int, loop: asyncio.AbstractEventLoop) -> None:
        self.client = client
        self.sender = sender
        self.part_count = part_count
        if big:
            self.request = SaveBigFilePartRequest(file_id, index, part_count, b"")
        else:
            self.request = SaveFilePartRequest(file_id, index, b"")
        self.stride = stride
        self.previous = None
        self.loop = loop

    async def next(self, data: bytes) -> None:
        if self.previous:
            await self.previous
        self.previous = self.loop.create_task(self._next(data))

    async def _next(self, data: bytes) -> None:
        self.request.bytes = data
        log.debug(f"Sending file part {self.request.file_part}/{self.part_count}"
                  f" with {len(data)} bytes")
        await self.client._call(self.sender, self.request)
        self.request.file_part += self.stride

    async def disconnect(self) -> None:
        if self.previous:
            await self.previous
        return await self.sender.disconnect()


class ParallelTransferrer:
    client: TelegramClient
    loop: asyncio.AbstractEventLoop
    dc_id: int
    senders: Optional[List[Union[DownloadSender, UploadSender]]]
    auth_key: AuthKey
    upload_ticker: int

    def __init__(self, client: TelegramClient, dc_id: Optional[int] = None) -> None:
        self.client = client
        self.loop = self.client.loop
        self.dc_id = dc_id or self.client.session.dc_id
        self.auth_key = (None if dc_id and self.client.session.dc_id != dc_id
                         else self.client.session.auth_key)
        self.senders = None
        self.upload_ticker = 0

    async def _cleanup(self) -> None:
        if not self.senders:
            return
        try:
            await asyncio.gather(*[sender.disconnect() for sender in self.senders], return_exceptions=True)
        except Exception:
            pass
        self.senders = None

    @staticmethod
    def _get_connection_count(file_size: int, max_count: int = 20,
                              full_size: int = 100 * 1024 * 1024) -> int:
        # Each user has their own session, so each transfer can use full connection capacity
        # This method is monkeypatched by helpers/transfer.py with size-aware logic
        if file_size <= 0:
            return max(6, max_count // 2)  # Safe fallback for unknown size
        return max_count

    async def _init_download(self, connections: int, file: TypeLocation, part_count: int,
                             part_size: int) -> None:
        minimum, remainder = divmod(part_count, connections)

        def get_part_count() -> int:
            nonlocal remainder
            if remainder > 0:
                remainder -= 1
                return minimum + 1
            return minimum

        # The first cross-DC sender will export+import the authorization, so we always create it
        # before creating any other senders.
        # Use try/finally to cleanup partially created senders on failure
        temp_senders = []
        try:
            first_sender = await self._create_download_sender(file, 0, part_size, connections * part_size,
                                                              get_part_count())
            temp_senders.append(first_sender)
            
            # Use return_exceptions=True to capture all results including failures
            # This prevents exceptions from cancelling successful senders
            results = await asyncio.gather(
                *[self._create_download_sender(file, i, part_size, connections * part_size,
                                               get_part_count())
                  for i in range(1, connections)],
                return_exceptions=True)
            
            # Check for any exceptions and collect successful senders
            for result in results:
                if isinstance(result, Exception):
                    # Cleanup all successful senders before raising
                    for sender in temp_senders:
                        try:
                            await sender.disconnect()
                        except Exception:
                            pass
                    raise result
                temp_senders.append(result)
            
            self.senders = temp_senders
        except Exception:
            # Cleanup any senders that were created before the failure
            for sender in temp_senders:
                try:
                    await sender.disconnect()
                except Exception:
                    pass
            raise

    async def _create_download_sender(self, file: TypeLocation, index: int, part_size: int,
                                      stride: int,
                                      part_count: int) -> DownloadSender:
        return DownloadSender(self.client, await self._create_sender(), file, index * part_size, part_size,
                              stride, part_count)

    async def _init_upload(self, connections: int, file_id: int, part_count: int, big: bool
                           ) -> None:
        # Use try/except to cleanup partially created senders on failure
        temp_senders = []
        try:
            first_sender = await self._create_upload_sender(file_id, part_count, big, 0, connections)
            temp_senders.append(first_sender)
            
            # Use return_exceptions=True to capture all results including failures
            # This prevents exceptions from cancelling successful senders
            results = await asyncio.gather(
                *[self._create_upload_sender(file_id, part_count, big, i, connections)
                  for i in range(1, connections)],
                return_exceptions=True)
            
            # Check for any exceptions and collect successful senders
            for result in results:
                if isinstance(result, Exception):
                    # Cleanup all successful senders before raising
                    for sender in temp_senders:
                        try:
                            await sender.disconnect()
                        except Exception:
                            pass
                    raise result
                temp_senders.append(result)
            
            self.senders = temp_senders
        except Exception:
            # Cleanup any senders that were created before the failure
            for sender in temp_senders:
                try:
                    await sender.disconnect()
                except Exception:
                    pass
            raise

    async def _create_upload_sender(self, file_id: int, part_count: int, big: bool, index: int,
                                    stride: int) -> UploadSender:
        return UploadSender(self.client, await self._create_sender(), file_id, part_count, big, index, stride,
                            loop=self.loop)

    async def _create_sender(self) -> MTProtoSender:
        dc = await self.client._get_dc(self.dc_id)
        sender = MTProtoSender(self.auth_key, loggers=self.client._log)
        await sender.connect(self.client._connection(dc.ip_address, dc.port, dc.id,
                                                     loggers=self.client._log,
                                                     proxy=self.client._proxy))
        if not self.auth_key:
            log.debug(f"Exporting auth to DC {self.dc_id}")
            auth = await self.client(ExportAuthorizationRequest(self.dc_id))
            self.client._init_request.query = ImportAuthorizationRequest(id=auth.id,
                                                                         bytes=auth.bytes)
            req = InvokeWithLayerRequest(LAYER, self.client._init_request)
            await sender.send(req)
            self.auth_key = sender.auth_key
        return sender

    async def init_upload(self, file_id: int, file_size: int, part_size_kb: Optional[float] = None,
                          connection_count: Optional[int] = None) -> Tuple[int, int, bool]:
        connection_count = connection_count or self._get_connection_count(file_size)
        # OPTIMIZED: Always use maximum part size (512KB) for fastest uploads
        part_size = (part_size_kb or 512) * 1024  # 512KB max chunk size
        part_count = (file_size + part_size - 1) // part_size
        is_large = file_size > 10 * 1024 * 1024
        await self._init_upload(connection_count, file_id, part_count, is_large)
        return part_size, part_count, is_large

    async def upload(self, part: bytes) -> None:
        await self.senders[self.upload_ticker].next(part)
        self.upload_ticker = (self.upload_ticker + 1) % len(self.senders)

    async def finish_upload(self) -> None:
        await self._cleanup()

    async def download(self, file: TypeLocation, file_size: int,
                       part_size_kb: Optional[float] = None,
                       connection_count: Optional[int] = None) -> AsyncGenerator[bytes, None]:
        connection_count = connection_count or self._get_connection_count(file_size)
        # OPTIMIZED: Always use maximum part size (512KB) for fastest downloads
        # Larger chunks = fewer requests = higher throughput
        part_size = (part_size_kb or 512) * 1024  # 512KB max chunk size
        part_count = math.ceil(file_size / part_size)
        log.debug("Starting parallel download: "
                  f"{connection_count} {part_size} {part_count} {file!s}")
        await self._init_download(connection_count, file, part_count, part_size)

        try:
            part = 0
            while part < part_count:
                tasks = []
                for sender in self.senders:
                    tasks.append(self.loop.create_task(sender.next()))
                for task in tasks:
                    data = await task
                    if not data:
                        break
                    yield data
                    part += 1
                    log.debug(f"Part {part} downloaded")
        finally:
            log.debug("Parallel download finished, cleaning up connections")
            await self._cleanup()


parallel_transfer_locks: DefaultDict[int, asyncio.Lock] = defaultdict(lambda: asyncio.Lock())


def stream_file(file_to_stream: BinaryIO, chunk_size=1024):
    while True:
        data_read = file_to_stream.read(chunk_size)
        if not data_read:
            break
        yield data_read


async def _internal_transfer_to_telegram(client: TelegramClient,
                                         response: BinaryIO,
                                         progress_callback: callable,
                                         connection_count: Optional[int] = None
                                         ) -> Tuple[TypeInputFile, int]:
    file_id = helpers.generate_random_long()
    file_size = os.path.getsize(response.name)
    
    # Extract filename from file path to preserve extension
    file_name = os.path.basename(response.name)

    hash_md5 = hashlib.md5()
    uploader = ParallelTransferrer(client)
    part_size, part_count, is_large = await uploader.init_upload(file_id, file_size, connection_count=connection_count)
    buffer = bytearray()
    try:
        for data in stream_file(response):
            if progress_callback:
                r = progress_callback(response.tell(), file_size)
                if inspect.isawaitable(r):
                    await r
            if not is_large:
                hash_md5.update(data)
            if len(buffer) == 0 and len(data) == part_size:
                await uploader.upload(data)
                continue
            new_len = len(buffer) + len(data)
            if new_len >= part_size:
                cutoff = part_size - len(buffer)
                buffer.extend(data[:cutoff])
                await uploader.upload(bytes(buffer))
                buffer.clear()
                buffer.extend(data[cutoff:])
            else:
                buffer.extend(data)
        if len(buffer) > 0:
            await uploader.upload(bytes(buffer))
    finally:
        await uploader.finish_upload()
    if is_large:
        return InputFileBig(file_id, part_count, file_name), file_size
    else:
        return InputFile(file_id, part_count, file_name, hash_md5.hexdigest()), file_size


async def download_file(client: TelegramClient,
                        location: TypeLocation,
                        out: BinaryIO,
                        progress_callback: callable = None,
                        file_size: Optional[int] = None,
                        connection_count: Optional[int] = None
                        ) -> BinaryIO:
    size = file_size if file_size is not None else location.size
    dc_id, location = utils.get_input_location(location)
    # We lock the transfers because telegram has connection count limits
    downloader = ParallelTransferrer(client, dc_id)
    downloaded = downloader.download(location, size, connection_count=connection_count)
    async for x in downloaded:
        out.write(x)
        if progress_callback:
            r = progress_callback(out.tell(), size)
            if inspect.isawaitable(r):
                await r

    return out


async def upload_file(client: TelegramClient,
                      file: BinaryIO,
                      progress_callback: callable = None,
                      connection_count: Optional[int] = None
                      ) -> TypeInputFile:
    res = (await _internal_transfer_to_telegram(client, file, progress_callback, connection_count))[0]
    return res
