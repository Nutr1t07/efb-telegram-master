# coding=utf-8
import io
from typing import Tuple, Optional, TYPE_CHECKING, List, IO, Union
import asyncio
import threading
import logging

import ehforwarderbot
import pyrogram
from PIL import Image
from ehforwarderbot import coordinator
from ehforwarderbot.exceptions import EFBOperationNotSupported
from ehforwarderbot.types import ChatID

from .chat import ETMChatType, ETMPrivateChat, ETMGroupChat, ETMSystemChat
from .locale_mixin import LocaleMixin
from . import utils

if TYPE_CHECKING:
    from . import TelegramChannel
    from .bot_manager import TelegramBotManager
    from .db import DatabaseManager


class AutoTGManager(LocaleMixin):
    """
    This is a wrapper of pyrogram which perform as a Telegram Client.
    Used to automatically create new telegram group.
    """

    def add_task(self, awaitable):
        return asyncio.run_coroutine_threadsafe(awaitable, self.tg_loop)

    def __init__(self, channel: 'TelegramChannel'):
        self.channel: 'TelegramChannel' = channel
        self.bot: 'TelegramBotManager' = self.channel.bot_manager
        self.logger: logging.Logger = logging.getLogger(__name__)
        self.flag: utils.ExperimentalFlagsManager = self.channel.flag
        self.db: 'DatabaseManager' = channel.db

        self.tg_config: dict = self.flag('auto_manage_tg_config')
        if self.tg_config.get('auto_manage_tg') and \
                self.tg_config.get('tg_api_id') and \
                self.tg_config.get('tg_api_hash'):
            async def _init_client():
                self.tg_client = pyrogram.Client(name='efb_telegram_auto_create_group_client',
                                                 api_id=self.tg_config.get('tg_api_id'),
                                                 api_hash=self.tg_config.get('tg_api_hash'),
                                                 workdir=ehforwarderbot.utils.get_data_path(channel.channel_id))

            
            self.tg_loop = asyncio.new_event_loop()
            threading.Thread(target=self.tg_loop.run_forever, daemon=True).start()
            self.add_task(_init_client())
            self.add_task(self._start_tg_client_if_needed())

    def create_tg_group_if_needed(self, chat: ETMChatType) -> Optional[utils.EFBChannelChatIDStr]:
        if not self.tg_client:
            return None

        if chat.vendor_specific.get('is_mp') and self.tg_config.get('mq_auto_link_group_id'):
            # 公众号绑定到同一个 TG 群
            mq_tg_group_id = str(self.tg_config.get('mq_auto_link_group_id', ''))
            if not mq_tg_group_id or not len(mq_tg_group_id):
                return None
            chat.link(self.channel.channel_id, mq_tg_group_id, True)
            tg_chats = self.db.get_chat_assoc(master_uid=utils.chat_id_to_str(chat_uid=mq_tg_group_id))
            if len(tg_chats) == 1:
                return tg_chats[0]
            else:
                self.logger.debug('could not find TG group with mq_auto_link_group_id')
        elif self._array_config_contains_chat_type('auto_create_tg_group', chat):
            try:
                return self.add_task(self._create_tg_group(chat)).result(30)
            except TimeoutError:
                self.logger.exception('took too long to create group, cancelling...')
                future.cancel()
            except Exception as exc:
                self.logger.exception(f'an exception raised while creating group: {exc!r}')
        else:
            return None

    async def _create_tg_group(self, chat: ETMChatType) -> utils.EFBChannelChatIDStr:
        try:
            tg_chat = await self._async_create_tg_group(chat)
            if not tg_chat:
                return None
            self.logger.debug("Auto create telegram Group Named: [%s]", tg_chat.title)
            chat.link(self.channel.channel_id, tg_chat.id, True)
            self._update_chat_image(tg_chat)
            tg_chats = self.db.get_chat_assoc(slave_uid=utils.chat_id_to_str(chat=chat))
            assert len(tg_chats) == 1
            return tg_chats[0]
        except Exception:
            self.logger.exception("Unknown error caught when creating TG group.")
            return None

    def _update_chat_image(self, tg_chat: pyrogram.types.Chat):
        picture: Optional[IO] = None
        pic_resized: Optional[IO] = None
        try:
            chats = self.db.get_chat_assoc(master_uid=utils.chat_id_to_str(channel=self.channel,
                                                                           chat_uid=ChatID(str(tg_chat.id))))
            assert len(chats) == 1
            channel_id, chat_uid, _ = utils.chat_id_str_to_id(chats[0])
            channel = coordinator.slaves[channel_id]
            _chat = self.channel.chat_manager.update_chat_obj(channel.get_chat(chat_uid), full_update=True)
            picture = channel.get_chat_picture(_chat)
            if not picture:
                pass
            pic_img = Image.open(picture)

            if pic_img.size[0] < 256 or pic_img.size[1] < 256:
                # resize
                scale = 256 / min(pic_img.size)
                pic_resized = io.BytesIO()
                pic_img.resize(tuple(map(lambda a: int(scale * a), pic_img.size)), Image.BICUBIC) \
                    .save(pic_resized, 'PNG')
                pic_resized.seek(0)
            picture.seek(0)

            self.bot.set_chat_photo(tg_chat.id, pic_resized or picture)
        except EFBOperationNotSupported:
            self.logger.warning('No profile picture provided from this chat.')
        except Exception:
            self.logger.exception("Unknown error caught when querying chat.")
        finally:
            if picture and getattr(picture, 'close', None):
                picture.close()
            if pic_resized and getattr(pic_resized, 'close', None):
                pic_resized.close()

    async def _async_create_tg_group(self, chat: ETMChatType) -> Optional[pyrogram.types.Chat]:
        try:
            await self._start_tg_client_if_needed()
            tg_chat = await self.tg_client.create_group(chat.chat_title, self.bot.me.username)
            bot = await self.tg_client.resolve_peer(self.bot.me.username)
            _raw_chat = await self.tg_client.resolve_peer(tg_chat.id)
            await self.tg_client.invoke(
                pyrogram.raw.functions.messages.EditChatAdmin(
                    chat_id=_raw_chat.chat_id,
                    user_id=bot,
                    is_admin=True))
            await self._add_tg_group_to_folder_if_needed(chat, tg_chat)
            await self._archive_tg_chat_if_needed(chat, tg_chat)
            await self._mute_tg_group_if_needed(chat, tg_chat)
            # await self._stop_tg_client()
            return tg_chat
        except Exception:
            self.logger.exception("Unknown error caught when creating TG group.")
            return None

    async def _start_tg_client_if_needed(self):
        if not self.tg_client.is_connected:
            await self.tg_client.start()

    async def _stop_tg_client(self):
        if self.tg_client.is_connected:
            await self.tg_client.stop()

    async def _add_tg_group_to_folder_if_needed(self, chat: ETMChatType, tg_chat: pyrogram.types.Chat):
        try:
            folder_config = self.tg_config.get('auto_add_group_to_folder', {})
            if not folder_config:
                return
            folders: List[pyrogram.raw.base.DialogFilter] = await self.tg_client.invoke(
                pyrogram.raw.functions.messages.GetDialogFilters())

            def get_target_folder(title: str) -> Optional[pyrogram.raw.types.DialogFilter]:
                result = list(
                    filter(lambda x: isinstance(x, pyrogram.raw.types.DialogFilter) and x.title == title, folders))
                if len(result) == 1:
                    return result[0]
                return None

            target_folder = Optional[pyrogram.raw.base.DialogFilter]
            if chat.vendor_specific.get('is_mp') and folder_config[4]:
                target_folder = get_target_folder(folder_config[4])
            elif isinstance(chat, ETMPrivateChat) and folder_config[1]:
                target_folder = get_target_folder(folder_config[1])
            elif isinstance(chat, ETMGroupChat) and folder_config[2]:
                target_folder = get_target_folder(folder_config[2])
            elif isinstance(chat, ETMSystemChat) and folder_config[3]:
                target_folder = get_target_folder(folder_config[3])

            if target_folder:
                peer = await self.tg_client.resolve_peer(tg_chat.id)
                target_folder.include_peers.append(peer)
                r = await self.tg_client.invoke(
                    pyrogram.raw.functions.messages.UpdateDialogFilter(id=target_folder.id, filter=target_folder))
                assert r
        except Exception:
            self.logger.exception("Unknown error caught when adding TG group to folder.")

    async def _archive_tg_chat_if_needed(self, chat: ETMChatType, tg_chat: pyrogram.types.Chat):
        try:
            if self._array_config_contains_chat_type('auto_archive_create_tg_group', chat):
                await self.tg_client.archive_chats(tg_chat.id)
        except Exception:
            self.logger.exception("Unknown error caught when archiving TG chat.")

    async def _mute_tg_group_if_needed(self, chat: ETMChatType, tg_chat: pyrogram.types.Chat):
        try:
            if self._array_config_contains_chat_type('auto_mute_created_tg_group', chat):
                peer = await self.tg_client.resolve_peer(tg_chat.id)
                await self.tg_client.invoke(pyrogram.raw.functions.account.UpdateNotifySettings(
                    peer=pyrogram.raw.types.InputNotifyPeer(peer=peer),
                    settings=pyrogram.raw.types.InputPeerNotifySettings(silent=True)))
        except Exception:
            self.logger.exception("Unknown error caught when muting TG chat.")

    def _array_config_contains_chat_type(self, config_name: str, chat: ETMChatType) -> bool:
        config = self.tg_config.get(config_name, [])
        if (chat.vendor_specific.get('is_mp') and 4 in config) or \
                (isinstance(chat, ETMPrivateChat) and 1 in config) or \
                (isinstance(chat, ETMGroupChat) and 2 in config) or \
                (isinstance(chat, ETMSystemChat) and 3 in config):
            return True
        return False
