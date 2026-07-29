import asyncio
from pathlib import Path
import sys
from types import SimpleNamespace
import unittest
from unittest import mock


PROJECT_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PROJECT_ROOT / "src"))
sys.path.insert(0, str(PROJECT_ROOT / "src" / "bot"))

import bot as bot_app


class BotCommandsTests(unittest.TestCase):
    def test_menu_commands_match_registered_handlers(self):
        menu_names = [command.command for command in bot_app.BOT_COMMANDS]
        handler_names = [name for name, _ in bot_app.COMMAND_HANDLERS]

        self.assertEqual(menu_names, handler_names)
        self.assertIn("calendar", menu_names)
        self.assertIn("monthpdf", menu_names)
        self.assertIn("targets", menu_names)
        self.assertIn("rebalance", menu_names)
        self.assertIn("invest", menu_names)

    def test_sync_bot_commands_updates_telegram_menu(self):
        set_my_commands = mock.AsyncMock()
        app = SimpleNamespace(bot=SimpleNamespace(set_my_commands=set_my_commands))

        asyncio.run(bot_app.sync_bot_commands(app))

        set_my_commands.assert_awaited_once_with(bot_app.BOT_COMMANDS)
