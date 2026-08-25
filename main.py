import os
import re
import math
import asyncio
import logging
from datetime import datetime, timedelta
from typing import Optional, List, Dict, Any

import aiohttp
import aiosqlite
import discord
from discord import app_commands
from discord.ext import commands, tasks
from dotenv import load_dotenv

# Попытка импорта библиотеки для A2S Query (информация о Rust серверах)
try:
    import a2s
    HAS_A2S = True
except ImportError:
    HAS_A2S = False

load_dotenv()

DISCORD_TOKEN = os.getenv("DISCORD_TOKEN")
STEAM_API_KEY = os.getenv("STEAM_API_KEY")
YOUTUBE_API_KEY = os.getenv("YOUTUBE_API_KEY")

RUST_APP_ID = "252490"
DB_NAME = "hate_rust_bot.db"

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(name)s: %(message)s")
logger = logging.getLogger("HateRustBot")

# ==============================================================================
# БАЗА ДАННЫХ (AIOSQLITE)
# ==============================================================================

async def init_db():
    async with aiosqlite.connect(DB_NAME) as db:
        await db.execute("""
            CREATE TABLE IF NOT EXISTS guild_settings (
                guild_id INTEGER PRIMARY KEY,
                clan_role_id INTEGER,
                mod_role_id INTEGER,
                leader_channel_id INTEGER,
                ticket_category_id INTEGER,
                voice_creator_id INTEGER,
                voice_category_id INTEGER,
                log_channel_id INTEGER,
                media_channel_id INTEGER,
                warn_limit INTEGER DEFAULT 3
            )
        """)
        await db.execute("""
            CREATE TABLE IF NOT EXISTS users (
                discord_id INTEGER PRIMARY KEY,
                steam_id TEXT,
                xp INTEGER DEFAULT 0,
                level INTEGER DEFAULT 1,
                scrap INTEGER DEFAULT 0
            )
        """)
        await db.execute("""
            CREATE TABLE IF NOT EXISTS wipes (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                guild_id INTEGER,
                channel_id INTEGER,
                message_id INTEGER,
                connect_cmd TEXT,
                wipe_time TIMESTAMP,
                ping_15m_sent INTEGER DEFAULT 0,
                started_sent INTEGER DEFAULT 0
            )
        """)
        await db.execute("""
            CREATE TABLE IF NOT EXISTS wipe_responses (
                wipe_id INTEGER,
                discord_id INTEGER,
                status TEXT NOT NULL,
                reason TEXT,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                PRIMARY KEY (wipe_id, discord_id)
            )
        """)
        await db.execute("""
            CREATE TABLE IF NOT EXISTS warns (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                guild_id INTEGER NOT NULL,
                discord_id INTEGER NOT NULL,
                moderator_id INTEGER NOT NULL,
                reason TEXT NOT NULL,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)
        await db.execute("""
            CREATE TABLE IF NOT EXISTS tickets (
                channel_id INTEGER PRIMARY KEY,
                user_id INTEGER,
                guild_id INTEGER,
                claimed_by INTEGER,
                ticket_type TEXT
            )
        """)
        await db.execute("""
            CREATE TABLE IF NOT EXISTS youtube_tracks (
                channel_id_yt TEXT PRIMARY KEY,
                last_video_id TEXT
            )
        """)
        await db.commit()

async def get_setting(guild_id: int, key: str) -> Optional[int]:
    async with aiosqlite.connect(DB_NAME) as db:
        async with db.execute(f"SELECT {key} FROM guild_settings WHERE guild_id = ?", (guild_id,)) as cursor:
            row = await cursor.fetchone()
            return row[0] if row else None

async def update_setting(guild_id: int, key: str, value: Any):
    async with aiosqlite.connect(DB_NAME) as db:
        await db.execute(f"""
            INSERT INTO guild_settings (guild_id, {key}) VALUES (?, ?)
            ON CONFLICT(guild_id) DO UPDATE SET {key} = ?
        """, (guild_id, value, value))
        await db.commit()

async def log_event(guild: discord.Guild, embed: discord.Embed):
    log_channel_id = await get_setting(guild.id, "log_channel_id")
    if log_channel_id:
        channel = guild.get_channel(log_channel_id)
        if channel and isinstance(channel, discord.TextChannel):
            await channel.send(embed=embed)

# ==============================================================================
# ВСПОМОГАТЕЛЬНЫЕ ФУНКЦИИ
# ==============================================================================

async def get_rust_server_info(ip: str, port: int) -> Optional[dict]:
    if not HAS_A2S:
        return None
    try:
        info = await a2s.ainfo((ip, port), timeout=3.0)
        return {
            "name": info.server_name,
            "players": info.players,
            "max_players": info.max_players,
            "map": info.map_name
        }
    except Exception:
        return None

async def resolve_steam_id(session: aiohttp.ClientSession, input_str: str) -> Optional[str]:
    input_str = input_str.strip()
    match = re.search(r"(7656119\d{10})", input_str)
    if match:
        return match.group(1)
    
    match_custom = re.search(r"steamcommunity\.com/id/([^/]+)", input_str)
    vanity_url = match_custom.group(1) if match_custom else input_str

    if not STEAM_API_KEY:
        return None

    url = f"http://api.steampowered.com/ISteamUser/ResolveVanityURL/v0001/?key={STEAM_API_KEY}&vanityurl={vanity_url}"
    async with session.get(url) as resp:
        if resp.status == 200:
            data = await resp.json()
            if data.get("response", {}).get("success") == 1:
                return data["response"]["steamid"]
    return None

async def build_report_embed(wipe_id: int, is_final: bool = False) -> discord.Embed:
    async with aiosqlite.connect(DB_NAME) as db:
        async with db.execute("""
            SELECT r.discord_id, r.status, r.reason, u.steam_id
            FROM wipe_responses r
            LEFT JOIN users u ON r.discord_id = u.discord_id
            WHERE r.wipe_id = ?
        """, (wipe_id,)) as cursor:
            responses = await cursor.fetchall()

    attending, late, absent = [], [], []
    for discord_id, status, reason, steam_id in responses:
        steam_str = f" | `{steam_id}`" if steam_id else " | *SteamID не привязан*"
        entry = f"<@{discord_id}>{steam_str}"
        if reason and reason != "Без задержек":
            entry += f" — *{reason}*"

        if status == "Идет":
            attending.append(entry)
        elif status == "Опоздает":
            late.append(entry)
        elif status == "Отсутствует":
            absent.append(entry)

    title = "🚀 ИТОГОВАЯ СВОДКА ЯВКИ НА ВАЙП" if is_final else "📊 Отчет готовности клана HATE к вайпу"
    color = discord.Color.red() if is_final else discord.Color.gold()

    embed = discord.Embed(title=title, color=color, timestamp=datetime.now())
    embed.add_field(name=f"✅ Идут ({len(attending)})", value="\n".join(attending) if attending else "Никого", inline=False)
    embed.add_field(name=f"⚠️ Опоздают ({len(late)})", value="\n".join(late) if late else "Никого", inline=False)
    embed.add_field(name=f"❌ Отсутствуют ({len(absent)})", value="\n".join(absent) if absent else "Никого", inline=False)
    
    if is_final:
        embed.set_footer(text="Автоматический итоговый отчёт при старте вайпа")
    return embed

async def update_leader_report(guild: discord.Guild, wipe_id: int, bot: commands.Bot):
    leader_channel_id = await get_setting(guild.id, "leader_channel_id")
    if not leader_channel_id:
        return
    channel = guild.get_channel(leader_channel_id)
    if channel and isinstance(channel, discord.TextChannel):
        embed = await build_report_embed(wipe_id, is_final=False)
        async for msg in channel.history(limit=5):
            if msg.author == bot.user and msg.embeds and "Отчет готовности" in msg.embeds[0].title:
                await msg.edit(embed=embed)
                return
        await channel.send(embed=embed)

# ==============================================================================
# ЯДРО БОТА (BOT CORE)
# ==============================================================================

class HateClanBot(commands.Bot):
    def __init__(self):
        intents = discord.Intents.default()
        intents.message_content = True
        intents.members = True
        intents.voice_states = True
        super().__init__(command_prefix="!", intents=intents)
        self.session: Optional[aiohttp.ClientSession] = None

    async def setup_hook(self):
        self.session = aiohttp.ClientSession()
        await init_db()

        # Регистрация Cogs
        await self.add_cog(SetupCog(self))
        await self.add_cog(WipeCog(self))
        await self.add_cog(VoiceCog(self))
        await self.add_cog(SteamCog(self))
        await self.add_cog(ModerationCog(self))
        await self.add_cog(CalculatorsCog(self))
        await self.add_cog(TicketsCog(self))
        await self.add_cog(EconomyLevelCog(self))
        await self.add_cog(AutoModAuditCog(self))
        await self.add_cog(MediaCog(self))

        # Persistent Views
        self.add_view(WipeView(wipe_id=0)) 
        self.add_view(AdminPanelView())
        self.add_view(TicketLaunchView())

    async def close(self):
        if self.session:
            await self.session.close()
        await super().close()

    async def on_ready(self):
        logger.info(f"Бот запущен под именем: {self.user} (ID: {self.user.id})")
        await self.change_presence(status=discord.Status.online, activity=discord.Game(name="Rust | /setup"))

bot = HateClanBot()

# ==============================================================================
# COG 1: НАСТРОЙКИ И АДМИН-ПАНЕЛЬ
# ==============================================================================

class SetupSelectView(discord.ui.View):
    def __init__(self):
        super().__init__(timeout=180)

    @discord.ui.select(cls=discord.ui.RoleSelect, placeholder="Выберите основную роль клана...", row=0)
    async def select_clan_role(self, interaction: discord.Interaction, select: discord.ui.RoleSelect):
        role = select.values[0]
        await update_setting(interaction.guild.id, "clan_role_id", role.id)
        await interaction.response.send_message(f"✅ Роль клана установлена: {role.mention}", ephemeral=True)

    @discord.ui.select(cls=discord.ui.RoleSelect, placeholder="Выберите роль модераторов...", row=1)
    async def select_mod_role(self, interaction: discord.Interaction, select: discord.ui.RoleSelect):
        role = select.values[0]
        await update_setting(interaction.guild.id, "mod_role_id", role.id)
        await interaction.response.send_message(f"✅ Роль модератора установлена: {role.mention}", ephemeral=True)

    @discord.ui.select(cls=discord.ui.ChannelSelect, channel_types=[discord.ChannelType.text], placeholder="Канал отчетов руководства...", row=2)
    async def select_leader_channel(self, interaction: discord.Interaction, select: discord.ui.ChannelSelect):
        ch = select.values[0]
        await update_setting(interaction.guild.id, "leader_channel_id", ch.id)
        await interaction.response.send_message(f"✅ Канал руководства установлен: {ch.mention}", ephemeral=True)

    @discord.ui.select(cls=discord.ui.ChannelSelect, channel_types=[discord.ChannelType.text], placeholder="Канал логов (аудит)...", row=3)
    async def select_log_channel(self, interaction: discord.Interaction, select: discord.ui.ChannelSelect):
        ch = select.values[0]
        await update_setting(interaction.guild.id, "log_channel_id", ch.id)
        await interaction.response.send_message(f"✅ Канал логов установлен: {ch.mention}", ephemeral=True)

class AdminPanelView(discord.ui.View):
    def __init__(self):
        super().__init__(timeout=None)

    @discord.ui.button(label="📢 Анонс Вайпа", style=discord.ButtonStyle.primary, custom_id="admin_panel_wipe", row=0)
    async def create_wipe(self, interaction: discord.Interaction, button: discord.ui.Button):
        if not interaction.user.guild_permissions.administrator:
            await interaction.response.send_message("❌ Недостаточно прав!", ephemeral=True)
            return
        await interaction.response.send_modal(AdminWipeModal())

    @discord.ui.button(label="📊 Сводка Явки", style=discord.ButtonStyle.success, custom_id="admin_panel_report", row=0)
    async def get_report(self, interaction: discord.Interaction, button: discord.ui.Button):
        if not interaction.user.guild_permissions.administrator:
            await interaction.response.send_message("❌ Недостаточно прав!", ephemeral=True)
            return
        await interaction.response.defer(ephemeral=True)
        async with aiosqlite.connect(DB_NAME) as db:
            async with db.execute("SELECT id FROM wipes WHERE guild_id = ? ORDER BY id DESC LIMIT 1", (interaction.guild.id,)) as cursor:
                row = await cursor.fetchone()
        if not row:
            await interaction.followup.send("❌ Активных анонсов вайпа не найдено.", ephemeral=True)
            return
        embed = await build_report_embed(row[0], is_final=False)
        await interaction.followup.send(embed=embed, ephemeral=True)

    @discord.ui.button(label="📣 Пингануть должников", style=discord.ButtonStyle.secondary, custom_id="admin_panel_ping", row=0)
    async def ping_missing(self, interaction: discord.Interaction, button: discord.ui.Button):
        if not interaction.user.guild_permissions.administrator or not interaction.guild:
            await interaction.response.send_message("❌ Недостаточно прав!", ephemeral=True)
            return
        await interaction.response.defer(ephemeral=True)
        
        async with aiosqlite.connect(DB_NAME) as db:
            async with db.execute("SELECT id FROM wipes WHERE guild_id = ? ORDER BY id DESC LIMIT 1", (interaction.guild.id,)) as cursor:
                wipe_row = await cursor.fetchone()
                if not wipe_row:
                    await interaction.followup.send("❌ Активных анонсов вайпа нет.", ephemeral=True)
                    return
                wipe_id = wipe_row[0]

            async with db.execute("SELECT discord_id FROM wipe_responses WHERE wipe_id = ?", (wipe_id,)) as cursor:
                responded_ids = {r[0] for r in await cursor.fetchall()}

        clan_role_id = await get_setting(interaction.guild.id, "clan_role_id")
        missing_mentions = []
        if clan_role_id:
            role = interaction.guild.get_role(clan_role_id)
            if role:
                for member in role.members:
                    if not member.bot and member.id not in responded_ids:
                        missing_mentions.append(member.mention)

        if missing_mentions and interaction.channel:
            msg_text = "🚨 **СРОЧНО проголосуйте в анонсе вайпа!**\n" + " ".join(missing_mentions[:30])
            await interaction.channel.send(msg_text)
            await interaction.followup.send(f"✅ Пингануто {len(missing_mentions)} игроков!", ephemeral=True)
        else:
            await interaction.followup.send("✅ Все бойцы проголосовали или роль не настроена!", ephemeral=True)

    @discord.ui.button(label="⚙️ Быстрая Настройка", style=discord.ButtonStyle.secondary, custom_id="admin_panel_settings", row=1)
    async def open_settings(self, interaction: discord.Interaction, button: discord.ui.Button):
        if not interaction.user.guild_permissions.administrator:
            await interaction.response.send_message("❌ Недостаточно прав!", ephemeral=True)
            return
        embed = discord.Embed(title="⚙️ Быстрая настройка параметров сервера", color=discord.Color.blue())
        await interaction.response.send_message(embed=embed, view=SetupSelectView(), ephemeral=True)

class SetupCog(commands.Cog):
    def __init__(self, bot: HateClanBot):
        self.bot = bot

    @app_commands.command(name="setup", description="Открыть интерактиную панель конфигурации бота")
    @app_commands.checks.has_permissions(administrator=True)
    async def setup(self, interaction: discord.Interaction):
        embed = discord.Embed(title="⚙️ Конфигуратор HATE Bot", description="Выберите нужные опции в меню ниже:", color=discord.Color.blue())
        await interaction.response.send_message(embed=embed, view=SetupSelectView(), ephemeral=True)

    @app_commands.command(name="admin_panel", description="Открыть главную панель управления кланом")
    @app_commands.checks.has_permissions(administrator=True)
    async def admin_panel(self, interaction: discord.Interaction):
        embed = discord.Embed(title="🛠️ ПАНЕЛЬ УПРАВЛЕНИЯ КЛАНОМ HATE", description="Управление сборами, отчетами и настройками:", color=discord.Color.dark_grey())
        await interaction.response.send_message(embed=embed, view=AdminPanelView())

    @app_commands.command(name="sync", description="Принудительная синхронизация слэш-команд")
    @app_commands.checks.has_permissions(administrator=True)
    async def sync_cmd(self, interaction: discord.Interaction):
        await interaction.response.defer(ephemeral=True)
        synced = await self.bot.tree.sync()
        await interaction.followup.send(f"✅ Синхронизировано {len(synced)} команд глобально!")

# ==============================================================================
# COG 2: СИСТЕМА ВАЙПОВ (WIPE SYSTEM)
# ==============================================================================

class LateReasonModal(discord.ui.Modal, title="Причина опоздания"):
    reason = discord.ui.TextInput(label="Время / Причина", style=discord.TextStyle.long, placeholder="Задержусь на 30 мин", required=True)

    def __init__(self, wipe_id: int):
        super().__init__()
        self.wipe_id = wipe_id

    async def on_submit(self, interaction: discord.Interaction):
        async with aiosqlite.connect(DB_NAME) as db:
            await db.execute("""
                INSERT INTO wipe_responses (wipe_id, discord_id, status, reason) VALUES (?, ?, 'Опоздает', ?)
                ON CONFLICT(wipe_id, discord_id) DO UPDATE SET status='Опоздает', reason=excluded.reason
            """, (self.wipe_id, interaction.user.id, self.reason.value))
            await db.commit()

        await interaction.response.send_message("Статус **'Опоздаю'** зафиксирован!", ephemeral=True)
        if interaction.guild:
            await update_leader_report(interaction.guild, self.wipe_id, interaction.client)

class AbsentReasonModal(discord.ui.Modal, title="Причина отсутствия"):
    reason = discord.ui.TextInput(label="Причина", style=discord.TextStyle.long, placeholder="Не смогу быть в игре", required=True)

    def __init__(self, wipe_id: int):
        super().__init__()
        self.wipe_id = wipe_id

    async def on_submit(self, interaction: discord.Interaction):
        async with aiosqlite.connect(DB_NAME) as db:
            await db.execute("""
                INSERT INTO wipe_responses (wipe_id, discord_id, status, reason) VALUES (?, ?, 'Отсутствует', ?)
                ON CONFLICT(wipe_id, discord_id) DO UPDATE SET status='Отсутствует', reason=excluded.reason
            """, (self.wipe_id, interaction.user.id, self.reason.value))
            await db.commit()

        await interaction.response.send_message("Статус **'Не буду'** зафиксирован!", ephemeral=True)
        if interaction.guild:
            await update_leader_report(interaction.guild, self.wipe_id, interaction.client)

class WipeView(discord.ui.View):
    def __init__(self, wipe_id: int = 0):
        super().__init__(timeout=None)
        self.wipe_id = wipe_id

    @discord.ui.button(label="Буду на вайпе", style=discord.ButtonStyle.success, custom_id="wipe_btn_attending")
    async def attending(self, interaction: discord.Interaction, button: discord.ui.Button):
        w_id = self.wipe_id or await self._fetch_latest_wipe(interaction.guild.id)
        async with aiosqlite.connect(DB_NAME) as db:
            await db.execute("""
                INSERT INTO wipe_responses (wipe_id, discord_id, status, reason) VALUES (?, ?, 'Идет', 'Без задержек')
                ON CONFLICT(wipe_id, discord_id) DO UPDATE SET status='Идет', reason='Без задержек'
            """, (w_id, interaction.user.id))
            await db.commit()
        await interaction.response.send_message("Вы подтвердили участие!", ephemeral=True)
        if interaction.guild:
            await update_leader_report(interaction.guild, w_id, interaction.client)

    @discord.ui.button(label="Опоздаю", style=discord.ButtonStyle.secondary, custom_id="wipe_btn_late")
    async def late(self, interaction: discord.Interaction, button: discord.ui.Button):
        w_id = self.wipe_id or await self._fetch_latest_wipe(interaction.guild.id)
        await interaction.response.send_modal(LateReasonModal(w_id))

    @discord.ui.button(label="Не буду", style=discord.ButtonStyle.danger, custom_id="wipe_btn_absent")
    async def absent(self, interaction: discord.Interaction, button: discord.ui.Button):
        w_id = self.wipe_id or await self._fetch_latest_wipe(interaction.guild.id)
        await interaction.response.send_modal(AbsentReasonModal(w_id))

    async def _fetch_latest_wipe(self, guild_id: int) -> int:
        async with aiosqlite.connect(DB_NAME) as db:
            async with db.execute("SELECT id FROM wipes WHERE guild_id = ? ORDER BY id DESC LIMIT 1", (guild_id,)) as cursor:
                row = await cursor.fetchone()
                return row[0] if row else 0

class AdminWipeModal(discord.ui.Modal, title="📢 Анонс и настройка вайпа"):
    connect_cmd = discord.ui.TextInput(label="Команда подключения", placeholder="connect 185.220.x.x:28015", required=True)
    date_time = discord.ui.TextInput(label="Дата и время (ДД.ММ.ГГГГ ЧЧ:ММ)", placeholder="25.10.2026 17:00", required=True)

    async def on_submit(self, interaction: discord.Interaction):
        await interaction.response.defer(ephemeral=True)
        try:
            wipe_dt = datetime.strptime(self.date_time.value, "%d.%m.%Y %H:%M")
        except ValueError:
            await interaction.followup.send("❌ Ошибка формата даты! Используйте `ДД.ММ.ГГГГ ЧЧ:ММ`", ephemeral=True)
            return

        ip_port_match = re.search(r"connect\s+([\d\.]+):(\d+)", self.connect_cmd.value)
        server_name, online_str, map_name = "Неизвестно", "Ожидание статуса", "Неизвестно"

        if ip_port_match:
            srv_info = await get_rust_server_info(ip_port_match.group(1), int(ip_port_match.group(2)))
            if srv_info:
                server_name = srv_info["name"]
                online_str = f"{srv_info['players']}/{srv_info['max_players']}"
                map_name = srv_info["map"]

        embed = discord.Embed(
            title="🔥 ВНИМАНИЕ, КЛАН HATE! ОБЪЯВЛЕН ВАЙП 🔥",
            description=(
                f"**Дата и время:** <t:{int(wipe_dt.timestamp())}:F> (<t:{int(wipe_dt.timestamp())}:R>)\n\n"
                f"⚠️ **ТРЕБОВАНИЕ:** Всем быть в голосовом канале за **30 минут** до старта!\n\n"
                f"📌 **Подключение к серверу:**\n```{self.connect_cmd.value}```"
            ),
            color=discord.Color.dark_red()
        )
        embed.add_field(name="Сервер", value=f"`{server_name}`", inline=False)
        embed.add_field(name="Онлайн", value=f"`{online_str}`", inline=True)
        embed.add_field(name="Карта", value=f"`{map_name}`", inline=True)
        embed.add_field(name="Статус", value="🟡 Ожидание вайпа", inline=True)

        msg = await interaction.channel.send(embed=embed)

        async with aiosqlite.connect(DB_NAME) as db:
            cursor = await db.execute("""
                INSERT INTO wipes (guild_id, channel_id, message_id, connect_cmd, wipe_time)
                VALUES (?, ?, ?, ?, ?)
            """, (interaction.guild.id, interaction.channel_id, msg.id, self.connect_cmd.value, wipe_dt))
            wipe_id = cursor.lastrowid
            await db.commit()

        await msg.edit(view=WipeView(wipe_id=wipe_id))
        await interaction.followup.send("✅ Анонс вайпа успешно запущен!", ephemeral=True)

class WipeCog(commands.Cog):
    def __init__(self, bot: HateClanBot):
        self.bot = bot
        self.check_wipe_task.start()

    def cog_unload(self):
        self.check_wipe_task.cancel()

    @tasks.loop(seconds=30)
    async def check_wipe_task(self):
        now = datetime.now()
        async with aiosqlite.connect(DB_NAME) as db:
            async with db.execute("""
                SELECT id, guild_id, channel_id, message_id, wipe_time, ping_15m_sent, started_sent
                FROM wipes WHERE started_sent = 0
            """) as cursor:
                wipes = await cursor.fetchall()

            for w_id, guild_id, channel_id, message_id, wipe_time_str, ping_15m, started in wipes:
                wipe_dt = datetime.strptime(wipe_time_str, "%Y-%m-%d %H:%M:%S") if isinstance(wipe_time_str, str) else wipe_time_str

                # Пинг за 15 минут
                if not ping_15m and (wipe_dt - now) <= timedelta(minutes=15) and now < wipe_dt:
                    await db.execute("UPDATE wipes SET ping_15m_sent = 1 WHERE id = ?", (w_id,))
                    await db.commit()

                    channel = self.bot.get_channel(channel_id)
                    if channel and isinstance(channel, discord.TextChannel):
                        clan_role_id = await get_setting(guild_id, "clan_role_id")
                        role_mention = f"<@&{clan_role_id}>" if clan_role_id else "@everyone"
                        await channel.send(f"🚨 {role_mention} **ДО ВАЙПА ОСТАЛОСЬ 15 МИНУТ! ЗАХОДИТЕ В ГОЛОС!**")

                # Старт вайпа
                if not started and now >= wipe_dt:
                    await db.execute("UPDATE wipes SET started_sent = 1 WHERE id = ?", (w_id,))
                    await db.commit()

                    channel = self.bot.get_channel(channel_id)
                    if channel and isinstance(channel, discord.TextChannel):
                        try:
                            msg = await channel.fetch_message(message_id)
                            embed = msg.embeds[0]
                            embed.set_field_at(3, name="Статус", value="🟢 ВАЙП СТАРТОВАЛ!", inline=True)
                            await msg.edit(embed=embed)
                        except Exception:
                            pass

                    guild = self.bot.get_guild(guild_id)
                    if guild:
                        leader_ch_id = await get_setting(guild_id, "leader_channel_id")
                        if leader_ch_id:
                            leader_ch = guild.get_channel(leader_ch_id)
                            if leader_ch and isinstance(leader_ch, discord.TextChannel):
                                final_embed = await build_report_embed(w_id, is_final=True)
                                await leader_ch.send(content="🔔 **ВАЙП НАЧАЛСЯ! Итоговая сводка по составу:**", embed=final_embed)

# ==============================================================================
# COG 3: ДИНАМИЧЕСКИЕ ГОЛОСОВЫЕ (DYNAMIC VOICE)
# ==============================================================================

class VoiceCog(commands.Cog):
    def __init__(self, bot: HateClanBot):
        self.bot = bot
        self.active_temp_channels = set()

    @app_commands.command(name="voice_setup", description="Настроить динамические приватные комнаты")
    @app_commands.checks.has_permissions(administrator=True)
    async def voice_setup(self, interaction: discord.Interaction, creator_channel: discord.VoiceChannel, category: discord.CategoryChannel):
        await update_setting(interaction.guild.id, "voice_creator_id", creator_channel.id)
        await update_setting(interaction.guild.id, "voice_category_id", category.id)
        
        embed = discord.Embed(title="⚙️ Динамические голосовые каналы настроены!", color=discord.Color.green())
        embed.add_field(name="Создатель", value=creator_channel.mention)
        embed.add_field(name="Категория", value=category.name)
        await interaction.response.send_message(embed=embed, ephemeral=True)

    @commands.Cog.listener()
    async def on_voice_state_update(self, member: discord.Member, before: discord.VoiceState, after: discord.VoiceState):
        if not member.guild:
            return

        creator_id = await get_setting(member.guild.id, "voice_creator_id")
        category_id = await get_setting(member.guild.id, "voice_category_id")

        if creator_id and after.channel and after.channel.id == creator_id:
            category = member.guild.get_channel(category_id) if category_id else after.channel.category
            if category and isinstance(category, discord.CategoryChannel):
                channel_name = f"🔊 Рубка {member.display_name}"
                temp_ch = await member.guild.create_voice_channel(name=channel_name, category=category)
                self.active_temp_channels.add(temp_ch.id)
                try:
                    await member.move_to(temp_ch)
                except Exception:
                    pass

        if before.channel and before.channel.id in self.active_temp_channels:
            if len(before.channel.members) == 0:
                self.active_temp_channels.remove(before.channel.id)
                try:
                    await before.channel.delete(reason="Временный канал пуст")
                except Exception:
                    pass

# ==============================================================================
# COG 4: STEAM И СТАСТИСТИКА ИГРОКОВ
# ==============================================================================

class SteamCog(commands.Cog):
    def __init__(self, bot: HateClanBot):
        self.bot = bot

    @app_commands.command(name="link", description="Привязать ваш SteamID64 / ссылку к Discord профилю")
    async def link(self, interaction: discord.Interaction, steam_input: str):
        await interaction.response.defer(ephemeral=True)
        if not self.bot.session:
            await interaction.followup.send("❌ Сессия бота недоступна.", ephemeral=True)
            return

        steam_id = await resolve_steam_id(self.bot.session, steam_input)
        if not steam_id:
            await interaction.followup.send("❌ Не удалось верифицировать Steam профиль.", ephemeral=True)
            return

        async with aiosqlite.connect(DB_NAME) as db:
            await db.execute("""
                INSERT INTO users (discord_id, steam_id) VALUES (?, ?)
                ON CONFLICT(discord_id) DO UPDATE SET steam_id = excluded.steam_id
            """, (interaction.user.id, steam_id))
            await db.commit()

        await interaction.followup.send(f"✅ Профиль успешно привязан! SteamID64: `{steam_id}`", ephemeral=True)

    @app_commands.command(name="profile", description="Карточка бойца клана HATE и его статистика Rust")
    async def profile(self, interaction: discord.Interaction, member: Optional[discord.Member] = None):
        await interaction.response.defer()
        target = member or interaction.user

        async with aiosqlite.connect(DB_NAME) as db:
            async with db.execute("SELECT steam_id, xp, level, scrap FROM users WHERE discord_id = ?", (target.id,)) as cursor:
                row = await cursor.fetchone()

        if not row or not row[0]:
            await interaction.followup.send(f"❌ У {target.mention} не привязан Steam профиль (`/link`).", ephemeral=True)
            return

        steam_id, xp, lvl, scrap = row
        if not self.bot.session:
            return

        stats_url = f"http://api.steampowered.com/ISteamUserStats/GetUserStatsForGame/v0002/?appid={RUST_APP_ID}&key={STEAM_API_KEY}&steamid={steam_id}"
        summaries_url = f"http://api.steampowered.com/ISteamUser/GetPlayerSummaries/v0002/?key={STEAM_API_KEY}&steamids={steam_id}"

        try:
            async with self.bot.session.get(summaries_url) as res_sum:
                sum_data = await res_sum.json()
                players = sum_data.get("response", {}).get("players", [])
                if not players:
                    await interaction.followup.send("❌ Не удалось найти данные игрока в Steam.", ephemeral=True)
                    return
                p_data = players[0]
                avatar_url = p_data.get("avatarfull", "")
                personaname = p_data.get("personaname", "Неизвестно")

            async with self.bot.session.get(stats_url) as res_stats:
                if res_stats.status in (400, 403):
                    await interaction.followup.send(f"⚠️ Профиль Steam `{personaname}` приватен.", ephemeral=True)
                    return

                stats_data = await res_stats.json()
                user_stats = {item["name"]: item["value"] for item in stats_data.get("playerstats", {}).get("stats", [])}

                kills = user_stats.get("kill_player", 0)
                deaths = user_stats.get("deaths", 0)
                kd = round(kills / deaths, 2) if deaths > 0 else kills
                hours = user_stats.get("hours_passed", 0) // 3600 or user_stats.get("hours_played", 0)

                embed = discord.Embed(title=f"⚔️ Боец HATE | {personaname}", color=discord.Color.red())
                embed.set_thumbnail(url=avatar_url)
                embed.add_field(name="Discord", value=target.mention, inline=True)
                embed.add_field(name="SteamID64", value=f"`{steam_id}`", inline=True)
                embed.add_field(name="Уровень / XP", value=f"Lvl {lvl} (`{xp} XP`)", inline=True)
                embed.add_field(name="Часов в Rust", value=f"⏳ `{hours} ч.`", inline=True)
                embed.add_field(name="K/D Ratio", value=f"📊 `{kd}` (`{kills}` K / `{deaths}` D)", inline=True)
                embed.add_field(name="Баланс Scrap", value=f"🧩 `{scrap}`", inline=True)

                await interaction.followup.send(embed=embed)
        except Exception as e:
            await interaction.followup.send(f"❌ Ошибка сбора статистики: {e}", ephemeral=True)

# ==============================================================================
# COG 5: МОДЕРАЦИЯ И СИСТЕМА ПРЕДУПРЕЖДЕНИЙ (WARNS)
# ==============================================================================

class ModerationCog(commands.Cog):
    def __init__(self, bot: HateClanBot):
        self.bot = bot

    warn_group = app_commands.Group(name="warn", description="Управление предупреждениями и банами")

    @warn_group.command(name="add", description="Выдать предупреждение")
    @app_commands.checks.has_permissions(kick_members=True)
    async def warn_add(self, interaction: discord.Interaction, member: discord.Member, reason: str):
        await interaction.response.defer(ephemeral=True)
        max_warns = await get_setting(interaction.guild.id, "warn_limit") or 3

        async with aiosqlite.connect(DB_NAME) as db:
            await db.execute("""
                INSERT INTO warns (guild_id, discord_id, moderator_id, reason) VALUES (?, ?, ?, ?)
            """, (interaction.guild.id, member.id, interaction.user.id, reason))
            await db.commit()

            async with db.execute("SELECT COUNT(*) FROM warns WHERE guild_id = ? AND discord_id = ?", (interaction.guild.id, member.id)) as cursor:
                warn_count = (await cursor.fetchone())[0]

        embed = discord.Embed(title="⚠️ Выдано предупреждение", color=discord.Color.orange())
        embed.add_field(name="Боец", value=member.mention)
        embed.add_field(name="Модератор", value=interaction.user.mention)
        embed.add_field(name="Причина", value=reason, inline=False)
        embed.add_field(name="Всего варнов", value=f"`{warn_count} / {max_warns}`")
        
        await interaction.followup.send(embed=embed, ephemeral=True)
        await log_event(interaction.guild, embed)

        if warn_count >= max_warns:
            try:
                await member.ban(reason=f"Превышен лимит предупреждений ({warn_count}/{max_warns})")
                ban_embed = discord.Embed(
                    title="🔨 Автоматический Бан",
                    description=f"{member.mention} забанен за получение `{warn_count}/{max_warns}` варнов.",
                    color=discord.Color.red()
                )
                await interaction.channel.send(embed=ban_embed)
            except Exception as e:
                await interaction.followup.send(f"❌ Ошибка авто-бана: {e}", ephemeral=True)

    @warn_group.command(name="list", description="Посмотреть список варнов бойца")
    async def warn_list(self, interaction: discord.Interaction, member: Optional[discord.Member] = None):
        target = member or interaction.user
        max_warns = await get_setting(interaction.guild.id, "warn_limit") or 3

        async with aiosqlite.connect(DB_NAME) as db:
            async with db.execute("""
                SELECT id, moderator_id, reason, created_at FROM warns
                WHERE guild_id = ? AND discord_id = ? ORDER BY id DESC
            """, (interaction.guild.id, target.id)) as cursor:
                warns = await cursor.fetchall()

        if not warns:
            await interaction.response.send_message(f"✅ У {target.mention} нет активных предупреждений.", ephemeral=True)
            return

        embed = discord.Embed(title=f"📋 Варны | {target.display_name} ({len(warns)}/{max_warns})", color=discord.Color.yellow())
        for w_id, mod_id, reason, date_str in warns:
            embed.add_field(name=f"Варн #{w_id} | Модератор: <@{mod_id}> ({date_str})", value=f"Reason: *{reason}*", inline=False)

        await interaction.response.send_message(embed=embed, ephemeral=True)

# ==============================================================================
# COG 6: RUST КАЛЬКУЛЯТОРЫ (CRAFT & RAID)
# ==============================================================================

class CalculatorsCog(commands.Cog):
    def __init__(self, bot: HateClanBot):
        self.bot = bot

    @app_commands.command(name="craft", description="Калькулятор ресурсов для крафта в Rust")
    @app_commands.choices(item=[
        app_commands.Choice(name="C4 (Взрывчатка)", value="c4"),
        app_commands.Choice(name="Ракета", value="rocket"),
        app_commands.Choice(name="Разрывные патроны 5.56", value="explosive_ammo")
    ])
    async def craft_calc(self, interaction: discord.Interaction, item: str, amount: int = 1):
        if amount <= 0:
            await interaction.response.send_message("Количество должно быть > 0!", ephemeral=True)
            return

        res = {}
        if item == "c4":
            exp = 20 * amount
            res = {
                "Сера (Sulfur)": (exp * 10) + (exp * 50 * 2),
                "Уголь (Charcoal)": exp * 50 * 3,
                "Металл (Metal Frags)": exp * 10,
                "ТНК (Low Grade Fuel)": exp * 10,
                "Ткань": 5 * amount,
                "Микросхемы (Tech Trash)": 2 * amount
            }
            title = f"💣 Крафт: {amount}x C4"
        elif item == "rocket":
            exp = 10 * amount
            total_gp = (exp * 50) + (150 * amount)
            res = {
                "Сера (Sulfur)": (exp * 10) + (total_gp * 2),
                "Уголь (Charcoal)": total_gp * 3,
                "Металл (Metal Frags)": exp * 10,
                "ТНК (Low Grade Fuel)": exp * 10,
                "Трубы": 2 * amount
            }
            title = f"🚀 Крафт: {amount}x Ракет"
        elif item == "explosive_ammo":
            crafts = (amount + 1) // 2
            gp = 10 * crafts
            res = {
                "Сера (Sulfur)": (gp * 2) + (5 * crafts),
                "Уголь (Charcoal)": gp * 3,
                "Металл (Metal Frags)": 10 * crafts
            }
            title = f"💥 Крафт: {amount}x Разрывных патронов"

        embed = discord.Embed(title=title, color=discord.Color.orange())
        for name, val in res.items():
            embed.add_field(name=name, value=f"`{val:,}`", inline=False)
        await interaction.response.send_message(embed=embed)

    @app_commands.command(name="raid", description="Калькулятор стоимости выноса конструкций")
    @app_commands.choices(target=[
        app_commands.Choice(name="Деревянная стена / Дверь", value="wood"),
        app_commands.Choice(name="Каменная стена", value="stone"),
        app_commands.Choice(name="Металлическая стена", value="metal"),
        app_commands.Choice(name="МВК стена (Armored)", value="armored"),
        app_commands.Choice(name="Гаражная дверь", value="garage")
    ])
    async def raid_calc(interaction: discord.Interaction, target: str):
        costs = {
            "wood": "🪵 **Дерево:**\n• 1 C4 | 1 Ракета | 18 Разрывных | 2 Бобовые мины",
            "stone": "🪨 **Камень:**\n• 2 C4 | 4 Ракеты | 185 Разрывных | 10 Бобовых мин",
            "metal": "⚙️ **Металл:**\n• 4 C4 | 8 Ракет | 400 Разрывных",
            "armored": "🛡️ **МВК (Armored):**\n• 8 C4 | 15 Ракет | 799 Разрывных",
            "garage": "🚪 **Гаражная дверь:**\n• 1 C4 + 40 Разрывных | 3 Ракеты | 150 Разрывных"
        }
        embed = discord.Embed(title="💥 Затраты на рейд", description=costs.get(target, "Данные отсутствуют"), color=discord.Color.red())
        await interaction.response.send_message(embed=embed)

# ==============================================================================
# COG 7: СИСТЕМА ТИКЕТОВ И НАБОРА В КЛАН
# ==============================================================================

class TicketModal(discord.ui.Modal):
    def __init__(self, ticket_type: str):
        super().__init__(title=f"Анкета: {ticket_type}")
        self.ticket_type = ticket_type

        self.age = discord.ui.TextInput(label="Возраст", placeholder="18+", min_length=1, max_length=3)
        self.hours = discord.ui.TextInput(label="Часы в Rust", placeholder="2500", min_length=1, max_length=6)
        self.steam = discord.ui.TextInput(label="SteamID / Ссылка", placeholder="7656119...", min_length=5)
        self.info = discord.ui.TextInput(label="Опыт / Сильные стороны", style=discord.TextStyle.paragraph, required=False)

        self.add_item(self.age)
        self.add_item(self.hours)
        self.add_item(self.steam)
        self.add_item(self.info)

    async def on_submit(self, interaction: discord.Interaction):
        guild = interaction.guild
        cat_id = await get_setting(guild.id, "ticket_category_id")
        mod_role_id = await get_setting(guild.id, "mod_role_id")
        
        category = guild.get_channel(cat_id) if cat_id else None
        mod_role = guild.get_role(mod_role_id) if mod_role_id else None

        overwrites = {
            guild.default_role: discord.PermissionOverwrite(read_messages=False),
            interaction.user: discord.PermissionOverwrite(read_messages=True, send_messages=True, attach_files=True),
            guild.me: discord.PermissionOverwrite(read_messages=True, send_messages=True, manage_channels=True)
        }
        if mod_role:
            overwrites[mod_role] = discord.PermissionOverwrite(read_messages=True, send_messages=True)

        ch_name = f"ticket-{interaction.user.name}".lower()
        channel = await guild.create_text_channel(name=ch_name, category=category, overwrites=overwrites)

        async with aiosqlite.connect(DB_NAME) as db:
            await db.execute("INSERT INTO tickets (channel_id, user_id, guild_id, ticket_type) VALUES (?, ?, ?, ?)",
                             (channel.id, interaction.user.id, guild.id, self.ticket_type))
            await db.commit()

        embed = discord.Embed(title=f"🎫 Заявка: {self.ticket_type}", color=discord.Color.green())
        embed.add_field(name="Кандидат", value=interaction.user.mention)
        embed.add_field(name="Возраст", value=self.age.value)
        embed.add_field(name="Часы Rust", value=self.hours.value)
        embed.add_field(name="Steam", value=self.steam.value, inline=False)
        if self.info.value:
            embed.add_field(name="Дополнительно", value=self.info.value, inline=False)

        await channel.send(content=f"{interaction.user.mention} {mod_role.mention if mod_role else ''}", embed=embed, view=TicketControlView())
        await interaction.response.send_message(f"✅ Ваш тикет успешно создан: {channel.mention}", ephemeral=True)

class TicketControlView(discord.ui.View):
    def __init__(self):
        super().__init__(timeout=None)

    @discord.ui.button(label="Взять тикет", style=discord.ButtonStyle.green, custom_id="btn_claim_ticket")
    async def claim_ticket(self, interaction: discord.Interaction, button: discord.ui.Button):
        async with aiosqlite.connect(DB_NAME) as db:
            await db.execute("UPDATE tickets SET claimed_by = ? WHERE channel_id = ?", (interaction.user.id, interaction.channel.id))
            await db.commit()
        await interaction.response.send_message(f"🛠️ Тикет принят офицером: {interaction.user.mention}")

    @discord.ui.button(label="Закрыть тикет", style=discord.ButtonStyle.secondary, custom_id="btn_close_ticket")
    async def close_ticket(self, interaction: discord.Interaction, button: discord.ui.Button):
        await interaction.response.send_message("🔒 Тикет архивируется и удаляется...")
        
        embed = discord.Embed(title="📜 Лог закрытия тикета", color=discord.Color.orange())
        embed.add_field(name="Канал", value=interaction.channel.name)
        embed.add_field(name="Закрыл", value=interaction.user.mention)
        await log_event(interaction.guild, embed)

        async with aiosqlite.connect(DB_NAME) as db:
            await db.execute("DELETE FROM tickets WHERE channel_id = ?", (interaction.channel.id,))
            await db.commit()

        await asyncio.sleep(3)
        await interaction.channel.delete()

class TicketLaunchSelect(discord.ui.Select):
    def __init__(self):
        options = [
            discord.SelectOption(label="Заявка в клан HATE", description="Подача анкеты на вступление", emoji="⚔️"),
            discord.SelectOption(label="Жалоба на игрока", description="Сообщить модерации о нарушении", emoji="⚠️"),
            discord.SelectOption(label="Вопрос руководству", description="Задать приватный вопрос", emoji="❓")
        ]
        super().__init__(placeholder="Выберите тему для открытия тикета...", custom_id="ticket_launch_select", options=options)

    async def callback(self, interaction: discord.Interaction):
        await interaction.response.send_modal(TicketModal(self.values[0]))

class TicketLaunchView(discord.ui.View):
    def __init__(self):
        super().__init__(timeout=None)
        self.add_item(TicketLaunchSelect())

class TicketsCog(commands.Cog):
    def __init__(self, bot: HateClanBot):
        self.bot = bot

    @app_commands.command(name="ticket_panel", description="Заспавнить панель подачи заявок и тикетов")
    @app_commands.checks.has_permissions(administrator=True)
    async def ticket_panel(self, interaction: discord.Interaction):
        embed = discord.Embed(title="🛡️ Поддержка & Набор в Клан HATE", color=discord.Color.red())
        embed.description = "Выберите направление в меню ниже, чтобы открыть личную комнату с офицерами."
        await interaction.channel.send(embed=embed, view=TicketLaunchView())
        await interaction.response.send_message("Панель успешно отправлена!", ephemeral=True)

# ==============================================================================
# COG 8: ЭКОНОМИКА, УРОВНИ И МАГАЗИН SCRAP
# ==============================================================================

class ShopView(discord.ui.View):
    def __init__(self):
        super().__init__(timeout=None)

    @discord.ui.select(
        placeholder="Выберите предмет для покупки за Scrap...",
        options=[
            discord.SelectOption(label="VIP Роль (1 неделя)", value="role_vip", description="Стоимость: 5000 Scrap", emoji="⭐"),
            discord.SelectOption(label="Скипер ночи", value="role_skipper", description="Стоимость: 2500 Scrap", emoji="🌙")
        ]
    )
    async def buy_item(self, interaction: discord.Interaction, select: discord.ui.Select):
        cost = 5000 if select.values[0] == "role_vip" else 2500
        async with aiosqlite.connect(DB_NAME) as db:
            async with db.execute("SELECT scrap FROM users WHERE discord_id = ?", (interaction.user.id,)) as cursor:
                row = await cursor.fetchone()
                user_scrap = row[0] if row else 0

            if user_scrap < cost:
                await interaction.response.send_message("❌ Недостаточно Scrap!", ephemeral=True)
                return

            await db.execute("UPDATE users SET scrap = scrap - ? WHERE discord_id = ?", (cost, interaction.user.id))
            await db.commit()

        await interaction.response.send_message(f"🎉 Успешно куплено! Списано `{cost}` Scrap.", ephemeral=True)

class EconomyLevelCog(commands.Cog):
    def __init__(self, bot: HateClanBot):
        self.bot = bot
        self.cooldowns = {}

    @commands.Cog.listener()
    async def on_message(self, message: discord.Message):
        if message.author.bot or not message.guild:
            return

        now = datetime.now()
        usr_id = message.author.id
        if usr_id in self.cooldowns and (now - self.cooldowns[usr_id]).total_seconds() < 60:
            return

        self.cooldowns[usr_id] = now
        async with aiosqlite.connect(DB_NAME) as db:
            await db.execute("""
                INSERT INTO users (discord_id, xp, scrap) VALUES (?, 15, 10)
                ON CONFLICT(discord_id) DO UPDATE SET xp = xp + 15, scrap = scrap + 10
            """, (usr_id,))
            
            async with db.execute("SELECT xp, level FROM users WHERE discord_id = ?", (usr_id,)) as cursor:
                row = await cursor.fetchone()
                if row:
                    xp, lvl = row
                    next_lvl = int(math.sqrt(xp / 100)) + 1
                    if next_lvl > lvl:
                        await db.execute("UPDATE users SET level = ? WHERE discord_id = ?", (next_lvl, usr_id))
                        await message.channel.send(f"🎉 {message.author.mention} повысил уровень до **{next_lvl}**!")
            await db.commit()

    @app_commands.command(name="rank", description="Посмотреть ранг, уровень и Scrap баланс")
    async def rank(self, interaction: discord.Interaction):
        async with aiosqlite.connect(DB_NAME) as db:
            async with db.execute("SELECT xp, level, scrap FROM users WHERE discord_id = ?", (interaction.user.id,)) as cursor:
                row = await cursor.fetchone()
        
        xp, lvl, scrap = row if row else (0, 1, 0)
        await interaction.response.send_message(f"📊 **{interaction.user.name}** | Lvl: `{lvl}` | XP: `{xp}` | Scrap: `🧩 {scrap}`")

    @app_commands.command(name="shop", description="Магазин внутриклановых бонусов за Scrap")
    async def shop(self, interaction: discord.Interaction):
        embed = discord.Embed(title="🛒 Клановый Магазин HATE", color=discord.Color.gold())
        embed.description = "Обменивайте вашу текстовую активность на привилегии!"
        await interaction.response.send_message(embed=embed, view=ShopView())

# ==============================================================================
# COG 9: АВТОМОДЕРАЦИЯ И АУДИТ ЛОГ
# ==============================================================================

class AutoModAuditCog(commands.Cog):
    def __init__(self, bot: HateClanBot):
        self.bot = bot

    @commands.Cog.listener()
    async def on_message(self, message: discord.Message):
        if message.author.bot or not message.guild:
            return

        # Фильтрация рекламы Discord приглашений
        if "discord.gg/" in message.content or "discord.com/invite/" in message.content:
            await message.delete()
            await message.channel.send(f"❌ {message.author.mention}, реклама сторонних серверов запрещена!", delete_after=5)
            return

        # Защита от полного КАПСА
        if len(message.content) > 12 and sum(1 for c in message.content if c.isupper()) / len(message.content) > 0.75:
            await message.delete()
            await message.channel.send(f"❌ {message.author.mention}, отключите Caps Lock!", delete_after=5)

    @commands.Cog.listener()
    async def on_message_delete(self, message: discord.Message):
        if message.author.bot or not message.guild:
            return
        embed = discord.Embed(title="🗑️ Сообщение удалено", color=discord.Color.red())
        embed.add_field(name="Автор", value=message.author.mention)
        embed.add_field(name="Канал", value=message.channel.mention)
        embed.add_field(name="Текст", value=message.content or "*Вложение / Embed*", inline=False)
        await log_event(message.guild, embed)

# ==============================================================================
# COG 10: YOUTUBE МОНИТОРИНГ МЕДИАКАНАЛОВ
# ==============================================================================

class MediaCog(commands.Cog):
    def __init__(self, bot: HateClanBot):
        self.bot = bot
        self.youtube_loop.start()

    def cog_unload(self):
        self.youtube_loop.cancel()

    @tasks.loop(minutes=10)
    async def youtube_loop(self):
        if not YOUTUBE_API_KEY or not self.bot.session:
            return

        async with aiosqlite.connect(DB_NAME) as db:
            async with db.execute("SELECT channel_id_yt, last_video_id FROM youtube_tracks") as cursor:
                async for yt_id, last_vid in cursor:
                    url = f"https://www.googleapis.com/youtube/v3/search?key={YOUTUBE_API_KEY}&channelId={yt_id}&part=snippet,id&order=date&maxResults=1"
                    async with self.bot.session.get(url) as resp:
                        if resp.status == 200:
                            data = await resp.json()
                            items = data.get("items", [])
                            if items:
                                vid_id = items[0]["id"].get("videoId")
                                if vid_id and vid_id != last_vid:
                                    await db.execute("UPDATE youtube_tracks SET last_video_id = ? WHERE channel_id_yt = ?", (vid_id, yt_id))
                                    await db.commit()

                                    for guild in self.bot.guilds:
                                        m_ch_id = await get_setting(guild.id, "media_channel_id")
                                        if m_ch_id:
                                            ch = guild.get_channel(m_ch_id)
                                            if ch and isinstance(ch, discord.TextChannel):
                                                await ch.send(f"🎬 Новое медиа-видео клана!\nhttps://www.youtube.com/watch?v={vid_id}")

# ==============================================================================
# ЗАПУСК И ТОЧКА ВХОДА
# ==============================================================================

if __name__ == "__main__":
    if not DISCORD_TOKEN:
        logger.error("ОШИБКА: Токен DISCORD_TOKEN отсутствует в .env файле!")
    else:
        bot.run(DISCORD_TOKEN)