import os
import re
import sqlite3
import asyncio
from datetime import datetime, timedelta
import aiohttp
import discord
from discord import app_commands
from discord.ext import commands, tasks
from dotenv import load_dotenv

# Попытка импорта библиотеки для A2S Query к Rust-серверу
try:
    import a2s
    HAS_A2S = True
except ImportError:
    HAS_A2S = False

# Загрузка переменных окружения
load_dotenv()
DISCORD_TOKEN = os.getenv("DISCORD_TOKEN")
STEAM_API_KEY = os.getenv("STEAM_API_KEY")

RUST_APP_ID = "252490"
active_temp_channels = set()
wipe_data = {}

# ==============================================================================
# БАЗА ДАННЫХ (SQLite)
# ==============================================================================
DB_NAME = "hate_clan.db"

def init_db():
    conn = sqlite3.connect(DB_NAME)
    cursor = conn.cursor()
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS users (
            discord_id INTEGER PRIMARY KEY,
            steam_id TEXT NOT NULL
        )
    """)
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS wipe_responses (
            discord_id INTEGER PRIMARY KEY,
            status TEXT NOT NULL,
            reason TEXT,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS voice_settings (
            guild_id INTEGER PRIMARY KEY,
            creator_channel_id INTEGER NOT NULL,
            category_id INTEGER NOT NULL
        )
    """)
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS warns (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            guild_id INTEGER NOT NULL,
            discord_id INTEGER NOT NULL,
            moderator_id INTEGER NOT NULL,
            reason TEXT NOT NULL,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS warn_settings (
            guild_id INTEGER PRIMARY KEY,
            max_warns INTEGER DEFAULT 3
        )
    """)
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS guild_config (
            guild_id INTEGER PRIMARY KEY,
            leader_channel_id INTEGER,
            clan_role_id INTEGER
        )
    """)
    conn.commit()
    conn.close()

def db_save_steam_id(discord_id: int, steam_id: str):
    conn = sqlite3.connect(DB_NAME)
    cursor = conn.cursor()
    cursor.execute("INSERT OR REPLACE INTO users (discord_id, steam_id) VALUES (?, ?)", (discord_id, steam_id))
    conn.commit()
    conn.close()

def db_get_steam_id(discord_id: int) -> str | None:
    conn = sqlite3.connect(DB_NAME)
    cursor = conn.cursor()
    cursor.execute("SELECT steam_id FROM users WHERE discord_id = ?", (discord_id,))
    row = cursor.fetchone()
    conn.close()
    return row[0] if row else None

def db_get_all_steam_ids():
    conn = sqlite3.connect(DB_NAME)
    cursor = conn.cursor()
    cursor.execute("SELECT discord_id, steam_id FROM users")
    rows = cursor.fetchall()
    conn.close()
    return rows

def db_save_wipe_response(discord_id: int, status: str, reason: str = ""):
    conn = sqlite3.connect(DB_NAME)
    cursor = conn.cursor()
    cursor.execute("""
        INSERT OR REPLACE INTO wipe_responses (discord_id, status, reason, updated_at)
        VALUES (?, ?, ?, CURRENT_TIMESTAMP)
    """, (discord_id, status, reason))
    conn.commit()
    conn.close()

def db_get_wipe_responses():
    conn = sqlite3.connect(DB_NAME)
    cursor = conn.cursor()
    cursor.execute("""
        SELECT r.discord_id, r.status, r.reason, u.steam_id
        FROM wipe_responses r
        LEFT JOIN users u ON r.discord_id = u.discord_id
    """)
    rows = cursor.fetchall()
    conn.close()
    return rows

def db_clear_wipe_responses():
    conn = sqlite3.connect(DB_NAME)
    cursor = conn.cursor()
    cursor.execute("DELETE FROM wipe_responses")
    conn.commit()
    conn.close()

def db_save_voice_settings(guild_id: int, creator_channel_id: int, category_id: int):
    conn = sqlite3.connect(DB_NAME)
    cursor = conn.cursor()
    cursor.execute("""
        INSERT OR REPLACE INTO voice_settings (guild_id, creator_channel_id, category_id)
        VALUES (?, ?, ?)
    """, (guild_id, creator_channel_id, category_id))
    conn.commit()
    conn.close()

def db_get_voice_settings(guild_id: int):
    conn = sqlite3.connect(DB_NAME)
    cursor = conn.cursor()
    cursor.execute("SELECT creator_channel_id, category_id FROM voice_settings WHERE guild_id = ?", (guild_id,))
    row = cursor.fetchone()
    conn.close()
    return row if row else (None, None)

def db_set_leader_channel(guild_id: int, channel_id: int):
    conn = sqlite3.connect(DB_NAME)
    cursor = conn.cursor()
    cursor.execute("""
        INSERT INTO guild_config (guild_id, leader_channel_id) VALUES (?, ?)
        ON CONFLICT(guild_id) DO UPDATE SET leader_channel_id=excluded.leader_channel_id
    """, (guild_id, channel_id))
    conn.commit()
    conn.close()

def db_set_clan_role(guild_id: int, role_id: int):
    conn = sqlite3.connect(DB_NAME)
    cursor = conn.cursor()
    cursor.execute("""
        INSERT INTO guild_config (guild_id, clan_role_id) VALUES (?, ?)
        ON CONFLICT(guild_id) DO UPDATE SET clan_role_id=excluded.clan_role_id
    """, (guild_id, role_id))
    conn.commit()
    conn.close()

def db_get_guild_config(guild_id: int) -> dict:
    conn = sqlite3.connect(DB_NAME)
    cursor = conn.cursor()
    cursor.execute("SELECT leader_channel_id, clan_role_id FROM guild_config WHERE guild_id = ?", (guild_id,))
    row = cursor.fetchone()
    conn.close()
    if row:
        return {"leader_channel_id": row[0], "clan_role_id": row[1]}
    return {"leader_channel_id": None, "clan_role_id": None}

def db_add_warn(guild_id: int, discord_id: int, moderator_id: int, reason: str) -> int:
    conn = sqlite3.connect(DB_NAME)
    cursor = conn.cursor()
    cursor.execute("""
        INSERT INTO warns (guild_id, discord_id, moderator_id, reason)
        VALUES (?, ?, ?, ?)
    """, (guild_id, discord_id, moderator_id, reason))
    conn.commit()
    cursor.execute("SELECT COUNT(*) FROM warns WHERE guild_id = ? AND discord_id = ?", (guild_id, discord_id))
    count = cursor.fetchone()[0]
    conn.close()
    return count

def db_get_warns(guild_id: int, discord_id: int):
    conn = sqlite3.connect(DB_NAME)
    cursor = conn.cursor()
    cursor.execute("""
        SELECT id, moderator_id, reason, created_at FROM warns
        WHERE guild_id = ? AND discord_id = ?
        ORDER BY id DESC
    """, (guild_id, discord_id))
    rows = cursor.fetchall()
    conn.close()
    return rows

def db_remove_warn(guild_id: int, discord_id: int) -> bool:
    conn = sqlite3.connect(DB_NAME)
    cursor = conn.cursor()
    cursor.execute("""
        DELETE FROM warns WHERE id = (
            SELECT id FROM warns WHERE guild_id = ? AND discord_id = ? ORDER BY id DESC LIMIT 1
        )
    """, (guild_id, discord_id))
    affected = cursor.rowcount
    conn.commit()
    conn.close()
    return affected > 0

def db_clear_warns(guild_id: int, discord_id: int):
    conn = sqlite3.connect(DB_NAME)
    cursor = conn.cursor()
    cursor.execute("DELETE FROM warns WHERE guild_id = ? AND discord_id = ?", (guild_id, discord_id))
    conn.commit()
    conn.close()

def db_set_max_warns(guild_id: int, max_warns: int):
    conn = sqlite3.connect(DB_NAME)
    cursor = conn.cursor()
    cursor.execute("INSERT OR REPLACE INTO warn_settings (guild_id, max_warns) VALUES (?, ?)", (guild_id, max_warns))
    conn.commit()
    conn.close()

def db_get_max_warns(guild_id: int) -> int:
    conn = sqlite3.connect(DB_NAME)
    cursor = conn.cursor()
    cursor.execute("SELECT max_warns FROM warn_settings WHERE guild_id = ?", (guild_id,))
    row = cursor.fetchone()
    conn.close()
    return row[0] if row else 3

# ==============================================================================
# ВСПОМОГАТЕЛЬНЫЕ ФУНКЦИИ
# ==============================================================================

async def get_rust_server_info(ip: str, port: int) -> dict | None:
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

async def resolve_steam_id(session: aiohttp.ClientSession, input_str: str) -> str | None:
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

async def build_report_embed(is_final: bool = False) -> discord.Embed:
    responses = await asyncio.to_thread(db_get_wipe_responses)
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

async def update_leader_report(guild: discord.Guild, bot: commands.Bot):
    config = await asyncio.to_thread(db_get_guild_config, guild.id)
    leader_channel_id = config.get("leader_channel_id")
    if not leader_channel_id:
        return
    channel = guild.get_channel(leader_channel_id)
    if channel:
        embed = await build_report_embed(is_final=False)
        async for msg in channel.history(limit=5):
            if msg.author == bot.user and msg.embeds and "Отчет готовности" in msg.embeds[0].title:
                await msg.edit(embed=embed)
                return
        await channel.send(embed=embed)

# ==============================================================================
# UI И МОДАЛЬНЫЕ ОКНА
# ==============================================================================

class LateReasonModal(discord.ui.Modal, title="Опоздание на вайп"):
    reason = discord.ui.TextInput(
        label="Время и причина опоздания",
        style=discord.TextStyle.long,
        placeholder="Например: Задержусь на 30 минут, с работы",
        required=True,
        max_length=300
    )

    async def on_submit(self, interaction: discord.Interaction):
        await asyncio.to_thread(db_save_wipe_response, interaction.user.id, "Опоздает", self.reason.value)
        await interaction.response.send_message("Ваш статус **'Опоздаю'** записан!", ephemeral=True)
        if interaction.guild:
            await update_leader_report(interaction.guild, interaction.client)

class AbsentReasonModal(discord.ui.Modal, title="Отсутствие на вайпе"):
    reason = discord.ui.TextInput(
        label="Причина отсутствия",
        style=discord.TextStyle.long,
        placeholder="Например: Уезжаю, нет доступа к ПК",
        required=True,
        max_length=300
    )

    async def on_submit(self, interaction: discord.Interaction):
        await asyncio.to_thread(db_save_wipe_response, interaction.user.id, "Отсутствует", self.reason.value)
        await interaction.response.send_message("Ваш статус **'Не буду'** записан!", ephemeral=True)
        if interaction.guild:
            await update_leader_report(interaction.guild, interaction.client)

class WipeView(discord.ui.View):
    def __init__(self):
        super().__init__(timeout=None)

    @discord.ui.button(label="Буду на вайпе", style=discord.ButtonStyle.success, custom_id="wipe_attending")
    async def attending_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        await asyncio.to_thread(db_save_wipe_response, interaction.user.id, "Идет", "Без задержек")
        await interaction.response.send_message("Вы подтвердили участие в вайпе!", ephemeral=True)
        if interaction.guild:
            await update_leader_report(interaction.guild, interaction.client)

    @discord.ui.button(label="Опоздаю", style=discord.ButtonStyle.secondary, custom_id="wipe_late")
    async def late_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        await interaction.response.send_modal(LateReasonModal())

    @discord.ui.button(label="Не буду", style=discord.ButtonStyle.danger, custom_id="wipe_absent")
    async def absent_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        await interaction.response.send_modal(AbsentReasonModal())

class AdminWipeModal(discord.ui.Modal, title="📢 Анонс и настройка вайпа"):
    connect_cmd = discord.ui.TextInput(
        label="Команда подключения",
        placeholder="connect 185.220.x.x:28015",
        required=True
    )
    date_time = discord.ui.TextInput(
        label="Дата и время вайпа (ДД.ММ.ГГГГ ЧЧ:ММ)",
        placeholder="25.10.2026 17:00",
        required=True
    )

    async def on_submit(self, interaction: discord.Interaction):
        await interaction.response.defer(ephemeral=True)
        try:
            wipe_dt = datetime.strptime(self.date_time.value, "%d.%m.%Y %H:%M")
        except ValueError:
            await interaction.followup.send("❌ Неверный формат даты! Используйте `ДД.ММ.ГГГГ ЧЧ:ММ`", ephemeral=True)
            return

        await asyncio.to_thread(db_clear_wipe_responses)

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
        embed.set_footer(text="Выберите ваш статус с помощью кнопок ниже:")

        global wipe_data
        wipe_data = {
            "datetime": wipe_dt,
            "channel_id": interaction.channel_id,
            "guild_id": interaction.guild.id if interaction.guild else None,
            "ping_15m_sent": False,
            "wipe_started_sent": False,
            "connect": self.connect_cmd.value
        }

        if interaction.channel:
            msg = await interaction.channel.send(embed=embed, view=WipeView())
            wipe_data["message_id"] = msg.id

        if not check_wipe_time.is_running():
            check_wipe_time.start()

        await interaction.followup.send("✅ Анонс вайпа успешно запущен в этом канале!", ephemeral=True)

class AdminWarnLimitModal(discord.ui.Modal, title="⚙️ Настройка лимита варнов"):
    limit = discord.ui.TextInput(
        label="Лимит предупреждений до авто-бана",
        placeholder="3",
        required=True
    )

    async def on_submit(self, interaction: discord.Interaction):
        if not self.limit.value.isdigit() or int(self.limit.value) <= 0:
            await interaction.response.send_message("❌ Введите число больше 0!", ephemeral=True)
            return

        max_w = int(self.limit.value)
        if interaction.guild:
            await asyncio.to_thread(db_set_max_warns, interaction.guild.id, max_w)
        await interaction.response.send_message(f"⚙️ Новый лимит варнов для автоматического бана равен `{max_w}`", ephemeral=True)

class SetupSelectView(discord.ui.View):
    def __init__(self):
        super().__init__(timeout=180)

    @discord.ui.select(cls=discord.ui.RoleSelect, placeholder="Выберите основную роль клана...", row=0)
    async def select_role(self, interaction: discord.Interaction, select: discord.ui.RoleSelect):
        if not interaction.guild:
            return
        selected_role = select.values[0]
        await asyncio.to_thread(db_set_clan_role, interaction.guild.id, selected_role.id)
        await interaction.response.send_message(f"✅ Роль клана успешно установлена: {selected_role.mention}", ephemeral=True)

    @discord.ui.select(cls=discord.ui.ChannelSelect, channel_types=[discord.ChannelType.text], placeholder="Выберите канал для отчетов руководству...", row=1)
    async def select_channel(self, interaction: discord.Interaction, select: discord.ui.ChannelSelect):
        if not interaction.guild:
            return
        selected_channel = select.values[0]
        await asyncio.to_thread(db_set_leader_channel, interaction.guild.id, selected_channel.id)
        await interaction.response.send_message(f"✅ Канал сводки руководства установлен: {selected_channel.mention}", ephemeral=True)

class AdminPanelView(discord.ui.View):
    def __init__(self):
        super().__init__(timeout=None)

    @discord.ui.button(label="📢 Анонс Вайпа", style=discord.ButtonStyle.primary, custom_id="admin_panel_wipe", row=0)
    async def create_wipe(self, interaction: discord.Interaction, button: discord.ui.Button):
        if not interaction.user.guild_permissions.administrator:
            await interaction.response.send_message("❌ У вас нет прав администратора!", ephemeral=True)
            return
        await interaction.response.send_modal(AdminWipeModal())

    @discord.ui.button(label="📊 Сводка Явки", style=discord.ButtonStyle.success, custom_id="admin_panel_report", row=0)
    async def get_report(self, interaction: discord.Interaction, button: discord.ui.Button):
        if not interaction.user.guild_permissions.administrator:
            await interaction.response.send_message("❌ У вас нет прав администратора!", ephemeral=True)
            return
        await interaction.response.defer(ephemeral=True)
        embed = await build_report_embed(is_final=False)
        await interaction.followup.send(embed=embed, ephemeral=True)

    @discord.ui.button(label="📣 Пинать неопределившихся", style=discord.ButtonStyle.secondary, custom_id="admin_panel_ping", row=0)
    async def ping_missing(self, interaction: discord.Interaction, button: discord.ui.Button):
        if not interaction.user.guild_permissions.administrator or not interaction.guild:
            await interaction.response.send_message("❌ У вас нет прав администратора!", ephemeral=True)
            return

        await interaction.response.defer(ephemeral=True)
        responses = await asyncio.to_thread(db_get_wipe_responses)
        responded_ids = {r[0] for r in responses}

        config = await asyncio.to_thread(db_get_guild_config, interaction.guild.id)
        clan_role_id = config.get("clan_role_id")

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
            await interaction.followup.send(f"✅ Отправлен пинг {len(missing_mentions)} бойцам!", ephemeral=True)
        else:
            await interaction.followup.send("✅ Все бойцы уже проголосовали или роль клана еще не настроена через `/setup`!", ephemeral=True)

    @discord.ui.button(label="🆔 Все SteamID", style=discord.ButtonStyle.primary, custom_id="admin_panel_steam_list", row=1)
    async def get_steam_list(self, interaction: discord.Interaction, button: discord.ui.Button):
        if not interaction.user.guild_permissions.administrator:
            await interaction.response.send_message("❌ У вас нет прав администратора!", ephemeral=True)
            return

        await interaction.response.defer(ephemeral=True)
        steam_data = await asyncio.to_thread(db_get_all_steam_ids)
        if not steam_data:
            await interaction.followup.send("❌ В базе данных пока нет привязанных SteamID.", ephemeral=True)
            return

        lines = [f"<@{discord_id}> — `{steam_id}`" for discord_id, steam_id in steam_data]
        embed = discord.Embed(title="📋 Список привязанных SteamID клана", description="\n".join(lines)[:1900], color=discord.Color.blue())
        await interaction.followup.send(embed=embed, ephemeral=True)

    @discord.ui.button(label="🔔 Пинг без Steam", style=discord.ButtonStyle.danger, custom_id="admin_panel_notify_no_steam", row=1)
    async def notify_no_steam(self, interaction: discord.Interaction, button: discord.ui.Button):
        if not interaction.user.guild_permissions.administrator or not interaction.guild:
            await interaction.response.send_message("❌ У вас нет прав администратора!", ephemeral=True)
            return

        await interaction.response.defer(ephemeral=True)
        steam_data = await asyncio.to_thread(db_get_all_steam_ids)
        linked_ids = {r[0] for r in steam_data}

        config = await asyncio.to_thread(db_get_guild_config, interaction.guild.id)
        clan_role_id = config.get("clan_role_id")

        unlinked_mentions = []
        if clan_role_id:
            role = interaction.guild.get_role(clan_role_id)
            if role:
                for member in role.members:
                    if not member.bot and member.id not in linked_ids:
                        unlinked_mentions.append(member.mention)

        if unlinked_mentions and interaction.channel:
            msg_text = (
                "⚠️ **НАПОМИНАНИЕ О ПРИВЯЗКЕ STEAM** ⚠️\n"
                "Следующие бойцы клана ещё не привязали свой SteamID в боте:\n"
                + " ".join(unlinked_mentions[:30]) +
                "\n\nПожалуйста, напишите команду `/link <ваш_steamid_или_ссылка>` прямо сейчас!"
            )
            await interaction.channel.send(msg_text)
            await interaction.followup.send(f"✅ Напоминание отправлено {len(unlinked_mentions)} бойцам!", ephemeral=True)
        else:
            await interaction.followup.send("✅ Все участники клана с ролью уже привязали SteamID (или роль клана не настроена через `/setup`)!", ephemeral=True)

    @discord.ui.button(label="⚙️ Настройки сервера", style=discord.ButtonStyle.secondary, custom_id="admin_panel_settings", row=2)
    async def open_settings(self, interaction: discord.Interaction, button: discord.ui.Button):
        if not interaction.user.guild_permissions.administrator:
            await interaction.response.send_message("❌ У вас нет прав администратора!", ephemeral=True)
            return
        
        embed = discord.Embed(title="⚙️ Быстрая настройка сервера", description="Выберите параметры ниже:", color=discord.Color.blue())
        await interaction.response.send_message(embed=embed, view=SetupSelectView(), ephemeral=True)

    @discord.ui.button(label="⚠️ Лимит Варнов", style=discord.ButtonStyle.secondary, custom_id="admin_panel_warn_limit", row=2)
    async def warn_limit(self, interaction: discord.Interaction, button: discord.ui.Button):
        if not interaction.user.guild_permissions.administrator:
            await interaction.response.send_message("❌ У вас нет прав администратора!", ephemeral=True)
            return
        await interaction.response.send_modal(AdminWarnLimitModal())

# ==============================================================================
# БОТ И СОБЫТИЯ
# ==============================================================================

class HateClanBot(commands.Bot):
    def __init__(self):
        intents = discord.Intents.default()
        intents.message_content = True
        intents.members = True
        intents.voice_states = True
        super().__init__(command_prefix="!", intents=intents)
        self.session: aiohttp.ClientSession | None = None

    async def setup_hook(self):
        self.session = aiohttp.ClientSession()
        self.add_view(WipeView())
        self.add_view(AdminPanelView())

    async def close(self):
        if self.session:
            await self.session.close()
        await super().close()

bot = HateClanBot()

@bot.event
async def on_ready():
    init_db()
    print(f"Бот {bot.user} успешно запущен!")

# --- ТЕКСТОВАЯ АЛЬТЕРНАТИВА И СИНХРОНИЗАЦИЯ ---

@bot.command(name="admin_panel")
@commands.has_permissions(administrator=True)
async def text_admin_panel(ctx: commands.Context):
    embed = discord.Embed(
        title="🛠️ ПАНЕЛЬ УПРАВЛЕНИЯ КЛАНОМ HATE",
        description="Используйте кнопки ниже для быстрого управления кланом:",
        color=discord.Color.dark_grey()
    )
    await ctx.send(embed=embed, view=AdminPanelView())

@bot.command(name="sync")
@commands.has_permissions(administrator=True)
async def manual_sync(ctx: commands.Context):
    if not ctx.guild:
        return
    msg = await ctx.send("⏳ Выполняется принудительная синхронизация команд...")
    bot.tree.copy_global_to(guild=ctx.guild)
    synced = await bot.tree.sync(guild=ctx.guild)
    await msg.edit(content=f"✅ Успешно привязано {len(synced)} слэш-команд прямо к этому серверу! Обновите клиенты Discord (CTRL+R).")

# --- СЛЭШ-КОМАНДЫ НАСТРОЙКИ СЕРВЕРА ---

@bot.tree.command(name="setup", description="Открыть интерактивное меню настройки роли и каналов клана (Админ)")
@app_commands.checks.has_permissions(administrator=True)
async def setup_cmd(interaction: discord.Interaction):
    embed = discord.Embed(
        title="⚙️ Интерактивная настройка бота HATE",
        description="Используйте выпадающие меню ниже, чтобы выбрать роль клана и канал для авто-сводки руководства:",
        color=discord.Color.blue()
    )
    await interaction.response.send_message(embed=embed, view=SetupSelectView(), ephemeral=True)

@bot.tree.command(name="set_clan_role", description="Установить основную роль клана для пингов и отчетов (Админ)")
@app_commands.checks.has_permissions(administrator=True)
@app_commands.describe(role="Роль вашего клана")
async def set_clan_role_cmd(interaction: discord.Interaction, role: discord.Role):
    if not interaction.guild:
        return
    await asyncio.to_thread(db_set_clan_role, interaction.guild.id, role.id)
    await interaction.response.send_message(f"✅ Роль клана успешно установлена: {role.mention}", ephemeral=True)

@bot.tree.command(name="set_leader_channel", description="Назначить канал для автоматической сводки руководству (Админ)")
@app_commands.checks.has_permissions(administrator=True)
@app_commands.describe(channel="Канал руководства")
async def set_leader_channel_cmd(interaction: discord.Interaction, channel: discord.TextChannel):
    if not interaction.guild:
        return
    await asyncio.to_thread(db_set_leader_channel, interaction.guild.id, channel.id)
    await interaction.response.send_message(f"✅ Канал {channel.mention} установлен как канал для авто-сводки руководства!", ephemeral=True)

@bot.tree.command(name="settings", description="Посмотреть текущие настройки бота для сервера")
@app_commands.checks.has_permissions(administrator=True)
async def settings_cmd(interaction: discord.Interaction):
    if not interaction.guild:
        return
    config = await asyncio.to_thread(db_get_guild_config, interaction.guild.id)
    max_warns = await asyncio.to_thread(db_get_max_warns, interaction.guild.id)
    creator_id, category_id = await asyncio.to_thread(db_get_voice_settings, interaction.guild.id)

    role_str = f"<@&{config['clan_role_id']}>" if config['clan_role_id'] else "❌ *Не настроена*"
    channel_str = f"<#{config['leader_channel_id']}>" if config['leader_channel_id'] else "❌ *Не настроен*"
    voice_str = f"<#{creator_id}>" if creator_id else "❌ *Не настроены*"

    embed = discord.Embed(title=f"⚙️ Настройки бота | {interaction.guild.name}", color=discord.Color.gold())
    embed.add_field(name="Роль клана", value=role_str, inline=False)
    embed.add_field(name="Канал сводки руководству", value=channel_str, inline=False)
    embed.add_field(name="Динамические голосовые", value=voice_str, inline=False)
    embed.add_field(name="Лимит варнов для бана", value=f"`{max_warns}`", inline=False)

    await interaction.response.send_message(embed=embed, ephemeral=True)

@bot.tree.command(name="admin_panel", description="Заспавнить интерактивную админ-панель (Админ)")
@app_commands.checks.has_permissions(administrator=True)
async def admin_panel_cmd(interaction: discord.Interaction):
    embed = discord.Embed(
        title="🛠️ ПАНЕЛЬ УПРАВЛЕНИЯ КЛАНОМ HATE",
        description="Используйте кнопки ниже для быстрого управления кланом:",
        color=discord.Color.dark_grey()
    )
    await interaction.response.send_message(embed=embed, view=AdminPanelView())

@bot.tree.command(name="steam_list", description="Посмотреть список всех привязанных SteamID (Админ)")
@app_commands.checks.has_permissions(administrator=True)
async def steam_list(interaction: discord.Interaction):
    await interaction.response.defer(ephemeral=True)
    steam_data = await asyncio.to_thread(db_get_all_steam_ids)
    if not steam_data:
        await interaction.followup.send("❌ В базе данных пока нет привязанных SteamID.", ephemeral=True)
        return

    lines = [f"<@{discord_id}> — `{steam_id}`" for discord_id, steam_id in steam_data]
    embed = discord.Embed(title="📋 Список привязанных SteamID клана", description="\n".join(lines)[:1900], color=discord.Color.blue())
    await interaction.followup.send(embed=embed, ephemeral=True)

@bot.tree.command(name="notify_no_steam", description="Уведомить игроков с ролью клана, которые ещё не привязали SteamID (Админ)")
@app_commands.checks.has_permissions(administrator=True)
async def notify_no_steam_cmd(interaction: discord.Interaction):
    if not interaction.guild or not interaction.channel:
        return
    await interaction.response.defer(ephemeral=True)
    steam_data = await asyncio.to_thread(db_get_all_steam_ids)
    linked_ids = {r[0] for r in steam_data}

    config = await asyncio.to_thread(db_get_guild_config, interaction.guild.id)
    clan_role_id = config.get("clan_role_id")

    unlinked_mentions = []
    if clan_role_id:
        role = interaction.guild.get_role(clan_role_id)
        if role:
            for member in role.members:
                if not member.bot and member.id not in linked_ids:
                    unlinked_mentions.append(member.mention)

    if unlinked_mentions:
        msg_text = (
            "⚠️ **НАПОМИНАНИЕ О ПРИВЯЗКЕ STEAM** ⚠️\n"
            "Следующие бойцы клана ещё не привязали свой SteamID в боте:\n"
            + " ".join(unlinked_mentions[:30]) +
            "\n\nПожалуйста, напишите команду `/link <ваш_steamid_или_ссылка>` прямо сейчас!"
        )
        await interaction.channel.send(msg_text)
        await interaction.followup.send(f"✅ Напоминание отправлено {len(unlinked_mentions)} бойцам!", ephemeral=True)
    else:
        await interaction.followup.send("✅ Все участники клана с ролью уже привязали SteamID (или роль не настроена через `/setup`)!", ephemeral=True)

# --- ДИНАМИЧЕСКИЕ ГОЛОСОВЫЕ КАНАЛЫ ---

@bot.tree.command(name="voice_setup", description="Настроить систему динамических голосовых каналов (Админ)")
@app_commands.checks.has_permissions(administrator=True)
@app_commands.describe(
    creator_channel="Голосовой канал, при входе в который создается комната",
    category="Категория, в которой будут создаваться приватные комнаты"
)
async def voice_setup(
    interaction: discord.Interaction, 
    creator_channel: discord.VoiceChannel, 
    category: discord.CategoryChannel
):
    if not interaction.guild:
        return
    await asyncio.to_thread(db_save_voice_settings, interaction.guild.id, creator_channel.id, category.id)
    embed = discord.Embed(title="⚙️ Динамические голосовые каналы настроены!", color=discord.Color.green())
    embed.add_field(name="Канал-создатель", value=creator_channel.mention, inline=False)
    embed.add_field(name="Категория для комнат", value=category.name, inline=False)
    await interaction.response.send_message(embed=embed, ephemeral=True)

@bot.event
async def on_voice_state_update(member: discord.Member, before: discord.VoiceState, after: discord.VoiceState):
    if not member.guild:
        return

    creator_channel_id, category_id = await asyncio.to_thread(db_get_voice_settings, member.guild.id)
    if not creator_channel_id or not category_id:
        return

    if after.channel and after.channel.id == creator_channel_id:
        guild = member.guild
        category = guild.get_channel(category_id)
        if category and isinstance(category, discord.CategoryChannel):
            channel_name = f"🔊 Рубка {member.display_name}"
            temp_channel = await guild.create_voice_channel(name=channel_name, category=category)
            active_temp_channels.add(temp_channel.id)
            try:
                await member.move_to(temp_channel)
            except (discord.Forbidden, discord.HTTPException):
                pass

    if before.channel and before.channel.id in active_temp_channels:
        if len(before.channel.members) == 0:
            active_temp_channels.remove(before.channel.id)
            try:
                await before.channel.delete(reason="Временный канал пуст")
            except (discord.NotFound, discord.Forbidden, discord.HTTPException):
                pass

# --- ТРЕКИНГ И СТАТИСТИКА STEAM ---

@bot.tree.command(name="link", description="Привязать ваш SteamID64 или ссылку на профиль к аккаунту Discord")
@app_commands.describe(steam_id_or_url="SteamID64, ссылка на профиль или custom URL")
async def link_steam(interaction: discord.Interaction, steam_id_or_url: str):
    await interaction.response.defer(ephemeral=True)
    if not bot.session:
        await interaction.followup.send("❌ Сессия подключения недоступна.", ephemeral=True)
        return

    steam_id = await resolve_steam_id(bot.session, steam_id_or_url)
    
    if not steam_id:
        await interaction.followup.send("❌ Не удалось найти Steam профиль. Проверьте правильность введенных данных.", ephemeral=True)
        return

    await asyncio.to_thread(db_save_steam_id, interaction.user.id, steam_id)
    await interaction.followup.send(f"✅ Профиль успешно привязан! Ваш SteamID64: `{steam_id}`", ephemeral=True)

@bot.tree.command(name="profile", description="Карточка игрока и его статистика в Rust")
@app_commands.describe(member="Участник клана (по умолчанию вы)")
async def profile(interaction: discord.Interaction, member: discord.Member = None):
    await interaction.response.defer()
    target = member or interaction.user
    steam_id = await asyncio.to_thread(db_get_steam_id, target.id)

    if not steam_id:
        await interaction.followup.send(f"❌ У пользователя {target.mention} не привязан Steam профиль.", ephemeral=True)
        return

    if not bot.session:
        await interaction.followup.send("❌ Сессия подключения недоступна.", ephemeral=True)
        return

    stats_url = f"http://api.steampowered.com/ISteamUserStats/GetUserStatsForGame/v0002/?appid={RUST_APP_ID}&key={STEAM_API_KEY}&steamid={steam_id}"
    summaries_url = f"http://api.steampowered.com/ISteamUser/GetPlayerSummaries/v0002/?key={STEAM_API_KEY}&steamids={steam_id}"

    try:
        async with bot.session.get(summaries_url) as res_sum:
            sum_data = await res_sum.json()
            players = sum_data.get("response", {}).get("players", [])
            if not players:
                await interaction.followup.send("❌ Не удалось найти профиль Steam.", ephemeral=True)
                return
            player_data = players[0]
            avatar_url = player_data.get("avatarfull", "")
            personaname = player_data.get("personaname", "Неизвестно")

        async with bot.session.get(stats_url) as res_stats:
            if res_stats.status in (400, 403):
                await interaction.followup.send(f"⚠️ Профиль Steam `{personaname}` закрыт настройками приватности.", ephemeral=True)
                return
            
            stats_data = await res_stats.json()
            user_stats = {item["name"]: item["value"] for item in stats_data.get("playerstats", {}).get("stats", [])}

            kills = user_stats.get("kill_player", 0)
            deaths = user_stats.get("deaths", 0)
            kd = round(kills / deaths, 2) if deaths > 0 else kills
            seconds_played = user_stats.get("hours_passed", 0)
            hours = user_stats.get("hours_played", seconds_played // 3600)

            embed = discord.Embed(title=f"⚔️ Профиль бойца HATE | {personaname}", color=discord.Color.red())
            embed.set_thumbnail(url=avatar_url)
            embed.add_field(name="Discord", value=target.mention, inline=True)
            embed.add_field(name="SteamID64", value=f"`{steam_id}`", inline=True)
            embed.add_field(name="Часов в Rust", value=f"⏳ {hours} ч.", inline=False)
            embed.add_field(name="Убийств", value=f"⚔️ {kills}", inline=True)
            embed.add_field(name="Смертей", value=f"💀 {deaths}", inline=True)
            embed.add_field(name="K/D Ratio", value=f"📊 {kd}", inline=True)

            await interaction.followup.send(embed=embed)

    except Exception as e:
        await interaction.followup.send(f"❌ Ошибка получения статистики: {str(e)}", ephemeral=True)

# --- СИСТЕМА ПРЕДУПРЕЖДЕНИЙ (WARNS) ---

warn_group = app_commands.Group(name="warn", description="Управление предупреждениями и банами (Админ)")

@warn_group.command(name="add", description="Выдать варн пользователю")
@app_commands.checks.has_permissions(administrator=True)
@app_commands.describe(member="Боец", reason="Причина предупреждения")
async def warn_add(interaction: discord.Interaction, member: discord.Member, reason: str):
    if not interaction.guild:
        return
    await interaction.response.defer(ephemeral=True)

    warn_count = await asyncio.to_thread(db_add_warn, interaction.guild.id, member.id, interaction.user.id, reason)
    max_warns = await asyncio.to_thread(db_get_max_warns, interaction.guild.id)

    embed = discord.Embed(title="⚠️ Выдано предупреждение", color=discord.Color.orange())
    embed.add_field(name="Боец", value=member.mention, inline=True)
    embed.add_field(name="Причина", value=reason, inline=False)
    embed.add_field(name="Всего варнов", value=f"`{warn_count} / {max_warns}`", inline=True)

    await interaction.followup.send(embed=embed, ephemeral=True)

    try:
        await member.send(f"⚠️ Вам выдано предупреждение на сервере **{interaction.guild.name}**.\n**Причина:** {reason}\n**Всего варнов:** {warn_count}/{max_warns}")
    except Exception:
        pass

    if warn_count >= max_warns and interaction.channel:
        try:
            await member.ban(reason=f"Превышен лимит предупреждений ({warn_count}/{max_warns}). Последний варн: {reason}")
            ban_embed = discord.Embed(
                title="🔨 Автоматический бан игрока",
                description=f"Игрок {member.mention} был забанен за достижение лимита варнов (`{warn_count}/{max_warns}`).",
                color=discord.Color.red()
            )
            await interaction.channel.send(embed=ban_embed)
        except Exception as e:
            await interaction.channel.send(f"❌ Не удалось автоматически забанить {member.mention}: {str(e)}")

@warn_group.command(name="remove", description="Снять последнее предупреждение с бойца")
@app_commands.checks.has_permissions(administrator=True)
@app_commands.describe(member="Боец")
async def warn_remove(interaction: discord.Interaction, member: discord.Member):
    if not interaction.guild:
        return
    success = await asyncio.to_thread(db_remove_warn, interaction.guild.id, member.id)
    if success:
        warns = await asyncio.to_thread(db_get_warns, interaction.guild.id, member.id)
        await interaction.response.send_message(f"✅ Последнее предупреждение снято с {member.mention}. Осталось варнов: `{len(warns)}`", ephemeral=True)
    else:
        await interaction.response.send_message(f"❌ У пользователя {member.mention} нет активных варнов.", ephemeral=True)

@warn_group.command(name="list", description="Посмотреть список варнов бойца")
@app_commands.describe(member="Боец (по умолчанию вы)")
async def warn_list(interaction: discord.Interaction, member: discord.Member = None):
    if not interaction.guild:
        return
    target = member or interaction.user
    warns = await asyncio.to_thread(db_get_warns, interaction.guild.id, target.id)
    max_warns = await asyncio.to_thread(db_get_max_warns, interaction.guild.id)

    if not warns:
        await interaction.response.send_message(f"✅ У {target.mention} нет предупреждений.", ephemeral=True)
        return

    embed = discord.Embed(title=f"📋 Список варнов | {target.display_name} (`{len(warns)}/{max_warns}`)", color=discord.Color.yellow())
    for w_id, mod_id, reason, date_str in warns:
        embed.add_field(name=f"Варн #{w_id} от <@{mod_id}> ({date_str})", value=f"Причина: *{reason}*", inline=False)

    await interaction.response.send_message(embed=embed, ephemeral=True)

@warn_group.command(name="clear", description="Очистить все предупреждения бойца")
@app_commands.checks.has_permissions(administrator=True)
@app_commands.describe(member="Боец")
async def warn_clear(interaction: discord.Interaction, member: discord.Member):
    if not interaction.guild:
        return
    await asyncio.to_thread(db_clear_warns, interaction.guild.id, member.id)
    await interaction.response.send_message(f"🧹 Все предупреждения бойца {member.mention} успешно очищены.", ephemeral=True)

@warn_group.command(name="set_limit", description="Установить максимальное количество варнов до бана")
@app_commands.checks.has_permissions(administrator=True)
@app_commands.describe(max_warns="Лимит предупреждений (по умолчанию 3)")
async def warn_set_limit(interaction: discord.Interaction, max_warns: int):
    if not interaction.guild:
        return
    if max_warns <= 0:
        await interaction.response.send_message("❌ Лимит должен быть больше 0!", ephemeral=True)
        return
    await asyncio.to_thread(db_set_max_warns, interaction.guild.id, max_warns)
    await interaction.response.send_message(f"⚙️ Новый лимит варнов для бана установлен: `{max_warns}`", ephemeral=True)

bot.tree.add_command(warn_group)

# --- МОНИТОРИНГ ВАЙПА И КАЛЬКУЛЯТОРЫ ---

@tasks.loop(seconds=30)
async def check_wipe_time():
    global wipe_data
    if not wipe_data:
        return

    now = datetime.now()
    wipe_dt = wipe_data.get("datetime")
    if not wipe_dt:
        return

    if not wipe_data.get("ping_15m_sent") and (wipe_dt - now) <= timedelta(minutes=15) and now < wipe_dt:
        wipe_data["ping_15m_sent"] = True
        channel = bot.get_channel(wipe_data["channel_id"])
        
        if channel and isinstance(channel, discord.TextChannel):
            guild_id = wipe_data.get("guild_id")
            config = await asyncio.to_thread(db_get_guild_config, guild_id) if guild_id else {}
            clan_role_id = config.get("clan_role_id")
            
            role_mention = f"<@&{clan_role_id}>" if clan_role_id else "@everyone"
            await channel.send(f"🚨 {role_mention} **ДО ВАЙПА ОСТАЛОСЬ 15 МИНУТ! ЗАХОДИТЕ В ГОЛОС!**")
            
            try:
                msg = await channel.fetch_message(wipe_data["message_id"])
                embed = msg.embeds[0]
                embed.set_field_at(3, name="Статус", value="🔴 ПОДГОТОВКА К СТАРТУ", inline=True)
                await msg.edit(embed=embed)
            except Exception:
                pass

    if not wipe_data.get("wipe_started_sent") and now >= wipe_dt:
        wipe_data["wipe_started_sent"] = True
        
        channel = bot.get_channel(wipe_data["channel_id"])
        if channel and isinstance(channel, discord.TextChannel):
            try:
                msg = await channel.fetch_message(wipe_data["message_id"])
                embed = msg.embeds[0]
                embed.set_field_at(3, name="Статус", value="🟢 ВАЙП СТАРТОВАЛ!", inline=True)
                await msg.edit(embed=embed)
            except Exception:
                pass

        guild_id = wipe_data.get("guild_id")
        if guild_id:
            config = await asyncio.to_thread(db_get_guild_config, guild_id)
            leader_channel_id = config.get("leader_channel_id")
            if leader_channel_id:
                leader_channel = bot.get_channel(leader_channel_id)
                if leader_channel and isinstance(leader_channel, discord.TextChannel):
                    final_embed = await build_report_embed(is_final=True)
                    await leader_channel.send(content="🔔 **ВАЙП НАЧАЛСЯ! Итоговая сводка по готовности состава:**", embed=final_embed)

@bot.tree.command(name="craft", description="Калькулятор ресурсов для крафта предметов в Rust")
@app_commands.choices(item=[
    app_commands.Choice(name="C4 (Взрывчатка)", value="c4"),
    app_commands.Choice(name="Ракета", value="rocket"),
    app_commands.Choice(name="Патрон 5.56 (Разрывной)", value="explosive_ammo")
])
@app_commands.describe(amount="Количество предметов")
async def craft_calc(interaction: discord.Interaction, item: str, amount: int = 1):
    if amount <= 0:
        await interaction.response.send_message("Количество должно быть больше 0!", ephemeral=True)
        return

    res = {}
    if item == "c4":
        explosives = 20 * amount
        res = {
            "Сера (Всего)": (explosives * 10) + (explosives * 50 * 2),
            "Уголь": explosives * 50 * 3,
            "Металлические фрагменты": explosives * 10,
            "Топливо низкого качества": explosives * 10,
            "Ткань": 5 * amount,
            "Микросхемы (Tech Trash)": 2 * amount
        }
        title = f"💣 Расчет ресурсов для крафта: {amount}x C4"

    elif item == "rocket":
        explosives = 10 * amount
        extra_gunpowder = 150 * amount
        total_gp = (explosives * 50) + extra_gunpowder
        res = {
            "Сера (Всего)": (explosives * 10) + (total_gp * 2),
            "Уголь": total_gp * 3,
            "Металлические фрагменты": explosives * 10,
            "Топливо низкого качества": explosives * 10,
            "Металлические трубы": 2 * amount
        }
        title = f"🚀 Расчет ресурсов для крафта: {amount}x Ракета"

    elif item == "explosive_ammo":
        crafts = (amount + 1) // 2
        gp = 10 * crafts
        res = {
            "Сера (Всего)": (gp * 2) + (5 * crafts),
            "Уголь": gp * 3,
            "Металлические фрагменты": 10 * crafts
        }
        title = f"💥 Расчет ресурсов для крафта: {amount}x Разрывных патронов"

    embed = discord.Embed(title=title, color=discord.Color.orange())
    for name, value in res.items():
        embed.add_field(name=name, value=f"`{value:,}`", inline=False)

    await interaction.response.send_message(embed=embed)

@bot.tree.command(name="raid", description="Калькулятор стоимости рейда конструкций")
@app_commands.choices(target=[
    app_commands.Choice(name="Деревянная стена / Дверь", value="wood"),
    app_commands.Choice(name="Каменная стена", value="stone"),
    app_commands.Choice(name="Металлическая стена (Sheet Metal)", value="metal"),
    app_commands.Choice(name="МВК стена (Armored)", value="armored"),
    app_commands.Choice(name="Гаражная дверь", value="garage")
])
async def raid_calc(interaction: discord.Interaction, target: str):
    raid_costs = {
        "wood": "🪵 **Деревянная стена / Дверь:**\n• 1 C4 | 1 Ракета | 18 Разрывных патронов | 2 Бобовые мины",
        "stone": "🪨 **Каменная стена:**\n• 2 C4 | 4 Ракеты | 185 Разрывных патронов | 10 Бобовых мин",
        "metal": "⚙️ **Металлическая стена:**\n• 4 C4 | 8 Ракет | 400 Разрывных патронов",
        "armored": "🛡️ **МВК (Armored) стена:**\n• 8 C4 | 15 Ракет | 799 Разрывных патронов",
        "garage": "🚪 **Гаражная дверь:**\n• 1 C4 + 40 Разрывных | 3 Ракеты | 150 Разрывных патронов | 9 Бобовых мин"
    }

    embed = discord.Embed(
        title="💥 Варианты выноса конструкции",
        description=raid_costs.get(target, "Данные не найдены"),
        color=discord.Color.red()
    )
    await interaction.response.send_message(embed=embed)

# ==============================================================================
# ЗАПУСК
# ==============================================================================

if __name__ == "__main__":
    if not DISCORD_TOKEN:
        print("ОШИБКА: DISCORD_TOKEN не найден в файле .env!")
    else:
        bot.run(DISCORD_TOKEN)