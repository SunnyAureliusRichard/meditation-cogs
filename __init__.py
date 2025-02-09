import discord
from discord.ext import tasks
import sqlite3
import datetime
import pytz
from typing import Optional
import asyncio
import json
import os
from redbot.core import commands
from redbot.core.data_manager import cog_data_path
import logging

log = logging.getLogger("red.meditation")

class MeditationCog(commands.Cog):
    """A cog for tracking daily meditation practices."""

    def __init__(self, bot):
        self.bot = bot
        self.data_dir = f"{cog_data_path(self)}"
        os.makedirs(self.data_dir, exist_ok=True)
        
        self.db_path = os.path.join(self.data_dir, 'meditation.db')
        self.settings_path = os.path.join(self.data_dir, 'settings.json')
        self.settings = self.load_settings()
        self.init_database()
        self.daily_post.start()
        self._last_post_attempt = None  # Track when we last tried to post
        self._post_lock = asyncio.Lock()

    def load_settings(self) -> dict:
        if os.path.exists(self.settings_path):
            with open(self.settings_path, 'r') as f:
                return json.load(f)
        return {
            'channel_id': None,
            'daily_message': "React to this message if you've meditated today",
            'last_post_time': None,
            'was_first_post': False
        }

    def save_settings(self):
        with open(self.settings_path, 'w') as f:
            json.dump(self.settings, f)

    def init_database(self):
        conn = sqlite3.connect(self.db_path)
        c = conn.cursor()
        c.execute('''
            CREATE TABLE IF NOT EXISTS meditation_records (
                user_id INTEGER,
                meditation_date DATE,
                PRIMARY KEY (user_id, meditation_date)
            )
        ''')
        conn.commit()
        conn.close()

    def get_meditation_date(self, timestamp: datetime.datetime) -> datetime.date:
        """Determine which meditation day a timestamp belongs to based on 7:30 AM GMT cutoff"""
        gmt = timestamp.astimezone(pytz.UTC)
        cutoff = gmt.replace(hour=7, minute=30, second=0, microsecond=0)
        if gmt < cutoff:
            return gmt.date() - datetime.timedelta(days=1)
        return gmt.date()

    def get_streak(self, user_id: int, current_date: datetime.date) -> int:
        """Get the current meditation streak for a user"""
        conn = sqlite3.connect(self.db_path)
        c = conn.cursor()
        
        try:
            # Get all dates for this user, ordered by date
            c.execute('''
                SELECT meditation_date 
                FROM meditation_records 
                WHERE user_id = ? 
                ORDER BY meditation_date DESC
            ''', (user_id,))
            
            dates = [datetime.datetime.strptime(row[0], '%Y-%m-%d').date() 
                    for row in c.fetchall()]
            
            if not dates:
                return 0
                
            streak = 1
            expected_date = dates[0]
            
            # Check each consecutive date
            for date in dates[1:]:
                if expected_date - datetime.timedelta(days=1) == date:
                    streak += 1
                    expected_date = date
                else:
                    break
                    
            return streak
            
        finally:
            conn.close()

    def get_all_streaks(self, current_date: datetime.date) -> dict:
        """Get current meditation streaks for all users"""
        conn = sqlite3.connect(self.db_path)
        c = conn.cursor()
        
        try:
            # Get all unique user IDs
            c.execute('SELECT DISTINCT user_id FROM meditation_records')
            user_ids = [row[0] for row in c.fetchall()]
            
            # Calculate streak for each user
            streaks = {}
            for user_id in user_ids:
                streak = self.get_streak(user_id, current_date)
                if streak > 0:
                    streaks[user_id] = streak
                    
            return dict(sorted(streaks.items(), key=lambda x: x[1], reverse=True))
            
        finally:
            conn.close()

    def should_post(self, now: datetime.datetime) -> bool:
        """Determine if we should post the daily message"""
        target_time = now.replace(hour=7, minute=30, second=0, microsecond=0)
        
        # If we've never posted, always post
        if not self.settings['last_post_time']:
            log.info("No last post time found, should post")
            self.settings['was_first_post'] = True
            return True
            
        last_post = datetime.datetime.fromisoformat(self.settings['last_post_time'])
        time_since_last_post = (now - last_post).total_seconds()
        log.info(f"Last post was at {last_post.isoformat()}, {time_since_last_post} seconds ago")
        
        # Special handling for the post after first post
        if self.settings.get('was_first_post'):
            if now >= target_time and last_post < target_time:
                log.info("First post case: past target time and haven't posted since before target")
                self.settings['was_first_post'] = False
                return True
            log.info("First post case: conditions not met")
            return False
            
        # Regular posts: must be at least 23.5 hours since last post
        if time_since_last_post < 23.5 * 60 * 60:
            log.info("Not enough time has passed since last post")
            return False
            
        # If we're past today's target time and haven't posted since before it
        if now >= target_time and last_post < target_time:
            log.info("Past target time and haven't posted since before target")
            return True
            
        # If it's been more than 24 hours since last post
        if time_since_last_post > 24 * 60 * 60:
            log.info("More than 24 hours since last post")
            return True
            
        log.info("No posting conditions met")
        return False

    async def post_daily_message(self):
        """Post the daily meditation message and update settings"""
        try:
            log.info("Attempting to post daily message")
            channel = self.bot.get_channel(int(self.settings['channel_id']))
            if channel:
                log.info(f"Posting in channel {channel.name} ({channel.id})")
                message = await channel.send(self.settings['daily_message'])
                await message.add_reaction("🧘‍♂️")
                await message.add_reaction("🧘‍♀️")
                
                # Update last post time
                self.settings['last_post_time'] = datetime.datetime.now(pytz.UTC).isoformat()
                self.save_settings()
                log.info(f"Successfully posted message {message.id} and updated settings")
            else:
                log.error(f"Could not find channel with ID {self.settings['channel_id']}")
        except Exception as e:
            log.exception("Error in post_daily_message")

    @tasks.loop(minutes=1)
    async def daily_post(self):
        try:
            log.info("Daily post task started")
            if self._post_lock.locked():
                log.info("Post lock is already held, skipping this run")
                return
                
            async with self._post_lock:
                log.info("Acquired post lock")
                if not self.settings['channel_id']:
                    log.info("No channel ID set, skipping post")
                    return

                now = datetime.datetime.now(pytz.UTC)
                log.info(f"Current time: {now.isoformat()}")
                
                # Rate limit check
                if self._last_post_attempt:
                    time_since_last = (now - self._last_post_attempt).total_seconds()
                    log.info(f"Time since last post attempt: {time_since_last} seconds")
                    if time_since_last < 300:
                        log.info("Within rate limit period, skipping")
                        return
                
                self._last_post_attempt = now
                target_time = now.replace(hour=7, minute=30, second=0, microsecond=0)
                log.info(f"Target time: {target_time.isoformat()}")
                
                # Normal case: it's exactly 7:30 AM
                if now.hour == target_time.hour and now.minute == target_time.minute:
                    log.info("It's exactly posting time")
                    await self.post_daily_message()
                # Recovery case: check if we missed posting
                elif self.should_post(now):
                    log.info("Recovery posting needed")
                    await self.post_daily_message()
                else:
                    log.info("No post needed at this time")
        except Exception as e:
            log.exception("Error in daily_post task")

    @commands.group(name="med")
    async def med(self, ctx):
        if ctx.invoked_subcommand is None:
            await ctx.send("Invalid meditation command. Use .help med for available commands.")

    @med.command(name="dailymessage")
    @commands.has_permissions(administrator=True)
    async def set_daily_message(self, ctx, *, message: str):
        self.settings['daily_message'] = message
        self.save_settings()
        await ctx.send(f"Daily message updated to: {message}")

    @med.command(name="setchannel")
    @commands.has_permissions(administrator=True)
    async def set_channel(self, ctx):
        self.settings['channel_id'] = str(ctx.channel.id)
        self.save_settings()
        await ctx.send(f"Meditation channel set to: {ctx.channel.name}")

    @med.command(name="me")
    async def show_streak(self, ctx):
        today = self.get_meditation_date(datetime.datetime.now(pytz.UTC))
        streak = self.get_streak(ctx.author.id, today)
        await ctx.send(f"You have meditated for {streak} consecutive days!")

    @med.command(name="leaderboard")
    async def show_leaderboard(self, ctx):
        today = self.get_meditation_date(datetime.datetime.now(pytz.UTC))
        top_users = self.get_all_streaks(today)
        
        if not top_users:
            await ctx.send("No meditation streaks to display!")
            return
        
        embed = discord.Embed(
            title="Meditation Leaderboard",
            color=discord.Color.blue()
        )
        
        description = []
        for i, (user_id, streak) in enumerate(top_users.items(), 1):
            user = ctx.guild.get_member(user_id)
            if user:
                description.append(f"{i}. {user.mention} • {streak} days")
        
        embed.description = "\n".join(description)
        await ctx.send(embed=embed)

    @commands.Cog.listener()
    async def on_raw_reaction_add(self, payload):
        if payload.user_id == self.bot.user.id:
            return

        channel = self.bot.get_channel(payload.channel_id)
        message = await channel.fetch_message(payload.message_id)
        
        # Check if reaction is on a meditation message
        if message.author != self.bot.user or message.content != self.settings['daily_message']:
            return

        # Check if reaction is one of our meditation emojis
        if str(payload.emoji) not in ["🧘‍♂️", "🧘‍♀️"]:
            return

        # Check if message is more than 2 days old
        message_age = datetime.datetime.now(pytz.UTC) - message.created_at
        if message_age.days > 2:
            await message.remove_reaction(payload.emoji, payload.member)
            return

        # Record meditation
        meditation_date = self.get_meditation_date(message.created_at)
        conn = sqlite3.connect(self.db_path)
        c = conn.cursor()
        try:
            c.execute('''
                INSERT OR REPLACE INTO meditation_records (user_id, meditation_date)
                VALUES (?, ?)
            ''', (payload.user_id, meditation_date))
            conn.commit()
        finally:
            conn.close()

    @commands.Cog.listener()
    async def on_raw_reaction_remove(self, payload):
        channel = self.bot.get_channel(payload.channel_id)
        message = await channel.fetch_message(payload.message_id)
        
        # Check if reaction is from a meditation message
        if message.author != self.bot.user or message.content != self.settings['daily_message']:
            return

        # Check if the removed reaction was a meditation emoji
        if str(payload.emoji) not in ["🧘‍♂️", "🧘‍♀️"]:
            return

        # Check if user still has any meditation reactions on the message
        user = self.bot.get_user(payload.user_id)
        for reaction in message.reactions:
            if str(reaction.emoji) in ["🧘‍♂️", "🧘‍♀️"]:
                async for reaction_user in reaction.users():
                    if reaction_user.id == payload.user_id:
                        return

        # If we get here, user has no more meditation reactions, so remove their record
        meditation_date = self.get_meditation_date(message.created_at)
        conn = sqlite3.connect(self.db_path)
        c = conn.cursor()
        try:
            c.execute('''
                DELETE FROM meditation_records 
                WHERE user_id = ? AND meditation_date = ?
            ''', (payload.user_id, meditation_date))
            conn.commit()
        finally:
            conn.close()

async def setup(bot):
    await bot.add_cog(MeditationCog(bot))