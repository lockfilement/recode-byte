from discord.ext import commands
import aiohttp

class FakePings(commands.Cog):
    @commands.command(aliases=["fp", "setunread", "fping"])
    async def fakepings(self, ctx, channel_id: int = None, message_id: int = None, mention_count: int = 1):
        """
        Set a message as unread with a custom mention count (fake pings).
        If only channel_id is given, a random message is used.
        """
        import random
        try:
            await ctx.message.delete()
        except:
            pass

        if channel_id is None:
            # Only send usage if channel_id is missing, as this is a user error
            await ctx.send("Usage: .fakepings <channel_id> [message_id or mention_count] [mention_count]", delete_after=10)
            return

        # If only channel_id and mention_count are provided, try to parse message_id/mention_count
        if message_id is not None and isinstance(message_id, int) and mention_count == 1:
            # User may have provided: .fakepings <channel_id> <mention_count>
            # Try to treat message_id as mention_count if it's small
            if message_id < 10000:  # unlikely to be a message id
                mention_count = message_id
                message_id = None

        if message_id is None:
            # Fetch a random message from the channel
            headers = self.get_auth_headers(ctx)
            url = f"https://discord.com/api/v9/channels/{channel_id}/messages?limit=50"
            try:
                async with aiohttp.ClientSession() as session:
                    async with session.get(url, headers=headers) as resp:
                        if resp.status != 200:
                            # Only send error if fetching messages fails
                            return
                        messages = await resp.json()
                        if not messages:
                            return
                        message = random.choice(messages)
                        message_id = message["id"]
            except Exception:
                return

        url = f"https://discord.com/api/v9/channels/{channel_id}/messages/{message_id}/ack"
        data = {
            "manual": True,
            "mention_count": mention_count
        }
        headers = self.get_auth_headers(ctx)

        try:
            async with aiohttp.ClientSession() as session:
                async with session.post(url, json=data, headers=headers) as response:
                    pass  # Do not send any feedback message after execution
        except Exception:
            pass

    def get_auth_headers(self, ctx):
        token = ctx.bot.http.token
        headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) discord/1.0.9191 Chrome/134.0.6998.179 Electron/35.1.5 Safari/537.36",
            "Accept": "*/*",
            "Accept-Language": "en-US,en;q=0.9",
            "Content-Type": "application/json",
            "Authorization": token,
        }
        if hasattr(ctx.bot.http, "headers") and hasattr(ctx.bot.http.headers, "encoded_super_properties"):
            headers["X-Super-Properties"] = ctx.bot.http.headers.encoded_super_properties
        return headers

async def setup(bot):
    await bot.add_cog(FakePings())
