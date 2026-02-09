from discord.ext import commands
import discord
import asyncio
import random

class Edate(commands.Cog):
    def __init__(self, bot):
        self.bot = bot
        self.edate_messages = {
            1: "Can we honestly e date? You're so beautiful. You always make me laugh, you always make me smile. You literally make me want to become a better person... I really enjoy every moment we spend together. My time has no value unless its spent with you. I tell everyone of my irls how awesome you are. Thank you for being you. Whenever you need someone to be there for you, know that i'll always be right there by your side. I love you so much. I don't think you ever realize how amazing you are sometimes. Life isn't as fun when you're not around. You are truly stunning. I want you to be my soulmate. I love the way you smile, your eyes are absolutely gorgeous. If I had a star for everytime you crossed my mind i could make the entire galaxy. Your personality is as pretty as you are and thats saying something. I love you, please date me. I am not even calling it e dating anymore because I know we will meet soon enough heart OK I ADMIT IT I LOVE YOU OK i hecking love you and it breaks my heart when i see you play with someone else or anyone commenting in your profile i just want to be your girlfriend and put a heart in my profile linking to your profile and have a walltext of you commenting cute things i want to play video games talk in discord all night and watch a movie together but you just seem so uninsterested in me it hecking kills me and i cant take it anymore i want to remove you but i care too much about you so please i'm begging you to eaither love me back or remove me and never contact me again it hurts so much to say this because i need you by my side but if you dont love me then i want you to leave because seeing your icon in my friendlist would kill me everyday of my pathetic life. <3",
            2: "Oh my god, please edate me already. I've been on this discord for the past 4 months obsessing over the 5 different profile pictures you've used over time and you make my cock so fucking hard every time you insult me and others. You know you want me, I already know a lot about you so we wouldnt'd have to go on a lot of dates to get to the bedroom. I won't hesitate to propose either, I am fully prepared to spend the rest of my life with you, honey. Please, please just tell me you love me already. I can't wait to hear your lovely voice say those three words, or hear your voice in general. You've never wanted to talk to me, and I understand, we can go slow if you need to. I don't need to move in with you right away, I still have 4 months until my parents being me to court for shilling them. I'll sell all of my stuffed bears and replica hats to help you pay for rent. I'll see you later today! OK I ADMIT IT I LOVE YOU OK I fucking love you and it breaks my hear when I see you play with someone else or anyone commenting in your profile I just want to be your boyfriend and put a heart in my profile linking to your profile and have a walltext of you commenting cute things. I want to play video games, talk in discord all night and watch a movie together but you just seem so uninterested in me it fucking kills me and I cant take it anymore I want to remove you but I care too much about you so please I'm begging you to either love me back or remove me and NEVER contact me again it hurts so much to say this because I need you by my side but if you don't love me then I want you to leave because seeing your icon in my friend list would kill me everyday of my pathetic life."
        }
        self.use_hashtag = False
        self.random_hashtag = False

    @commands.command(aliases=['ed'])
    async def edate(self, ctx, *args):
        """Send a proposal to someone
        
        .edate [#/rh/l] [1/2] <user>
        # - Add hashtags
        rh - Random hashtags
        l - Send message laddered (one sentence at a time)
        1/2 - Choose edate message variant (default: 1)"""
        try:
            await ctx.message.delete()
        except (discord.HTTPException, discord.Forbidden):
            pass
        
        # Parse arguments
        target = None
        self.use_hashtag = False
        self.random_hashtag = False
        laddered = False
        message_choice = 1  # Default to first message
        
        args = list(args)
        while args:
            arg = args.pop(0)
            if arg == '#':
                self.use_hashtag = True
            elif arg.lower() == 'rh':
                self.random_hashtag = True
            elif arg.lower() == 'l':
                laddered = True
            elif arg in ['1', '2']:
                message_choice = int(arg)
            else:
                # Last argument should be the user
                try:
                    target = await commands.UserConverter().convert(ctx, arg)
                except:
                    try:
                        target = await commands.MemberConverter().convert(ctx, arg)
                    except:
                        await ctx.send("Invalid user specified", delete_after=self.bot.config_manager.auto_delete.delay if self.bot.config_manager.auto_delete.enabled else None)
                        return
                break

        if not target:
            await ctx.send("You need to specify a user", delete_after=self.bot.config_manager.auto_delete.delay if self.bot.config_manager.auto_delete.enabled else None)
            return

        # Don't allow targeting self or bots
        if target.id == ctx.author.id or target.bot:
            await ctx.send("You can't e-date yourself or bots!", 
                delete_after=self.bot.config_manager.auto_delete.delay if self.bot.config_manager.auto_delete.enabled else None)
            return

        # Get the selected edate message
        edate_message = self.edate_messages.get(message_choice, self.edate_messages[1])

        if laddered:
            # Split into sentences and clean up
            sentences = [s.strip() for s in edate_message.split('.') if s.strip()]
            
            for sentence in sentences:
                try:
                    # Make new random hashtag decision for each sentence
                    use_hashtag = self.use_hashtag or (self.random_hashtag and random.choice([True, False]))
                    # Add period back and mention user for each part
                    message = f"{'# ' if use_hashtag else ''}<@{target.id}> {sentence}."
                    await ctx.send(message)
                    await asyncio.sleep(0.5)  # 0.5 second delay between sentences
                except discord.HTTPException as e:
                    if e.status == 429:  # Rate limited
                        await asyncio.sleep(5)
                        continue
                    break
        else:
            # One random hashtag decision for the full message
            use_hashtag = self.use_hashtag or (self.random_hashtag and random.choice([True, False]))
            # Send the full e-dating message
            message = f"{'# ' if use_hashtag else ''}<@{target.id}>, {edate_message}"
            await ctx.send(message)

async def setup(bot):
    await bot.add_cog(Edate(bot))
