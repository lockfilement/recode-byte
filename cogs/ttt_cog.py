import discord
from discord.ext import commands
import asyncio
import random
import logging
import re

logger = logging.getLogger(__name__)

class TicTacToe(commands.Cog):
    def __init__(self, bot):
        self.bot = bot
        self.enabled_guilds = set()
        self.board_cache = {}
        
        # Enhanced winning combinations with weights
        self.WINNING_COMBOS = [
            # Horizontal rows
            [(0,0), (0,1), (0,2)],
            [(1,0), (1,1), (1,2)],
            [(2,0), (2,1), (2,2)],
            # Vertical columns
            [(0,0), (1,0), (2,0)],
            [(0,1), (1,1), (2,1)],
            [(0,2), (1,2), (2,2)],
            # Diagonals
            [(0,0), (1,1), (2,2)],
            [(0,2), (1,1), (2,0)]
        ]

    def parse_board(self, components):
        """Convert Discord components into a 2D board array"""
        board = [[None for _ in range(3)] for _ in range(3)]
        
        for row_idx, row in enumerate(components):
            for col_idx, button in enumerate(row.children):  # Use .children instead of ['components']
                if button.label == 'X':
                    board[row_idx][col_idx] = 'X'
                elif button.label == 'O':
                    board[row_idx][col_idx] = 'O'
        return board

    def get_best_move(self, board, player):
        """Enhanced strategy combining minimax with controlled randomness"""
        opponent = 'O' if player == 'X' else 'X'
        empty_spots = [(r,c) for r in range(3) for c in range(3) if board[r][c] is None]
        
        if not empty_spots:
            return None

        # Keep the immediate win/block logic
        winning_move = self.find_winning_move(board, player)
        if winning_move:
            return winning_move

        blocking_move = self.find_winning_move(board, opponent)
        if blocking_move:
            return blocking_move

        # Store moves with their scores
        move_scores = []
        
        for r, c in empty_spots:
            board[r][c] = player
            score = self.minimax(board, 0, float('-inf'), float('inf'), False, player)
            board[r][c] = None
            move_scores.append(((r, c), score))
        
        # Find the maximum score
        max_score = max(score for _, score in move_scores)
        
        # Get all moves that have the maximum score
        best_moves = [move for move, score in move_scores if score == max_score]
        
        # Randomly choose among the best moves
        return random.choice(best_moves)

    def find_winning_move(self, board, player):
        """Find an immediate winning move if available"""
        for combo in self.WINNING_COMBOS:
            marks = [board[r][c] for r,c in combo]
            if marks.count(player) == 2 and marks.count(None) == 1:
                return next((pos for pos in combo if board[pos[0]][pos[1]] is None))
        return None
    
    def minimax(self, board, depth, alpha, beta, is_maximizing, player):
        """
        Minimax algorithm with alpha-beta pruning
        """
        opponent = 'O' if player == 'X' else 'X'
        
        # Check terminal states
        winner = self.check_winner(board)
        if winner == player:
            return 10 - depth
        elif winner == opponent:
            return depth - 10
        elif not any(board[r][c] is None for r in range(3) for c in range(3)):
            return 0
            
        if is_maximizing:
            max_eval = float('-inf')
            for r in range(3):
                for c in range(3):
                    if board[r][c] is None:
                        board[r][c] = player
                        eval = self.minimax(board, depth + 1, alpha, beta, False, player)
                        board[r][c] = None
                        max_eval = max(max_eval, eval)
                        alpha = max(alpha, eval)
                        if beta <= alpha:
                            break
            return max_eval
        else:
            min_eval = float('inf')
            for r in range(3):
                for c in range(3):
                    if board[r][c] is None:
                        board[r][c] = opponent
                        eval = self.minimax(board, depth + 1, alpha, beta, True, player)
                        board[r][c] = None
                        min_eval = min(min_eval, eval)
                        beta = min(beta, eval)
                        if beta <= alpha:
                            break
            return min_eval
    
    def check_winner(self, board):
        """
        Check if there's a winner on the board
        """
        for combo in self.WINNING_COMBOS:
            marks = [board[r][c] for r,c in combo]
            if len(set(marks)) == 1 and marks[0] is not None:
                return marks[0]
        return None

    def get_custom_id(self, message, row, col):
        """Get the button custom ID for given position"""
        try:
            return message.components[row].children[col].custom_id
        except (IndexError, AttributeError):
            return None

    @commands.command(aliases=['tt'])
    async def tictactoe(self, ctx, *args):
        """TicTacToe ai for bleed
        tictactoe [guild_id] [on|off]
        or tictactoe [on|off] to use current guild"""
        try:
            await ctx.message.delete()
        except:
            pass
    
        # Parse arguments
        guild_id = None
        setting = None
        
        # if just on/off or nothing provided, use current guild to show status
        if not args:
            guild_id = ctx.guild.id
        elif len(args) == 1:
            if args[0].lower() in ['on', 'off', 'enable', 'disable']:
                setting = args[0]
                guild_id = ctx.guild.id
            else:
                # user provided guild ID only
                try:
                    guild_id = int(args[0])
                except ValueError:
                    await ctx.send("Invalid guild ID format")
                    return
        elif len(args) == 2:
            try:
                guild_id = int(args[0])
                setting = args[1]
            except ValueError:
                await ctx.send("Invalid guild ID format")
                return
    
        if setting and setting.lower() in ['on', 'enable']:
            self.enabled_guilds.add(guild_id)
        elif setting and setting.lower() in ['off', 'disable']:
            self.enabled_guilds.discard(guild_id)
        else:
            status = "enabled" if guild_id in self.enabled_guilds else "disabled"
            await ctx.send(f"TicTacToe auto-player is {status} for guild {guild_id}")

    @commands.Cog.listener()
    async def on_message(self, message):
        """Handler for message events"""
        if message.author.id != 593921296224747521:  # Bleed bot ID
            return
            
        if not message.guild or message.guild.id not in self.enabled_guilds:
            return

        # Check if this is a game message with a board
        if not message.components or len(message.components) != 3:
            return

        # Only look for game start pattern
        game_start = re.search(r'\*\*(.+?)\*\*\s+vs\s+\*\*(.+?)\*\*', message.content)
        if not game_start:
            return

        our_name = self.bot.user.name.lower()
        player1, player2 = game_start.groups()
        
        if our_name not in [player1.lower(), player2.lower()]:
            return

        our_symbol = 'X' if player1.lower() == our_name else 'O'
        self.board_cache[message.id] = our_symbol

        # Only make a move if we're X (first player)
        if our_symbol == 'X':
            await asyncio.sleep(random.uniform(0.5, 1.5))
            await self.make_move_with_retry(message, our_symbol)

    @commands.Cog.listener()
    async def on_message_edit(self, before, after):
        """Handler for message edit events"""
        if before.author.id != 593921296224747521:  # Bleed bot ID
            return
            
        if not after.guild or after.guild.id not in self.enabled_guilds:
            return
    
        # Check if this is a game message with a board
        if not after.components or len(after.components) != 3:
            return
    
        # Check for game end first
        if any(phrase in after.content for phrase in ["Nobody won! It's a tie.", "won!"]):
            self.board_cache.pop(after.id, None)
            return
    
        # Verify we're actually playing in this game
        game_players = re.search(r'\*\*(.+?)\*\*\s+vs\s+\*\*(.+?)\*\*', after.content)
        if not game_players:
            return
    
        our_name = self.bot.user.name.lower()
        player1, player2 = game_players.groups()
        if our_name not in [player1.lower(), player2.lower()]:
            return
    
        # Get our symbol from cache
        our_symbol = self.board_cache.get(after.id)
        if not our_symbol:
            return
    
        # Check whose turn it is
        is_x_turn = "❌" in after.content
        is_our_turn = (our_symbol == 'X' and is_x_turn) or (our_symbol == 'O' and not is_x_turn)
    
        if is_our_turn:
            try:
                await self.make_move_with_retry(after, our_symbol)
            except Exception as e:
                logger.error(f"Failed to make move: {e}")
                self.board_cache.pop(after.id, None)

    async def make_move_with_retry(self, message, our_symbol, max_retries=3):
        """Make a move with exponential backoff retries"""
        board = self.parse_board(message.components)
        move = self.get_best_move(board, our_symbol)
        
        if not move:
            return False

        row, col = move
        
        for attempt in range(max_retries):
            try:
                # Get fresh message data before each attempt
                message = await message.channel.fetch_message(message.id)
                button = message.components[row].children[col]
                
                # Validate button state
                if button.label in ['X', 'O'] or button.disabled:
                    logger.debug("Button already used or disabled")
                    return False
                
                # Increase delay between attempts
                await asyncio.sleep(random.uniform(0.5, 1.5))
                
                # Try clicking with proper interaction handling
                try:
                    await button.click()
                    return True
                except discord.NotFound:
                    logger.debug("Game no longer exists")
                    self.board_cache.pop(message.id, None)
                    return False

            except discord.HTTPException as e:
                if attempt < max_retries - 1:
                    logger.warning(f"Move attempt {attempt + 1} failed: {e}")
                    # Exponential backoff
                    await asyncio.sleep(random.uniform(0.5, 1.5))
                    continue
                else:
                    logger.error(f"All move attempts failed: {e}")
                    self.board_cache.pop(message.id, None)
                    return False
            
            except Exception as e:
                logger.error(f"Unexpected error making move: {e}")
                self.board_cache.pop(message.id, None)
                return False

        return False

    async def cog_unload(self):
        """Cleanup when cog is unloaded"""
        self.board_cache.clear()

async def setup(bot):
    await bot.add_cog(TicTacToe(bot))
