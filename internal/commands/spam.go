package commands

import (
	"context"
	"fmt"
	"math/rand"
	"strconv"
	"strings"
	"sync"
	"time"

	"selfbot/internal/interfaces"

	"github.com/LightningDev1/discordgo"
	log "github.com/sirupsen/logrus"
)

// SpamCommand implements the spam command with clean Go patterns
type SpamCommand struct {
	bot         interfaces.BotInterface
	mu          sync.RWMutex
	isSpamming  map[string]bool  // Track spam status per channel
	cancelFuncs map[string]context.CancelFunc // Cancel functions for stopping spam
}

// NewSpamCommand creates a new spam command
func NewSpamCommand(bot interfaces.BotInterface) *SpamCommand {
	return &SpamCommand{
		bot:         bot,
		isSpamming:  make(map[string]bool),
		cancelFuncs: make(map[string]context.CancelFunc),
	}
}

func (c *SpamCommand) Name() string        { return "spam" }
func (c *SpamCommand) Aliases() []string   { return []string{"s"} }
func (c *SpamCommand) Description() string { return "Spam messages with various options" }

// SpamOptions contains parsed spam command options
type SpamOptions struct {
	Amount      int
	Messages    []string
	UseMax      bool
	UseDelete   bool
	UseRandom   bool
	Delay       time.Duration
	ChannelID   string
}

// Execute executes the spam command with clean Go logic
func (c *SpamCommand) Execute(s *discordgo.Session, m *discordgo.MessageCreate, args []string) error {
	if len(args) < 2 {
		return c.sendUsage(s, m.ChannelID)
	}

	// Parse amount
	amount, err := strconv.Atoi(args[0])
	if err != nil || amount < 1 || amount > 1000 {
		return c.sendError(s, m.ChannelID, "Amount must be a number between 1 and 1000")
	}

	// Parse options
	opts, err := c.parseSpamOptions(amount, args[1:])
	if err != nil {
		return c.sendError(s, m.ChannelID, err.Error())
	}

	// Set default channel
	if opts.ChannelID == "" {
		opts.ChannelID = m.ChannelID
	}

	// Validate messages
	if len(opts.Messages) == 0 {
		return c.sendError(s, m.ChannelID, "No message content provided")
	}

	// Check if already spamming in this channel
	c.mu.Lock()
	if c.isSpamming[opts.ChannelID] {
		c.mu.Unlock()
		return c.sendError(s, m.ChannelID, "Already spamming in this channel. Use `stop` or `sspam` to stop.")
	}
	c.isSpamming[opts.ChannelID] = true
	c.mu.Unlock()

	// Start spamming in goroutine
	ctx, cancel := context.WithCancel(context.Background())
	c.mu.Lock()
	c.cancelFuncs[opts.ChannelID] = cancel
	c.mu.Unlock()

	go c.executeSpam(ctx, s, opts)

	return nil
}

// parseSpamOptions parses command arguments into SpamOptions
func (c *SpamCommand) parseSpamOptions(amount int, args []string) (*SpamOptions, error) {
	opts := &SpamOptions{
		Amount:   amount,
		Messages: []string{},
		Delay:    0,
	}

	var currentMessage strings.Builder
	i := 0

	for i < len(args) {
		arg := args[i]

		switch arg {
		case "-max":
			opts.UseMax = true
		case "-delete":
			opts.UseDelete = true
		case "-r", "-random":
			opts.UseRandom = true
		case "-d", "-delay":
			if i+1 >= len(args) {
				return nil, fmt.Errorf("missing delay value after %s", arg)
			}
			i++
			delaySeconds, err := strconv.Atoi(args[i])
			if err != nil || delaySeconds < 0 || delaySeconds > 3600 {
				return nil, fmt.Errorf("delay must be a number between 0 and 3600 seconds")
			}
			opts.Delay = time.Duration(delaySeconds) * time.Second
		case "-c", "-channel":
			if i+1 >= len(args) {
				return nil, fmt.Errorf("missing channel ID after %s", arg)
			}
			i++
			opts.ChannelID = args[i]
		case "-multi":
			// Multi-message mode - collect quoted messages
			if currentMessage.Len() > 0 {
				opts.Messages = append(opts.Messages, strings.TrimSpace(currentMessage.String()))
				currentMessage.Reset()
			}
			// Continue to collect remaining messages as separate entries
		default:
			// Regular message content
			if currentMessage.Len() > 0 {
				currentMessage.WriteString(" ")
			}
			currentMessage.WriteString(arg)
		}
		i++
	}

	// Add final message if any
	if currentMessage.Len() > 0 {
		opts.Messages = append(opts.Messages, strings.TrimSpace(currentMessage.String()))
	}

	// If no messages collected, something went wrong
	if len(opts.Messages) == 0 {
		return nil, fmt.Errorf("no message content provided")
	}

	// Apply max length if requested
	if opts.UseMax {
		opts.Messages = c.applyMaxLength(opts.Messages)
	}

	return opts, nil
}

// applyMaxLength applies maximum message length to messages
func (c *SpamCommand) applyMaxLength(messages []string) []string {
	const maxLength = 2000 // Discord message limit
	var result []string

	for _, msg := range messages {
		if len(msg) == 0 {
			continue
		}
		
		// Calculate how many times we can repeat the message
		repeatCount := maxLength / (len(msg) + 1) // +1 for space
		if repeatCount < 1 {
			// Message is already too long, truncate it
			result = append(result, msg[:maxLength])
		} else {
			// Repeat the message up to the limit
			repeated := strings.Repeat(msg+" ", repeatCount)
			result = append(result, strings.TrimSpace(repeated))
		}
	}

	return result
}

// executeSpam performs the actual spamming
func (c *SpamCommand) executeSpam(ctx context.Context, s *discordgo.Session, opts *SpamOptions) {
	defer func() {
		// Clean up when done
		c.mu.Lock()
		delete(c.isSpamming, opts.ChannelID)
		delete(c.cancelFuncs, opts.ChannelID)
		c.mu.Unlock()
	}()

	log.Infof("Starting spam: %d messages to channel %s", opts.Amount, opts.ChannelID)

	for i := 0; i < opts.Amount; i++ {
		select {
		case <-ctx.Done():
			log.Info("Spam cancelled")
			return
		default:
		}

		// Select message
		var content string
		if opts.UseRandom && len(opts.Messages) > 1 {
			content = opts.Messages[rand.Intn(len(opts.Messages))]
		} else {
			content = opts.Messages[i%len(opts.Messages)]
		}

		// Send message
		msg, err := s.ChannelMessageSend(opts.ChannelID, content)
		if err != nil {
			log.Errorf("Failed to send spam message: %v", err)
			// Check for rate limit
			if strings.Contains(err.Error(), "429") {
				log.Warn("Rate limited, waiting 5 seconds...")
				select {
				case <-ctx.Done():
					return
				case <-time.After(5 * time.Second):
					continue
				}
			}
			continue
		}

		// Delete message if requested
		if opts.UseDelete && msg != nil {
			go func(msgID string) {
				time.Sleep(100 * time.Millisecond) // Brief delay before deletion
				if err := s.ChannelMessageDelete(opts.ChannelID, msgID); err != nil {
					log.Debugf("Failed to delete message: %v", err)
				}
			}(msg.ID)
		}

		// Apply delay if specified (except for last message)
		if opts.Delay > 0 && i < opts.Amount-1 {
			select {
			case <-ctx.Done():
				return
			case <-time.After(opts.Delay):
			}
		}
	}

	log.Infof("Spam completed: %d messages sent to channel %s", opts.Amount, opts.ChannelID)
}

// sendUsage sends command usage information
func (c *SpamCommand) sendUsage(s *discordgo.Session, channelID string) error {
	usage := `**Spam Command Usage:**
\` + "`" + `.spam <amount> <message> [flags]\` + "`" + `

**Flags:**
\` + "`" + `-max\` + "`" + ` - Maximize message length by repeating
\` + "`" + `-delete\` + "`" + ` - Send and immediately delete messages  
\` + "`" + `-r/-random\` + "`" + ` - Random message selection (with -multi)
\` + "`" + `-d <seconds>\` + "`" + ` - Delay between messages (0-3600)
\` + "`" + `-c <channel_id>\` + "`" + ` - Send to specific channel
\` + "`" + `-multi\` + "`" + ` - Multiple message mode

**Examples:**
\` + "`" + `.spam 5 Hello world\` + "`" + ` - Basic spam
\` + "`" + `.spam 10 Message -max -delete\` + "`" + ` - Max length with delete
\` + "`" + `.spam 5 -multi Hello World Test\` + "`" + ` - Rotate between messages
\` + "`" + `.spam 3 -multi -r Hi Hey Hello\` + "`" + ` - Random multi-messages
\` + "`" + `.spam 10 Test -d 2\` + "`" + ` - 2 second delay between messages`

	return c.sendTempMessage(s, channelID, usage)
}

// sendError sends an error message
func (c *SpamCommand) sendError(s *discordgo.Session, channelID, message string) error {
	return c.sendTempMessage(s, channelID, "❌ "+message)
}

// sendTempMessage sends a temporary message with auto-delete
func (c *SpamCommand) sendTempMessage(s *discordgo.Session, channelID, content string) error {
	msg, err := s.ChannelMessageSend(channelID, content)
	if err != nil {
		return err
	}

	// Auto-delete after delay
	if c.bot.GetConfig().AutoDelete.Enabled {
		go func() {
			time.Sleep(time.Duration(c.bot.GetConfig().AutoDelete.Delay) * time.Second)
			s.ChannelMessageDelete(channelID, msg.ID)
		}()
	}

	return nil
}

// StopSpamCommand implements the stop spam command
type StopSpamCommand struct {
	spamCmd *SpamCommand
}

// NewStopSpamCommand creates a new stop spam command
func NewStopSpamCommand(spamCmd *SpamCommand) *StopSpamCommand {
	return &StopSpamCommand{spamCmd: spamCmd}
}

func (c *StopSpamCommand) Name() string        { return "sspam" }
func (c *StopSpamCommand) Aliases() []string   { return []string{"stopspam", "ss"} }
func (c *StopSpamCommand) Description() string { return "Stop ongoing spam in channel" }

// Execute stops spam in the current channel
func (c *StopSpamCommand) Execute(s *discordgo.Session, m *discordgo.MessageCreate, args []string) error {
	channelID := m.ChannelID
	
	// Allow specifying channel ID
	if len(args) > 0 {
		channelID = args[0]
	}

	c.spamCmd.mu.Lock()
	defer c.spamCmd.mu.Unlock()

	if cancel, exists := c.spamCmd.cancelFuncs[channelID]; exists {
		cancel()
		delete(c.spamCmd.isSpamming, channelID)
		delete(c.spamCmd.cancelFuncs, channelID)
		
		return c.spamCmd.sendTempMessage(s, m.ChannelID, "✅ Spam stopped in target channel")
	}

	return c.spamCmd.sendTempMessage(s, m.ChannelID, "❌ No active spam found in target channel")
}