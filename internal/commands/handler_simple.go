package commands

import (
	"fmt"
	"strings"
	"time"

	"selfbot/internal/config"
	"selfbot/internal/interfaces"

	"github.com/LightningDev1/discordgo"
	log "github.com/sirupsen/logrus"
)

// SimpleHandler processes commands with clean Go patterns
type SimpleHandler struct {
	bot      interfaces.BotInterface
	config   *config.Config
	commands map[string]SimpleCommand
}

// SimpleCommand interface for all commands
type SimpleCommand interface {
	Execute(s *discordgo.Session, m *discordgo.MessageCreate, args []string) error
	Name() string
	Aliases() []string
	Description() string
}

// NewSimpleHandler creates a new command handler
func NewSimpleHandler(bot interfaces.BotInterface, cfg *config.Config) *SimpleHandler {
	h := &SimpleHandler{
		bot:      bot,
		config:   cfg,
		commands: make(map[string]SimpleCommand),
	}

	h.registerCommands()
	return h
}

// registerCommands registers all available commands
func (h *SimpleHandler) registerCommands() {
	// Create spam command and its stop command
	spamCmd := NewSpamCommand(h.bot)
	stopSpamCmd := NewStopSpamCommand(spamCmd)
	
	// Create help command
	helpCmd := NewSimpleHelpCommand(h.bot)
	
	commands := []SimpleCommand{
		// Utility commands
		NewSimplePingCommand(h.bot),
		NewSimpleInfoCommand(h.bot),
		helpCmd,
		
		// Snipe commands
		NewSimpleSnipeCommand(h.bot),
		NewSimpleEditSnipeCommand(h.bot),
		NewSimpleLastPingCommand(h.bot),
		
		// Presence command
		NewSimplePresenceCommand(h.bot),
		
		// Spam commands
		spamCmd,
		stopSpamCmd,
	}

	for _, cmd := range commands {
		h.commands[cmd.Name()] = cmd
		for _, alias := range cmd.Aliases() {
			h.commands[alias] = cmd
		}
	}
	
	// Set command map reference for help command
	helpCmd.SetCommands(&h.commands)
	
	log.Infof("Registered %d commands", len(commands))
}

// Handle processes a command message with simple, direct approach
func (h *SimpleHandler) Handle(s *discordgo.Session, m *discordgo.MessageCreate) {
	// Remove command prefix
	content := strings.TrimPrefix(m.Content, h.config.CommandPrefix)
	if content == m.Content {
		return // No prefix found
	}

	// Parse command and arguments
	parts := strings.Fields(content)
	if len(parts) == 0 {
		return
	}

	commandName := strings.ToLower(parts[0])
	args := parts[1:]

	// Find and execute command
	if cmd, exists := h.commands[commandName]; exists {
		// Delete command message immediately
		go func() {
			if err := s.ChannelMessageDelete(m.ChannelID, m.ID); err != nil {
				log.Debugf("Failed to delete command message: %v", err)
			}
		}()

		// Execute command in goroutine with error handling
		go func() {
			defer func() {
				if r := recover(); r != nil {
					log.Errorf("Command %s panicked: %v", commandName, r)
				}
			}()

			if err := cmd.Execute(s, m, args); err != nil {
				log.Errorf("Command %s error: %v", commandName, err)
				h.sendErrorMessage(s, m.ChannelID, fmt.Sprintf("Error executing command: %v", err))
			}
		}()
	}
}

// sendErrorMessage sends an error message with auto-delete
func (h *SimpleHandler) sendErrorMessage(s *discordgo.Session, channelID, content string) {
	msg, err := s.ChannelMessageSend(channelID, content)
	if err != nil {
		log.Errorf("Failed to send error message: %v", err)
		return
	}

	if h.config.AutoDelete.Enabled {
		time.AfterFunc(time.Duration(h.config.AutoDelete.Delay)*time.Second, func() {
			s.ChannelMessageDelete(channelID, msg.ID)
		})
	}
}

// SendWithAutoDelete is a helper for commands to send messages with auto-delete
func (h *SimpleHandler) SendWithAutoDelete(s *discordgo.Session, channelID, content string) {
	msg, err := s.ChannelMessageSend(channelID, content)
	if err != nil {
		log.Errorf("Failed to send message: %v", err)
		return
	}

	if h.config.AutoDelete.Enabled {
		time.AfterFunc(time.Duration(h.config.AutoDelete.Delay)*time.Second, func() {
			s.ChannelMessageDelete(channelID, msg.ID)
		})
	}
}

// Helper function to format messages with quote blocks (updated to match help formatting)
func FormatMessage(content string) string {
	lines := strings.Split(content, "\n")
	quotedLines := make([]string, len(lines))
	for i, line := range lines {
		quotedLines[i] = "> " + line
	}
	return strings.Join(quotedLines, "\n")
}

// Helper function to clean content for display
func CleanContent(content string) string {
	content = strings.ReplaceAll(content, "\\", "")
	content = strings.ReplaceAll(content, "```", "")
	content = strings.ReplaceAll(content, "`", "")
	content = strings.ReplaceAll(content, "|", "")
	content = strings.ReplaceAll(content, "*", "")
	return content
}

// Helper function to truncate content
func TruncateContent(content string, maxLength int) string {
	if len(content) > maxLength {
		return content[:maxLength-3] + "..."
	}
	return content
}