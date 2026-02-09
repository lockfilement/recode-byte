package commands

import (
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/LightningDev1/discordgo"
	"go.mongodb.org/mongo-driver/bson"
	log "github.com/sirupsen/logrus"
)

// SnipeCommand implements the snipe command
type SnipeCommand struct {
	bot Bot
}

// NewSnipeCommand creates a new snipe command
func NewSnipeCommand(bot Bot) *SnipeCommand {
	return &SnipeCommand{bot: bot}
}

// GetName returns the command name
func (c *SnipeCommand) GetName() string {
	return "snipe"
}

// GetAliases returns command aliases
func (c *SnipeCommand) GetAliases() []string {
	return []string{"sn"}
}

// GetDescription returns command description
func (c *SnipeCommand) GetDescription() string {
	return "Show deleted messages"
}

// Execute executes the snipe command
func (c *SnipeCommand) Execute(s *discordgo.Session, m *discordgo.MessageCreate, args []string) error {
	// Delete command message
	if err := s.ChannelMessageDelete(m.ChannelID, m.ID); err != nil {
		log.Debugf("Failed to delete command message: %v", err)
	}

	// Parse arguments
	var userID string
	var channelID string = m.ChannelID
	var limit int64 = 1

	for i, arg := range args {
		// Check if it's a user mention
		if strings.HasPrefix(arg, "<@") && strings.HasSuffix(arg, ">") {
			userID = strings.Trim(arg, "<@!>")
		} else if strings.HasPrefix(arg, "<#") && strings.HasSuffix(arg, ">") {
			// Channel mention
			channelID = strings.Trim(arg, "<#>")
		} else if num, err := strconv.ParseInt(arg, 10, 64); err == nil {
			if len(arg) > 15 { // Likely a Discord ID
				// Try to determine if it's a user or channel
				if user, err := s.User(arg); err == nil && user != nil {
					userID = arg
				} else if _, err := s.Channel(arg); err == nil {
					channelID = arg
				}
			} else {
				// Likely a count
				limit = num
			}
		}
		
		// Handle amount after user
		if userID != "" && i+1 < len(args) {
			if num, err := strconv.ParseInt(args[i+1], 10, 64); err == nil && num < 100 {
				limit = num
			}
		}
	}

	// Build filter
	filter := bson.M{
		"user_id": bson.M{"$ne": c.bot.GetUserID()}, // Exclude selfbot messages
	}

	if userID != "" {
		filter["user_id"] = userID
	}
	if channelID != "" {
		filter["channel_id"] = channelID
	}

	// Limit between 1 and 1000
	if limit < 1 {
		limit = 1
	} else if limit > 1000 {
		limit = 1000
	}

	// Get deleted messages from database
	messages, err := c.bot.GetDatabaseInterface().GetDeletedMessagesInterface(filter, limit)
	if err != nil {
		return fmt.Errorf("failed to fetch deleted messages: %w", err)
	}

	if len(messages) == 0 {
		content := "```ansi\n" +
			"\u001b[1;35mNo Messages Found\n" +
			"\u001b[0;37m─────────────────\n" +
			"\u001b[0;37mNo deleted messages found```"
		
		_, err := s.ChannelMessageSend(m.ChannelID, formatMessage(content))
		if err != nil {
			return err
		}
		return nil
	}

	// Format and send messages
	return c.formatAndSendMessages(s, m.ChannelID, messages)
}

// formatAndSendMessages formats and sends deleted messages
func (c *SnipeCommand) formatAndSendMessages(s *discordgo.Session, channelID string, messages []interface{}) error {
	const chunkSize = 10 // Process in chunks

	for chunkStart := 0; chunkStart < len(messages); chunkStart += chunkSize {
		chunkEnd := chunkStart + chunkSize
		if chunkEnd > len(messages) {
			chunkEnd = len(messages)
		}

		chunk := messages[chunkStart:chunkEnd]
		
		// Build message content
		content := "```ansi\n\u001b[30m\u001b[1m\u001b[4mDeleted Messages\u001b[0m\n"
		attachments := []string{}

		for idx, msgInterface := range chunk {
			// Type assertion - this is a simplified approach
			// In a real implementation, you'd want proper type handling
			msgMap, ok := msgInterface.(map[string]interface{})
			if !ok {
				continue
			}

			num := chunkStart + idx + 1
			username := "Unknown User"
			if u, ok := msgMap["username"].(string); ok {
				username = u
			}

			msgContent := ""
			if c, ok := msgMap["content"].(string); ok {
				msgContent = cleanContent(c)
				msgContent = truncateContent(msgContent, 256)
			}

			timestamp := "Unknown Time"
			if t, ok := msgMap["deleted_at"].(time.Time); ok {
				timestamp = t.Format("3:04 PM")
			}

			content += fmt.Sprintf("\u001b[1;33m#%d\n", num)
			content += fmt.Sprintf("\u001b[1;37m%s \u001b[0mToday at %s\n", username, timestamp)
			
			if msgContent != "" {
				for _, line := range strings.Split(msgContent, "\n") {
					content += fmt.Sprintf("\u001b[1;31m%s\n", line)
				}
			}

			// Handle attachments
			if attachList, ok := msgMap["attachments"].([]interface{}); ok && len(attachList) > 0 {
				if len(attachList) == 1 {
					content += "\u001b[0;36m└─── [ 1 Attachment ]\n"
				} else {
					content += fmt.Sprintf("\u001b[0;36m└─── [ %d Attachments ]\n", len(attachList))
				}
				
				for _, att := range attachList {
					if attStr, ok := att.(string); ok {
						attachments = append(attachments, attStr)
					}
				}
			}

			// Add location info
			location := "Unknown"
			if guildName, ok := msgMap["guild_name"].(string); ok && guildName != "" {
				if channelName, ok := msgMap["channel_name"].(string); ok {
					location = fmt.Sprintf("#%s in %s", channelName, guildName)
				}
			} else if channelType, ok := msgMap["channel_type"].(string); ok {
				switch channelType {
				case "group":
					location = "Group chat"
				case "DMs":
					location = fmt.Sprintf("DM with %s", username)
				default:
					location = "Unknown channel"
				}
			}

			content += fmt.Sprintf("\u001b[0;36m%s\n", location)
			content += "\u001b[0;37m────────────────────────────\n"
		}

		content += "```"

		// Send formatted message
		msg, err := s.ChannelMessageSend(channelID, formatMessage(content))
		if err != nil {
			return err
		}

		// Auto-delete if configured
		if c.bot.GetConfig().AutoDelete.Enabled {
			time.AfterFunc(time.Duration(c.bot.GetConfig().AutoDelete.Delay)*time.Second, func() {
				s.ChannelMessageDelete(channelID, msg.ID)
			})
		}

		// Send attachments if any
		if len(attachments) > 0 {
			attachmentMsg, err := s.ChannelMessageSend(channelID, strings.Join(attachments, "\n"))
			if err != nil {
				log.Errorf("Failed to send attachments: %v", err)
			} else if c.bot.GetConfig().AutoDelete.Enabled {
				time.AfterFunc(time.Duration(c.bot.GetConfig().AutoDelete.Delay)*time.Second, func() {
					s.ChannelMessageDelete(channelID, attachmentMsg.ID)
				})
			}
		}
	}

	return nil
}