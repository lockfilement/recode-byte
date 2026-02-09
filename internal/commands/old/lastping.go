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

// LastPingCommand implements the lastping command
type LastPingCommand struct {
	bot Bot
}

// NewLastPingCommand creates a new lastping command
func NewLastPingCommand(bot Bot) *LastPingCommand {
	return &LastPingCommand{bot: bot}
}

// GetName returns the command name
func (c *LastPingCommand) GetName() string {
	return "lastping"
}

// GetAliases returns command aliases
func (c *LastPingCommand) GetAliases() []string {
	return []string{"lp"}
}

// GetDescription returns command description
func (c *LastPingCommand) GetDescription() string {
	return "Show your recent mentions"
}

// Execute executes the lastping command
func (c *LastPingCommand) Execute(s *discordgo.Session, m *discordgo.MessageCreate, args []string) error {
	// Delete command message
	if err := s.ChannelMessageDelete(m.ChannelID, m.ID); err != nil {
		log.Debugf("Failed to delete command message: %v", err)
	}

	// Parse amount
	var limit int64 = 5 // Default 5
	if len(args) > 0 {
		if num, err := strconv.ParseInt(args[0], 10, 64); err == nil {
			limit = num
		}
	}

	// Limit between 1 and 1000
	if limit < 1 {
		limit = 1
	} else if limit > 1000 {
		limit = 1000
	}

	// Build filter for mentions targeting this user
	filter := bson.M{
		"target_id": c.bot.GetUserID(),
	}

	// Get mentions from database
	mentions, err := c.bot.GetDatabaseInterface().GetMentionsInterface(filter, limit)
	if err != nil {
		return fmt.Errorf("failed to fetch mentions: %w", err)
	}

	if len(mentions) == 0 {
		content := "```ansi\n" +
			"\u001b[1;35mNo Mentions Found\n" +
			"\u001b[0;37m─────────────────\n" +
			"\u001b[0;37mNo mentions found for you```"
		
		msg, err := s.ChannelMessageSend(m.ChannelID, formatMessage(content))
		if err != nil {
			return err
		}

		// Auto-delete if configured
		if c.bot.GetConfig().AutoDelete.Enabled {
			time.AfterFunc(time.Duration(c.bot.GetConfig().AutoDelete.Delay)*time.Second, func() {
				s.ChannelMessageDelete(m.ChannelID, msg.ID)
			})
		}
		return nil
	}

	// Format and send mentions
	return c.formatAndSendMentions(s, m.ChannelID, mentions)
}

// formatAndSendMentions formats and sends mention messages
func (c *LastPingCommand) formatAndSendMentions(s *discordgo.Session, channelID string, mentions []interface{}) error {
	const chunkSize = 10

	for chunkStart := 0; chunkStart < len(mentions); chunkStart += chunkSize {
		chunkEnd := chunkStart + chunkSize
		if chunkEnd > len(mentions) {
			chunkEnd = len(mentions)
		}

		chunk := mentions[chunkStart:chunkEnd]
		
		content := "```ansi\n\u001b[30m\u001b[1m\u001b[4mLast Mentions\u001b[0m\n"
		attachments := []string{}

		for idx, mentionInterface := range chunk {
			mentionMap, ok := mentionInterface.(map[string]interface{})
			if !ok {
				continue
			}

			num := chunkStart + idx + 1
			authorName := "Unknown User"
			if name, ok := mentionMap["author_name"].(string); ok {
				authorName = name
			}

			mentionContent := ""
			if c, ok := mentionMap["content"].(string); ok {
				mentionContent = cleanContent(c)
				mentionContent = truncateContent(mentionContent, 512)
			}

			_ = "Unknown Time" // unused
			timeStr := "Today at Unknown"
			if t, ok := mentionMap["created_at"].(time.Time); ok {
				now := time.Now()
				if now.Sub(t).Hours() > 24 {
					timeStr = fmt.Sprintf("Yesterday at %s", t.Format("3:04 PM"))
				} else {
					timeStr = fmt.Sprintf("Today at %s", t.Format("3:04 PM"))
				}
			}

			content += fmt.Sprintf("\u001b[1;33m#%d\n", num)
			content += fmt.Sprintf("\u001b[1;37m%s \u001b[0m%s\n", authorName, timeStr)
			
			if mentionContent != "" {
				for _, line := range strings.Split(mentionContent, "\n") {
					content += fmt.Sprintf("\u001b[1;31m%s\n", line)
				}
			}

			// Handle attachments
			if attachList, ok := mentionMap["attachments"].([]interface{}); ok && len(attachList) > 0 {
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
			if guildName, ok := mentionMap["guild_name"].(string); ok && guildName != "" {
				if channelName, ok := mentionMap["channel_name"].(string); ok {
					location = fmt.Sprintf("#%s in %s", channelName, guildName)
				}
			} else if channelType, ok := mentionMap["channel_type"].(int); ok {
				switch channelType {
				case 3:
					location = "Group chat"
				case 1:
					location = "DM"
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