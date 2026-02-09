package commands

import (
	"fmt"
	"strconv"
	"strings"
	"time"

	"selfbot/internal/database"
	"selfbot/internal/interfaces"

	"github.com/LightningDev1/discordgo"
	"go.mongodb.org/mongo-driver/bson"
	log "github.com/sirupsen/logrus"
)

// SimpleSnipeCommand implements the snipe command with clean Go patterns
type SimpleSnipeCommand struct {
	bot interfaces.BotInterface
}

// NewSimpleSnipeCommand creates a new snipe command
func NewSimpleSnipeCommand(bot interfaces.BotInterface) *SimpleSnipeCommand {
	return &SimpleSnipeCommand{bot: bot}
}

// Name returns the command name
func (c *SimpleSnipeCommand) Name() string {
	return "snipe"
}

// Aliases returns command aliases
func (c *SimpleSnipeCommand) Aliases() []string {
	return []string{"sn"}
}

// Description returns command description
func (c *SimpleSnipeCommand) Description() string {
	return "Show deleted messages"
}

// Execute executes the snipe command with simplified logic
func (c *SimpleSnipeCommand) Execute(s *discordgo.Session, m *discordgo.MessageCreate, args []string) error {
	// Parse arguments with simple approach
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

	// Build simple filter
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

	// Get deleted messages from database - direct call
	messages, err := c.bot.GetDatabase().GetDeletedMessages(filter, limit)
	if err != nil {
		return fmt.Errorf("failed to fetch deleted messages: %w", err)
	}

	if len(messages) == 0 {
		content := "```ansi\n" +
			"\u001b[1;35mNo Messages Found\n" +
			"\u001b[0;37m─────────────────\n" +
			"\u001b[0;37mNo deleted messages found```"
		
		msg, err := s.ChannelMessageSend(m.ChannelID, FormatMessage(content))
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

	// Format and send messages with simple approach
	return c.formatAndSendMessages(s, m.ChannelID, messages)
}

// formatAndSendMessages formats and sends deleted messages with clean logic
func (c *SimpleSnipeCommand) formatAndSendMessages(s *discordgo.Session, channelID string, messages []database.SimpleDeletedMessageData) error {
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

		for idx, msg := range chunk {
			num := chunkStart + idx + 1
			username := msg.Username
			if username == "" {
				username = "Unknown User"
			}

			msgContent := CleanContent(msg.Content)
			msgContent = TruncateContent(msgContent, 256)

			timestamp := msg.DeletedAt.Format("3:04 PM")

			content += fmt.Sprintf("\u001b[1;33m#%d\n", num)
			content += fmt.Sprintf("\u001b[1;37m%s \u001b[0mToday at %s\n", username, timestamp)
			
			if msgContent != "" {
				for _, line := range strings.Split(msgContent, "\n") {
					content += fmt.Sprintf("\u001b[1;31m%s\n", line)
				}
			}

			// Handle attachments
			if len(msg.Attachments) > 0 {
				if len(msg.Attachments) == 1 {
					content += "\u001b[0;36m└─── [ 1 Attachment ]\n"
				} else {
					content += fmt.Sprintf("\u001b[0;36m└─── [ %d Attachments ]\n", len(msg.Attachments))
				}
				
				attachments = append(attachments, msg.Attachments...)
			}

			// Add location info
			location := "Unknown"
			if msg.GuildName != "" && msg.ChannelName != "" {
				location = fmt.Sprintf("#%s in %s", msg.ChannelName, msg.GuildName)
			} else if msg.ChannelType == "group" {
				location = "Group chat"
			} else if msg.ChannelType == "DMs" {
				location = fmt.Sprintf("DM with %s", username)
			}

			content += fmt.Sprintf("\u001b[0;36m%s\n", location)
			content += "\u001b[0;37m────────────────────────────\n"
		}

		content += "```"

		// Send formatted message
		msg, err := s.ChannelMessageSend(channelID, FormatMessage(content))
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

// Placeholder implementations for other simple commands
type SimpleEditSnipeCommand struct {
	bot interfaces.BotInterface
}

func NewSimpleEditSnipeCommand(bot interfaces.BotInterface) *SimpleEditSnipeCommand {
	return &SimpleEditSnipeCommand{bot: bot}
}

func (c *SimpleEditSnipeCommand) Name() string { return "editsnipe" }
func (c *SimpleEditSnipeCommand) Aliases() []string { return []string{"es"} }
func (c *SimpleEditSnipeCommand) Description() string { return "Show edited messages" }
func (c *SimpleEditSnipeCommand) Execute(s *discordgo.Session, m *discordgo.MessageCreate, args []string) error {
	// Parse arguments similar to snipe
	var userID string
	var channelID string = m.ChannelID
	var limit int64 = 1

	for i, arg := range args {
		if strings.HasPrefix(arg, "<@") && strings.HasSuffix(arg, ">") {
			userID = strings.Trim(arg, "<@!>")
		} else if strings.HasPrefix(arg, "<#") && strings.HasSuffix(arg, ">") {
			channelID = strings.Trim(arg, "<#>")
		} else if num, err := strconv.ParseInt(arg, 10, 64); err == nil {
			if len(arg) > 15 {
				if user, err := s.User(arg); err == nil && user != nil {
					userID = arg
				} else if _, err := s.Channel(arg); err == nil {
					channelID = arg
				}
			} else {
				limit = num
			}
		}
		if userID != "" && i+1 < len(args) {
			if num, err := strconv.ParseInt(args[i+1], 10, 64); err == nil && num < 100 {
				limit = num
			}
		}
	}

	// Build filter
	filter := bson.M{
		"user_id": bson.M{"$ne": c.bot.GetUserID()},
	}
	if userID != "" {
		filter["user_id"] = userID
	}
	if channelID != "" {
		filter["channel_id"] = channelID
	}
	if limit < 1 {
		limit = 1
	} else if limit > 1000 {
		limit = 1000
	}

	// Get edited messages
	messages, err := c.bot.GetDatabase().GetEditedMessages(filter, limit)
	if err != nil {
		return fmt.Errorf("failed to fetch edited messages: %w", err)
	}

	if len(messages) == 0 {
		content := "```ansi\n" +
			"\u001b[1;35mNo Messages Found\n" +
			"\u001b[0;37m─────────────────\n" +
			"\u001b[0;37mNo edited messages found```"
		msg, err := s.ChannelMessageSend(m.ChannelID, FormatMessage(content))
		if err != nil {
			return err
		}
		if c.bot.GetConfig().AutoDelete.Enabled {
			time.AfterFunc(time.Duration(c.bot.GetConfig().AutoDelete.Delay)*time.Second, func() {
				s.ChannelMessageDelete(m.ChannelID, msg.ID)
			})
		}
		return nil
	}

	return c.formatAndSendEditedMessages(s, m.ChannelID, messages)
}

type SimpleLastPingCommand struct {
	bot interfaces.BotInterface
}

func NewSimpleLastPingCommand(bot interfaces.BotInterface) *SimpleLastPingCommand {
	return &SimpleLastPingCommand{bot: bot}
}

func (c *SimpleLastPingCommand) Name() string { return "lastping" }
func (c *SimpleLastPingCommand) Aliases() []string { return []string{"lp"} }
func (c *SimpleLastPingCommand) Description() string { return "Show your recent mentions" }
func (c *SimpleLastPingCommand) Execute(s *discordgo.Session, m *discordgo.MessageCreate, args []string) error {
	// Parse amount
	var limit int64 = 5
	if len(args) > 0 {
		if num, err := strconv.ParseInt(args[0], 10, 64); err == nil {
			limit = num
		}
	}
	if limit < 1 {
		limit = 1
	} else if limit > 1000 {
		limit = 1000
	}

	// Build filter for mentions targeting this user
	filter := bson.M{
		"target_id": c.bot.GetUserID(),
	}

	// Get mentions
	mentions, err := c.bot.GetDatabase().GetMentions(filter, limit)
	if err != nil {
		return fmt.Errorf("failed to fetch mentions: %w", err)
	}

	if len(mentions) == 0 {
		content := "```ansi\n" +
			"\u001b[1;35mNo Mentions Found\n" +
			"\u001b[0;37m─────────────────\n" +
			"\u001b[0;37mNo mentions found for you```"
		msg, err := s.ChannelMessageSend(m.ChannelID, FormatMessage(content))
		if err != nil {
			return err
		}
		if c.bot.GetConfig().AutoDelete.Enabled {
			time.AfterFunc(time.Duration(c.bot.GetConfig().AutoDelete.Delay)*time.Second, func() {
				s.ChannelMessageDelete(m.ChannelID, msg.ID)
			})
		}
		return nil
	}

	return c.formatAndSendMentions(s, m.ChannelID, mentions)
}

// formatAndSendEditedMessages formats and sends edited messages
func (c *SimpleEditSnipeCommand) formatAndSendEditedMessages(s *discordgo.Session, channelID string, messages []database.SimpleEditedMessageData) error {
	const chunkSize = 10

	for chunkStart := 0; chunkStart < len(messages); chunkStart += chunkSize {
		chunkEnd := chunkStart + chunkSize
		if chunkEnd > len(messages) {
			chunkEnd = len(messages)
		}

		chunk := messages[chunkStart:chunkEnd]
		content := "```ansi\n\u001b[30m\u001b[1m\u001b[4mEdited Messages\u001b[0m\n"
		attachments := []string{}

		for idx, msg := range chunk {
			num := chunkStart + idx + 1
			username := msg.Username
			if username == "" {
				username = "Unknown User"
			}

			beforeContent := CleanContent(msg.BeforeContent)
			beforeContent = TruncateContent(beforeContent, 128)
			afterContent := CleanContent(msg.AfterContent)
			afterContent = TruncateContent(afterContent, 128)

			timestamp := msg.EditedAt.Format("3:04 PM")

			content += fmt.Sprintf("\u001b[1;33m#%d\n", num)
			content += fmt.Sprintf("\u001b[1;37m%s \u001b[0mToday at %s\n", username, timestamp)
			content += fmt.Sprintf("\u001b[1;31m%s -> %s\n", beforeContent, afterContent)

			// Handle attachments
			if len(msg.AfterAttachments) > 0 {
				if len(msg.AfterAttachments) == 1 {
					content += "\u001b[0;36m└─── [ 1 Attachment ]\n"
				} else {
					content += fmt.Sprintf("\u001b[0;36m└─── [ %d Attachments ]\n", len(msg.AfterAttachments))
				}
				attachments = append(attachments, msg.AfterAttachments...)
			}

			// Add location info
			location := "Unknown"
			if msg.GuildName != "" && msg.ChannelName != "" {
				location = fmt.Sprintf("#%s in %s", msg.ChannelName, msg.GuildName)
			} else if msg.ChannelType == "group" {
				location = "Group chat"
			} else if msg.ChannelType == "DMs" {
				location = fmt.Sprintf("DM with %s", username)
			}

			content += fmt.Sprintf("\u001b[0;36m%s\n", location)
			content += "\u001b[0;37m────────────────────────────\n"
		}

		content += "```"

		// Send formatted message
		msg, err := s.ChannelMessageSend(channelID, FormatMessage(content))
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

// formatAndSendMentions formats and sends mention messages
func (c *SimpleLastPingCommand) formatAndSendMentions(s *discordgo.Session, channelID string, mentions []database.SimpleMentionData) error {
	const chunkSize = 10

	for chunkStart := 0; chunkStart < len(mentions); chunkStart += chunkSize {
		chunkEnd := chunkStart + chunkSize
		if chunkEnd > len(mentions) {
			chunkEnd = len(mentions)
		}

		chunk := mentions[chunkStart:chunkEnd]
		content := "```ansi\n\u001b[30m\u001b[1m\u001b[4mLast Mentions\u001b[0m\n"
		attachments := []string{}

		for idx, mention := range chunk {
			num := chunkStart + idx + 1
			authorName := mention.AuthorName
			if authorName == "" {
				authorName = "Unknown User"
			}

			mentionContent := CleanContent(mention.Content)
			mentionContent = TruncateContent(mentionContent, 512)

			timeStr := "Today at Unknown"
			now := time.Now()
			if now.Sub(mention.CreatedAt).Hours() > 24 {
				timeStr = fmt.Sprintf("Yesterday at %s", mention.CreatedAt.Format("3:04 PM"))
			} else {
				timeStr = fmt.Sprintf("Today at %s", mention.CreatedAt.Format("3:04 PM"))
			}

			content += fmt.Sprintf("\u001b[1;33m#%d\n", num)
			content += fmt.Sprintf("\u001b[1;37m%s \u001b[0m%s\n", authorName, timeStr)

			if mentionContent != "" {
				for _, line := range strings.Split(mentionContent, "\n") {
					content += fmt.Sprintf("\u001b[1;31m%s\n", line)
				}
			}

			// Handle attachments
			if len(mention.Attachments) > 0 {
				if len(mention.Attachments) == 1 {
					content += "\u001b[0;36m└─── [ 1 Attachment ]\n"
				} else {
					content += fmt.Sprintf("\u001b[0;36m└─── [ %d Attachments ]\n", len(mention.Attachments))
				}
				attachments = append(attachments, mention.Attachments...)
			}

			// Add location info
			location := "Unknown"
			if mention.GuildName != "" && mention.ChannelName != "" {
				location = fmt.Sprintf("#%s in %s", mention.ChannelName, mention.GuildName)
			} else if mention.ChannelType == 3 {
				location = "Group chat"
			} else if mention.ChannelType == 1 {
				location = "DM"
			}

			content += fmt.Sprintf("\u001b[0;36m%s\n", location)
			content += "\u001b[0;37m────────────────────────────\n"
		}

		content += "```"

		// Send formatted message
		msg, err := s.ChannelMessageSend(channelID, FormatMessage(content))
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

type SimplePresenceCommand struct {
	bot interfaces.BotInterface
}

func NewSimplePresenceCommand(bot interfaces.BotInterface) *SimplePresenceCommand {
	return &SimplePresenceCommand{bot: bot}
}

func (c *SimplePresenceCommand) Name() string { return "presence" }
func (c *SimplePresenceCommand) Aliases() []string { return []string{"rp"} }
func (c *SimplePresenceCommand) Description() string { return "Configure rich presence" }
func (c *SimplePresenceCommand) Execute(s *discordgo.Session, m *discordgo.MessageCreate, args []string) error {
	if len(args) == 0 {
		return c.sendPresenceUsage(s, m.ChannelID)
	}

	subcommand := strings.ToLower(args[0])
	switch subcommand {
	case "status":
		return c.handleStatusCommand(s, m.ChannelID, args[1:])
	case "activity":
		return c.handleActivityCommand(s, m.ChannelID, args[1:])
	case "clear":
		return c.clearPresence(s, m.ChannelID)
	case "show":
		return c.showCurrentPresence(s, m.ChannelID)
	default:
		return c.sendPresenceUsage(s, m.ChannelID)
	}
}

func (c *SimplePresenceCommand) handleStatusCommand(s *discordgo.Session, channelID string, args []string) error {
	if len(args) == 0 {
		return c.sendError(s, channelID, "Status required. Options: online, idle, dnd, invisible")
	}

	status := strings.ToLower(args[0])
	var discordStatus discordgo.Status

	switch status {
	case "online":
		discordStatus = discordgo.StatusOnline
	case "idle", "away":
		discordStatus = discordgo.StatusIdle
	case "dnd", "busy":
		discordStatus = discordgo.StatusDoNotDisturb
	case "invisible", "offline":
		discordStatus = discordgo.StatusInvisible
	default:
		return c.sendError(s, channelID, "Invalid status. Options: online, idle, dnd, invisible")
	}

	err := s.UpdateStatusComplex(discordgo.UpdateStatusData{
		Status: string(discordStatus),
		AFK:    false,
	})

	if err != nil {
		return c.sendError(s, channelID, "Failed to update status: "+err.Error())
	}

	return c.sendSuccess(s, channelID, fmt.Sprintf("Status updated to: %s", status))
}

func (c *SimplePresenceCommand) handleActivityCommand(s *discordgo.Session, channelID string, args []string) error {
	if len(args) < 2 {
		return c.sendError(s, channelID, "Usage: activity <type> <name> [state] [details]")
	}

	activityType := strings.ToLower(args[0])
	name := args[1]

	var discordActivityType discordgo.ActivityType
	switch activityType {
	case "playing", "game":
		discordActivityType = discordgo.ActivityTypeGame
	case "streaming":
		discordActivityType = discordgo.ActivityTypeStreaming
	case "listening":
		discordActivityType = discordgo.ActivityTypeListening
	case "watching":
		discordActivityType = discordgo.ActivityTypeWatching
	case "competing":
		discordActivityType = discordgo.ActivityTypeCompeting
	default:
		return c.sendError(s, channelID, "Invalid activity type. Options: playing, streaming, listening, watching, competing")
	}

	activity := &discordgo.Activity{
		Name: name,
		Type: discordActivityType,
	}

	if len(args) > 2 {
		activity.State = strings.Join(args[2:], " ")
	}

	if len(args) > 3 && activityType == "streaming" {
		activity.URL = args[2]
		if len(args) > 3 {
			activity.State = strings.Join(args[3:], " ")
		}
	}

	err := s.UpdateStatusComplex(discordgo.UpdateStatusData{
		Activities: []*discordgo.Activity{activity},
		AFK:        false,
	})

	if err != nil {
		return c.sendError(s, channelID, "Failed to update activity: "+err.Error())
	}

	return c.sendSuccess(s, channelID, fmt.Sprintf("Activity updated: %s %s", activityType, name))
}

func (c *SimplePresenceCommand) clearPresence(s *discordgo.Session, channelID string) error {
	err := s.UpdateStatusComplex(discordgo.UpdateStatusData{
		Status:     string(discordgo.StatusOnline),
		Activities: []*discordgo.Activity{},
		AFK:        false,
	})

	if err != nil {
		return c.sendError(s, channelID, "Failed to clear presence: "+err.Error())
	}

	return c.sendSuccess(s, channelID, "Presence cleared")
}

func (c *SimplePresenceCommand) showCurrentPresence(s *discordgo.Session, channelID string) error {
	user, err := s.User("@me")
	if err != nil {
		return c.sendError(s, channelID, "Failed to get current user: "+err.Error())
	}

	guild, err := s.State.Guild(channelID)
	var presence *discordgo.Presence
	if err == nil {
		for _, p := range guild.Presences {
			if p.User.ID == user.ID {
				presence = p
				break
			}
		}
	}

	content := "```ansi\n\\u001b[1;35mCurrent Presence\\n\\u001b[0;37m─────────────────\n"
	
	if presence != nil {
		content += fmt.Sprintf("\\u001b[1;37mStatus: \\u001b[0;34m%s\\n", presence.Status)
		if len(presence.Activities) > 0 {
			activity := presence.Activities[0]
			content += fmt.Sprintf("\\u001b[1;37mActivity: \\u001b[0;34m%s %s\\n", activity.Type, activity.Name)
			if activity.State != "" {
				content += fmt.Sprintf("\\u001b[1;37mState: \\u001b[0;34m%s\\n", activity.State)
			}
		} else {
			content += "\\u001b[1;37mActivity: \\u001b[0;34mNone\\n"
		}
	} else {
		content += "\\u001b[1;37mStatus: \\u001b[0;34mUnknown\\n"
		content += "\\u001b[1;37mActivity: \\u001b[0;34mNone\\n"
	}

	content += "```"

	msg, err := s.ChannelMessageSend(channelID, FormatMessage(content))
	if err != nil {
		return err
	}

	if c.bot.GetConfig().AutoDelete.Enabled {
		time.AfterFunc(time.Duration(c.bot.GetConfig().AutoDelete.Delay)*time.Second, func() {
			s.ChannelMessageDelete(channelID, msg.ID)
		})
	}

	return nil
}

func (c *SimplePresenceCommand) sendPresenceUsage(s *discordgo.Session, channelID string) error {
	usage := "**Presence Command Usage:**\n" +
		"`.presence status <status>` - Set status (online, idle, dnd, invisible)\n" +
		"`.presence activity <type> <name> [state]` - Set activity\n" +
		"`.presence clear` - Clear all presence\n" +
		"`.presence show` - Show current presence\n\n" +
		"**Activity Types:** playing, streaming, listening, watching, competing\n\n" +
		"**Examples:**\n" +
		"`.presence status dnd`\n" +
		"`.presence activity playing Minecraft`\n" +
		"`.presence activity listening to music`\n" +
		"`.presence activity streaming on Twitch https://twitch.tv/user`"

	return c.sendTempMessage(s, channelID, usage)
}

func (c *SimplePresenceCommand) sendError(s *discordgo.Session, channelID, message string) error {
	return c.sendTempMessage(s, channelID, "❌ "+message)
}

func (c *SimplePresenceCommand) sendSuccess(s *discordgo.Session, channelID, message string) error {
	return c.sendTempMessage(s, channelID, "✅ "+message)
}

func (c *SimplePresenceCommand) sendTempMessage(s *discordgo.Session, channelID, content string) error {
	msg, err := s.ChannelMessageSend(channelID, content)
	if err != nil {
		return err
	}

	if c.bot.GetConfig().AutoDelete.Enabled {
		go func() {
			time.Sleep(time.Duration(c.bot.GetConfig().AutoDelete.Delay) * time.Second)
			s.ChannelMessageDelete(channelID, msg.ID)
		}()
	}

	return nil
}