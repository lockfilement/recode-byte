package commands

import (
	"fmt"
	"runtime"
	"strings"
	"time"

	"selfbot/internal/interfaces"

	"github.com/LightningDev1/discordgo"
)

// SimplePingCommand implements a basic ping command
type SimplePingCommand struct {
	bot interfaces.BotInterface
}

func NewSimplePingCommand(bot interfaces.BotInterface) *SimplePingCommand {
	return &SimplePingCommand{bot: bot}
}

func (c *SimplePingCommand) Name() string        { return "ping" }
func (c *SimplePingCommand) Aliases() []string   { return []string{"latency"} }
func (c *SimplePingCommand) Description() string { return "Check bot latency" }

func (c *SimplePingCommand) Execute(s *discordgo.Session, m *discordgo.MessageCreate, args []string) error {
	start := time.Now()
	
	// Send initial message
	msg, err := s.ChannelMessageSend(m.ChannelID, "🏓 Pinging...")
	if err != nil {
		return err
	}
	
	// Calculate latency
	latency := time.Since(start)
	
	// Get heartbeat latency if available
	var heartbeat string
	if s.HeartbeatLatency() > 0 {
		heartbeat = fmt.Sprintf("%.0fms", float64(s.HeartbeatLatency().Nanoseconds())/1000000)
	} else {
		heartbeat = "N/A"
	}
	
	content := fmt.Sprintf("```ansi\n"+
		"\\u001b[1;35mPing Results\\n"+
		"\\u001b[0;37m─────────────\\n"+
		"\\u001b[1;37mAPI Latency: \\u001b[0;34m%.0fms\\n"+
		"\\u001b[1;37mHeartbeat: \\u001b[0;34m%s\\n"+
		"```", 
		float64(latency.Nanoseconds())/1000000, heartbeat)
	
	// Edit the message with results
	_, err = s.ChannelMessageEdit(m.ChannelID, msg.ID, FormatMessage(content))
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

// SimpleInfoCommand provides bot information
type SimpleInfoCommand struct {
	bot       interfaces.BotInterface
	startTime time.Time
}

func NewSimpleInfoCommand(bot interfaces.BotInterface) *SimpleInfoCommand {
	return &SimpleInfoCommand{
		bot:       bot,
		startTime: time.Now(),
	}
}

func (c *SimpleInfoCommand) Name() string        { return "info" }
func (c *SimpleInfoCommand) Aliases() []string   { return []string{"about", "stats"} }
func (c *SimpleInfoCommand) Description() string { return "Display bot information" }

func (c *SimpleInfoCommand) Execute(s *discordgo.Session, m *discordgo.MessageCreate, args []string) error {
	// Get memory stats
	var memStats runtime.MemStats
	runtime.ReadMemStats(&memStats)
	
	// Calculate uptime
	uptime := time.Since(c.startTime)
	
	// Get user info
	user := s.State.User
	username := "Unknown"
	userID := "Unknown"
	if user != nil {
		username = user.Username
		userID = user.ID
	}
	
	// Format memory usage
	memUsage := float64(memStats.Alloc) / 1024 / 1024 // Convert to MB
	
	content := fmt.Sprintf("```ansi\n"+
		"\\u001b[1;35mBot Information\\n"+
		"\\u001b[0;37m────────────────\\n"+
		"\\u001b[1;37mUser: \\u001b[0;34m%s (%s)\\n"+
		"\\u001b[1;37mUptime: \\u001b[0;34m%s\\n"+
		"\\u001b[1;37mMemory: \\u001b[0;34m%.1f MB\\n"+
		"\\u001b[1;37mGo Version: \\u001b[0;34m%s\\n"+
		"\\u001b[1;37mGoroutines: \\u001b[0;34m%d\\n"+
		"```",
		username, userID,
		formatDuration(uptime),
		memUsage,
		runtime.Version(),
		runtime.NumGoroutine())
	
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

// SimpleHelpCommand provides basic help information
type SimpleHelpCommand struct {
	bot      interfaces.BotInterface
	commands *map[string]SimpleCommand  // Reference to handler's command map
}

func NewSimpleHelpCommand(bot interfaces.BotInterface) *SimpleHelpCommand {
	return &SimpleHelpCommand{
		bot: bot,
	}
}

// SetCommands sets the reference to the command map
func (c *SimpleHelpCommand) SetCommands(commands *map[string]SimpleCommand) {
	c.commands = commands
}

func (c *SimpleHelpCommand) Name() string        { return "help" }
func (c *SimpleHelpCommand) Aliases() []string   { return []string{"h", "commands"} }
func (c *SimpleHelpCommand) Description() string { return "Display available commands" }

func (c *SimpleHelpCommand) Execute(s *discordgo.Session, m *discordgo.MessageCreate, args []string) error {
	if len(args) > 0 {
		// Check if it's a category first
		categoryName := strings.ToLower(args[0])
		if c.isValidCategory(categoryName) {
			return c.showCategoryHelp(s, m.ChannelID, categoryName)
		}
		
		// Show help for specific command
		return c.showCommandHelp(s, m.ChannelID, args[0])
	}
	
	// Show general help
	return c.showGeneralHelp(s, m.ChannelID)
}

func (c *SimpleHelpCommand) showGeneralHelp(s *discordgo.Session, channelID string) error {
	// Get bot prefix from config
	prefix := c.bot.GetConfig().CommandPrefix
	
	// Build the message content directly like Python version
	content := "```ansi\n" +
		"Category" + "\u001b[30m: \u001b[34m" + prefix + "help <category> [page]\u001b[0m\n" +
		"Commands" + "\u001b[30m: \u001b[34m" + prefix + "help <command>\u001b[0m\n" +
		"```" +
		"```ansi\n" +
		"\u001b[30m\u001b[1m\u001b[4mCategories\u001b[0m\n"
	
	if c.commands == nil {
		content += "\u001b[0;37mNo commands available\u001b[0m\n"
	} else {
		// Define categories with descriptions like Python version
		type CategoryInfo struct {
			Name        string
			Description string
			Commands    []SimpleCommand
		}
		
		categories := []CategoryInfo{
			{"General", "Config commands", []SimpleCommand{}},
			{"Tools", "Tool commands", []SimpleCommand{}},
			{"Utility", "Misc commands", []SimpleCommand{}},
			{"Tracking", "Tracking commands", []SimpleCommand{}},
		}
		
		// Categorize commands
		seen := make(map[string]bool)
		for _, cmd := range *c.commands {
			if seen[cmd.Name()] {
				continue
			}
			seen[cmd.Name()] = true
			
			switch cmd.Name() {
			case "help", "info":
				categories[0].Commands = append(categories[0].Commands, cmd)
			case "spam", "sspam":
				categories[1].Commands = append(categories[1].Commands, cmd)
			case "ping", "presence":
				categories[2].Commands = append(categories[2].Commands, cmd)
			case "snipe", "editsnipe", "lastping":
				categories[3].Commands = append(categories[3].Commands, cmd)
			}
		}
		
		// Calculate padding like Python version
		maxNameLen := 0
		for _, cat := range categories {
			if len(cat.Commands) > 0 && len(cat.Name) > maxNameLen {
				maxNameLen = len(cat.Name)
			}
		}
		paddingLength := maxNameLen + 2
		
		// Add categories with proper padding
		for _, cat := range categories {
			if len(cat.Commands) > 0 {
				namePadding := strings.Repeat(" ", paddingLength-len(cat.Name))
				content += fmt.Sprintf("\u001b[0;37m%s%s\u001b[30m| \u001b[0;34m%s\u001b[0m\n", 
					cat.Name, namePadding, cat.Description)
			}
		}
	}
	
	content += "```" +
		"```ansi\n" +
		"Ver" + "\u001b[30m: \u001b[34mGo Selfbot v1.0\u001b[0m\n" +
		"```"
	
	// Apply quote block formatting
	quotedContent := c.quoteBlock(content)
	
	msg, err := s.ChannelMessageSend(channelID, quotedContent)
	if err != nil {
		return err
	}
	
	// Auto-delete if configured
	if c.bot.GetConfig().AutoDelete.Enabled {
		time.AfterFunc(time.Duration(c.bot.GetConfig().AutoDelete.Delay)*time.Second, func() {
			s.ChannelMessageDelete(channelID, msg.ID)
		})
	}
	
	return nil
}

// isValidCategory checks if the given name is a valid category
func (c *SimpleHelpCommand) isValidCategory(name string) bool {
	validCategories := []string{"general", "tools", "utility", "tracking"}
	for _, cat := range validCategories {
		if cat == name {
			return true
		}
	}
	return false
}

// showCategoryHelp shows commands in a specific category
func (c *SimpleHelpCommand) showCategoryHelp(s *discordgo.Session, channelID, categoryName string) error {
	prefix := c.bot.GetConfig().CommandPrefix
	
	// Capitalize category name for display
	displayName := strings.Title(categoryName)
	
	// Get commands for this category
	var categoryCommands []SimpleCommand
	if c.commands != nil {
		seen := make(map[string]bool)
		for _, cmd := range *c.commands {
			if seen[cmd.Name()] {
				continue
			}
			seen[cmd.Name()] = true
			
			var belongsToCategory bool
			switch categoryName {
			case "general":
				belongsToCategory = cmd.Name() == "help" || cmd.Name() == "info"
			case "tools":
				belongsToCategory = cmd.Name() == "spam" || cmd.Name() == "sspam"
			case "utility":
				belongsToCategory = cmd.Name() == "ping" || cmd.Name() == "presence"
			case "tracking":
				belongsToCategory = cmd.Name() == "snipe" || cmd.Name() == "editsnipe" || cmd.Name() == "lastping"
			}
			
			if belongsToCategory {
				categoryCommands = append(categoryCommands, cmd)
			}
		}
	}
	
	// Build the message content
	content := "```ansi\n" +
		"\u001b[33m" + displayName + " \u001b[30m| \u001b[33mPage 1/1\u001b[0m\n" +
		"```" +
		"```ansi\n" +
		"\u001b[30m\u001b[1m\u001b[4mCommands\u001b[0m\n"
	
	if len(categoryCommands) == 0 {
		content += "\u001b[0;37mNo commands found in this category\u001b[0m\n"
	} else {
		// Calculate padding
		maxNameLen := 0
		for _, cmd := range categoryCommands {
			if len(cmd.Name()) > maxNameLen {
				maxNameLen = len(cmd.Name())
			}
		}
		paddingLength := maxNameLen + 2
		
		// Add commands
		for _, cmd := range categoryCommands {
			namePadding := strings.Repeat(" ", paddingLength-len(cmd.Name()))
			description := cmd.Description()
			if description == "" {
				description = "No description available"
			}
			content += fmt.Sprintf("\u001b[0;37m%s%s\u001b[30m| \u001b[0;34m%s\u001b[0m\n", 
				cmd.Name(), namePadding, description)
		}
	}
	
	content += "```" +
		"```ansi\n" +
		"\u001b[30m\u001b[0;37mNavigation \u001b[30m| \u001b[0;34m" + prefix + "help " + categoryName + " [1-1]\u001b[0m\n" +
		"```"
	
	// Apply quote block formatting
	quotedContent := c.quoteBlock(content)
	
	msg, err := s.ChannelMessageSend(channelID, quotedContent)
	if err != nil {
		return err
	}
	
	// Auto-delete if configured
	if c.bot.GetConfig().AutoDelete.Enabled {
		time.AfterFunc(time.Duration(c.bot.GetConfig().AutoDelete.Delay)*time.Second, func() {
			s.ChannelMessageDelete(channelID, msg.ID)
		})
	}
	
	return nil
}

// quoteBlock applies quote formatting like the Python version
func (c *SimpleHelpCommand) quoteBlock(text string) string {
	lines := strings.Split(text, "\n")
	quotedLines := make([]string, len(lines))
	for i, line := range lines {
		quotedLines[i] = "> " + line
	}
	return strings.Join(quotedLines, "\n")
}

func (c *SimpleHelpCommand) showCommandHelp(s *discordgo.Session, channelID, commandName string) error {
	if c.commands == nil {
		return c.sendError(s, channelID, "No commands available")
	}
	
	cmd, exists := (*c.commands)[strings.ToLower(commandName)]
	if !exists {
		return c.sendError(s, channelID, fmt.Sprintf("Command '%s' not found", commandName))
	}
	
	prefix := c.bot.GetConfig().CommandPrefix
	
	// Build the message content directly like Python version
	content := "```ansi\n" +
		"\u001b[33mCommand \u001b[30m| \u001b[33m" + prefix + cmd.Name() + "\u001b[0m\n" +
		"```" +
		"```ansi\n" +
		"\u001b[30m\u001b[1m\u001b[4mDetails\u001b[0m\n"
	
	// Calculate padding for labels
	labels := []string{"Info", "Usage", "Aliases"}
	paddingLength := 0
	for _, label := range labels {
		if len(label) > paddingLength {
			paddingLength = len(label)
		}
	}
	paddingLength += 2
	
	// Description
	description := cmd.Description()
	if description == "" {
		description = "No description available"
	}
	infoPadding := strings.Repeat(" ", paddingLength-len("Info"))
	content += fmt.Sprintf("\u001b[0;37mInfo%s\u001b[30m| \u001b[0;34m%s\u001b[0m\n", 
		infoPadding, description)
	
	// Usage
	usagePadding := strings.Repeat(" ", paddingLength-len("Usage"))
	var usage string
	switch cmd.Name() {
	case "spam":
		usage = fmt.Sprintf("%sspam <amount> <message> [flags]", prefix)
	case "snipe":
		usage = fmt.Sprintf("%ssnipe [user] [amount] [channel]", prefix)
	case "editsnipe":
		usage = fmt.Sprintf("%seditsnipe [user] [amount] [channel]", prefix)
	case "lastping":
		usage = fmt.Sprintf("%slastping [amount]", prefix)
	case "presence":
		usage = fmt.Sprintf("%spresence <status|activity|clear|show> [args]", prefix)
	default:
		usage = fmt.Sprintf("%s%s", prefix, cmd.Name())
	}
	content += fmt.Sprintf("\u001b[0;37mUsage%s\u001b[30m| \u001b[0;34m%s\u001b[0m\n", 
		usagePadding, usage)
	
	// Aliases
	if len(cmd.Aliases()) > 0 {
		aliasesPadding := strings.Repeat(" ", paddingLength-len("Aliases"))
		content += fmt.Sprintf("\u001b[0;37mAliases%s\u001b[30m| \u001b[0;34m%s\u001b[0m\n", 
			aliasesPadding, strings.Join(cmd.Aliases(), ", "))
	}
	
	content += "```" +
		"```ansi\n" +
		"Ver\u001b[30m: \u001b[34mGo Selfbot v1.0\u001b[0m\n" +
		"```"
	
	// Apply quote block formatting
	quotedContent := c.quoteBlock(content)
	
	msg, err := s.ChannelMessageSend(channelID, quotedContent)
	if err != nil {
		return err
	}
	
	// Auto-delete if configured
	if c.bot.GetConfig().AutoDelete.Enabled {
		time.AfterFunc(time.Duration(c.bot.GetConfig().AutoDelete.Delay)*time.Second, func() {
			s.ChannelMessageDelete(channelID, msg.ID)
		})
	}
	
	return nil
}

func (c *SimpleHelpCommand) sendError(s *discordgo.Session, channelID, message string) error {
	msg, err := s.ChannelMessageSend(channelID, "❌ "+message)
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

// formatDuration formats a duration into a human-readable string
func formatDuration(d time.Duration) string {
	if d < time.Minute {
		return fmt.Sprintf("%.0fs", d.Seconds())
	}
	if d < time.Hour {
		minutes := int(d.Minutes())
		seconds := int(d.Seconds()) % 60
		return fmt.Sprintf("%dm %ds", minutes, seconds)
	}
	if d < 24*time.Hour {
		hours := int(d.Hours())
		minutes := int(d.Minutes()) % 60
		return fmt.Sprintf("%dh %dm", hours, minutes)
	}
	days := int(d.Hours()) / 24
	hours := int(d.Hours()) % 24
	return fmt.Sprintf("%dd %dh", days, hours)
}