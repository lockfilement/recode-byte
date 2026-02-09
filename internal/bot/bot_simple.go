package bot

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"time"

	"selfbot/internal/config"
	"selfbot/internal/database"

	"github.com/LightningDev1/discordgo"
	log "github.com/sirupsen/logrus"
)


// SimpleBot represents a Discord selfbot instance with clean, idiomatic Go patterns
type SimpleBot struct {
	config   *config.Config
	database *database.SimpleDatabase
	session  *discordgo.Session
	
	token    string
	userID   string
	username string
	index    int
	
	// Simple state management
	mu        sync.RWMutex
	isReady   bool
	startTime time.Time
	
	// Context for shutdown
	ctx    context.Context
	cancel context.CancelFunc
	
	// Simple channel cache
	channelCache sync.Map
	
	// Command handler will be set after creation to avoid import cycles
	commandHandler interface {
		Handle(s *discordgo.Session, m *discordgo.MessageCreate)
	}
}

// ChannelInfo for caching channel data
type SimpleChannelInfo struct {
	Name      string
	Type      string
	IsGroup   bool
	GuildID   string
	GuildName string
}

// NewSimpleBot creates a new bot instance with clean patterns
func NewSimpleBot(cfg *config.Config, db *database.SimpleDatabase, token string, index int) *SimpleBot {
	ctx, cancel := context.WithCancel(context.Background())
	
	return &SimpleBot{
		config:    cfg,
		database:  db,
		token:     token,
		index:     index,
		ctx:       ctx,
		cancel:    cancel,
		startTime: time.Now(),
	}
}

// SetCommandHandler sets the command handler (called after creation to avoid import cycles)
func (b *SimpleBot) SetCommandHandler(handler interface {
	Handle(s *discordgo.Session, m *discordgo.MessageCreate)
}) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.commandHandler = handler
}

// Start initializes and starts the bot
func (b *SimpleBot) Start(ctx context.Context) error {
	log.Infof("Starting bot instance %d...", b.index)

	session, err := discordgo.New(b.token)
	if err != nil {
		return fmt.Errorf("failed to create Discord session: %w", err)
	}

	b.mu.Lock()
	b.session = session
	b.mu.Unlock()

	// Configure session for LightningDev1/discordgo selfbot library
	session.StateEnabled = false                    // Disable state to avoid parsing panics
	session.ShouldReconnectOnError = true          // Enable auto-reconnection for stability
	session.MaxRestRetries = 3                     // Limit REST API retries
	
	// User account specific settings - mimic a real browser
	session.UserAgent = "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:102.0) Gecko/20100101 Firefox/102.0"
	
	// Since state is disabled, we'll handle channel/guild info manually when needed

	// Add event handlers
	b.addEventHandlers()

	// Open connection with retry logic and selfbot-appropriate delays
	var openErr error
	for attempt := 1; attempt <= 3; attempt++ {
		log.Debugf("Connection attempt %d/3", attempt)
		
		openErr = session.Open()
		if openErr == nil {
			break
		}
		
		log.Warnf("Connection attempt %d failed: %v", attempt, openErr)
		if attempt < 3 {
			// Use longer delays for user accounts to avoid rate limits
			delay := time.Duration(attempt) * 5 * time.Second
			log.Infof("Waiting %v before retry (user account rate limit considerations)", delay)
			time.Sleep(delay)
		}
	}
	
	if openErr != nil {
		return fmt.Errorf("failed to open Discord connection after 3 attempts: %w", openErr)
	}

	// Wait for ready with timeout
	readyTimeout := time.NewTimer(30 * time.Second)
	defer readyTimeout.Stop()

	for {
		select {
		case <-ctx.Done():
			return fmt.Errorf("bot startup cancelled")
		case <-readyTimeout.C:
			b.mu.RLock()
			ready := b.isReady
			b.mu.RUnlock()
			if !ready {
				return fmt.Errorf("timeout waiting for bot to be ready")
			}
			return nil
		case <-time.After(100 * time.Millisecond):
			b.mu.RLock()
			ready := b.isReady
			b.mu.RUnlock()
			if ready {
				log.Infof("Bot instance %d started successfully", b.index)
				return nil
			}
		}
	}
}

// Stop gracefully stops the bot
func (b *SimpleBot) Stop() error {
	log.Infof("Stopping bot instance %d...", b.index)
	
	b.cancel()
	
	b.mu.Lock()
	session := b.session
	b.isReady = false
	b.mu.Unlock()
	
	if session != nil {
		return session.Close()
	}
	
	return nil
}

// addEventHandlers sets up Discord event handlers with clean patterns
func (b *SimpleBot) addEventHandlers() {
	s := b.session

	// Ready event
	s.AddHandler(func(s *discordgo.Session, r *discordgo.Ready) {
		defer func() {
			if rec := recover(); rec != nil {
				log.Errorf("Recovered from panic in ready handler: %v", rec)
			}
		}()
		
		b.mu.Lock()
		if r.User != nil {
			b.userID = r.User.ID
			b.username = r.User.Username
		}
		b.isReady = true
		b.mu.Unlock()
		
		if r.User != nil {
			log.Infof("Bot %d ready as %s (%s)", b.index, r.User.Username, r.User.ID)
		} else {
			log.Infof("Bot %d ready (user info unavailable)", b.index)
		}
		b.updatePresence()
	})

	// Message events - direct processing, no complex queuing
	s.AddHandler(func(s *discordgo.Session, m *discordgo.MessageCreate) {
		defer func() {
			if r := recover(); r != nil {
				log.Errorf("Recovered from panic in message handler: %v", r)
			}
		}()
		
		if m.Message == nil || m.Message.Author == nil {
			return
		}
		
		// Process message directly
		go func() {
			defer func() {
				if r := recover(); r != nil {
					log.Errorf("Recovered from panic in processMessage: %v", r)
				}
			}()
			b.processMessage(m.Message)
		}()
		
		// Handle commands
		b.mu.RLock()
		userID := b.userID
		b.mu.RUnlock()
		
		if userID != "" && m.Author.ID == userID && strings.HasPrefix(m.Content, b.config.CommandPrefix) {
			// Use command handler if available
			b.mu.RLock()
			handler := b.commandHandler
			b.mu.RUnlock()
			
			if handler != nil {
				go func() {
					defer func() {
						if r := recover(); r != nil {
							log.Errorf("Recovered from panic in command handler: %v", r)
						}
					}()
					handler.Handle(s, m)
				}()
			}
		}
	})

	s.AddHandler(func(s *discordgo.Session, m *discordgo.MessageDelete) {
		defer func() {
			if r := recover(); r != nil {
				log.Errorf("Recovered from panic in delete handler: %v", r)
			}
		}()
		
		if m.Message == nil {
			return
		}
		go func() {
			defer func() {
				if r := recover(); r != nil {
					log.Errorf("Recovered from panic in processDeletedMessage: %v", r)
				}
			}()
			b.processDeletedMessage(m.Message)
		}()
	})

	s.AddHandler(func(s *discordgo.Session, m *discordgo.MessageUpdate) {
		defer func() {
			if r := recover(); r != nil {
				log.Errorf("Recovered from panic in update handler: %v", r)
			}
		}()
		
		if m.Message == nil {
			return
		}
		go func() {
			defer func() {
				if r := recover(); r != nil {
					log.Errorf("Recovered from panic in processEditedMessage: %v", r)
				}
			}()
			b.processEditedMessage(m.BeforeUpdate, m.Message)
		}()
	})
}

// processMessage handles incoming messages with simple, direct approach
func (b *SimpleBot) processMessage(m *discordgo.Message) {
	if m.Author.Bot {
		return
	}

	b.mu.RLock()
	userID := b.userID
	b.mu.RUnlock()
	
	if userID == "" {
		return
	}

	// Create simple message data
	msgData := &database.SimpleMessageData{
		ID:         m.ID,
		MessageID:  m.ID,
		UserID:     m.Author.ID,
		Username:   m.Author.Username,
		Content:    m.Content,
		CreatedAt:  time.Now(),
		ChannelID:  m.ChannelID,
		InstanceID: userID,
		IsSelf:     m.Author.ID == userID,
	}

	// Add channel info
	if channelInfo := b.getChannelInfo(m.ChannelID); channelInfo != nil {
		msgData.ChannelName = channelInfo.Name
		msgData.ChannelType = channelInfo.Type
		msgData.IsGroup = channelInfo.IsGroup
		msgData.GuildID = channelInfo.GuildID
		msgData.GuildName = channelInfo.GuildName
	}

	// Add attachments
	if len(m.Attachments) > 0 {
		msgData.Attachments = make([]string, len(m.Attachments))
		for i, attachment := range m.Attachments {
			msgData.Attachments[i] = attachment.ProxyURL
		}
	}

	// Store directly - simple and effective
	if err := b.database.StoreMessage(msgData); err != nil {
		log.Errorf("Bot %d failed to store message: %v", b.index, err)
	}

	// Handle mentions directly
	if b.isUserMentioned(m) && m.Author.ID != userID {
		b.processMention(m)
	}
}

// processDeletedMessage handles deleted messages simply
func (b *SimpleBot) processDeletedMessage(m *discordgo.Message) {
	if m == nil || m.Author == nil || m.Author.Bot {
		return
	}

	b.mu.RLock()
	userID := b.userID
	b.mu.RUnlock()
	
	if userID == "" || m.Author.ID == userID {
		return
	}

	msgData := &database.SimpleDeletedMessageData{
		MessageID: m.ID,
		UserID:    m.Author.ID,
		Username:  m.Author.Username,
		Content:   m.Content,
		DeletedAt: time.Now(),
		ChannelID: m.ChannelID,
	}

	// Add channel info
	if channelInfo := b.getChannelInfo(m.ChannelID); channelInfo != nil {
		msgData.ChannelName = channelInfo.Name
		msgData.ChannelType = channelInfo.Type
		msgData.IsGroup = channelInfo.IsGroup
		msgData.GuildID = channelInfo.GuildID
		msgData.GuildName = channelInfo.GuildName
	}

	// Add attachments
	if len(m.Attachments) > 0 {
		msgData.Attachments = make([]string, len(m.Attachments))
		for i, attachment := range m.Attachments {
			msgData.Attachments[i] = attachment.ProxyURL
		}
	}

	if err := b.database.StoreDeletedMessage(msgData); err != nil {
		log.Errorf("Bot %d failed to store deleted message: %v", b.index, err)
	}
}

// processEditedMessage handles edited messages
func (b *SimpleBot) processEditedMessage(before, after *discordgo.Message) {
	if before == nil || after == nil || after.Author == nil || after.Author.Bot {
		return
	}

	b.mu.RLock()
	userID := b.userID
	b.mu.RUnlock()
	
	if userID == "" || after.Author.ID == userID {
		return
	}

	if before.Content == after.Content {
		return
	}

	msgData := &database.SimpleEditedMessageData{
		MessageID:     after.ID,
		UserID:        after.Author.ID,
		Username:      after.Author.Username,
		BeforeContent: before.Content,
		AfterContent:  after.Content,
		EditedAt:      time.Now(),
		ChannelID:     after.ChannelID,
	}

	// Add channel info
	if channelInfo := b.getChannelInfo(after.ChannelID); channelInfo != nil {
		msgData.ChannelName = channelInfo.Name
		msgData.ChannelType = channelInfo.Type
		msgData.IsGroup = channelInfo.IsGroup
		msgData.GuildID = channelInfo.GuildID
		msgData.GuildName = channelInfo.GuildName
	}

	// Add attachments
	if len(before.Attachments) > 0 {
		msgData.BeforeAttachments = make([]string, len(before.Attachments))
		for i, attachment := range before.Attachments {
			msgData.BeforeAttachments[i] = attachment.ProxyURL
		}
	}
	if len(after.Attachments) > 0 {
		msgData.AfterAttachments = make([]string, len(after.Attachments))
		for i, attachment := range after.Attachments {
			msgData.AfterAttachments[i] = attachment.ProxyURL
		}
	}

	if err := b.database.StoreEditedMessage(msgData); err != nil {
		log.Errorf("Bot %d failed to store edited message: %v", b.index, err)
	}
}

// processMention handles mentions
func (b *SimpleBot) processMention(m *discordgo.Message) {
	b.mu.RLock()
	userID := b.userID
	b.mu.RUnlock()
	
	if userID == "" {
		return
	}

	mentionData := &database.SimpleMentionData{
		MessageID:  m.ID,
		AuthorID:   m.Author.ID,
		AuthorName: m.Author.Username,
		Content:    m.Content,
		CreatedAt:  time.Now(),
		ChannelID:  m.ChannelID,
		TargetID:   userID,
	}

	// Add channel info and map to channel type int
	if channelInfo := b.getChannelInfo(m.ChannelID); channelInfo != nil {
		mentionData.ChannelName = channelInfo.Name
		mentionData.GuildID = channelInfo.GuildID
		mentionData.GuildName = channelInfo.GuildName
		mentionData.IsGroup = channelInfo.IsGroup
		
		switch channelInfo.Type {
		case "DMs":
			mentionData.ChannelType = 1
		case "group":
			mentionData.ChannelType = 3
		default:
			mentionData.ChannelType = 0
		}
	}

	// Add attachments
	if len(m.Attachments) > 0 {
		mentionData.Attachments = make([]string, len(m.Attachments))
		for i, attachment := range m.Attachments {
			mentionData.Attachments[i] = attachment.ProxyURL
		}
	}

	if err := b.database.StoreMention(mentionData); err != nil {
		log.Errorf("Bot %d failed to store mention: %v", b.index, err)
	}
}

// getChannelInfo retrieves and caches channel information
func (b *SimpleBot) getChannelInfo(channelID string) *SimpleChannelInfo {
	// Check cache first
	if cached, ok := b.channelCache.Load(channelID); ok {
		if channelInfo, ok := cached.(*SimpleChannelInfo); ok {
			return channelInfo
		}
	}

	// Create default channel info in case API calls fail
	channelInfo := &SimpleChannelInfo{
		Name: "Unknown Channel",
		Type: "text",
	}

	// Try to get channel info with comprehensive error handling
	func() {
		defer func() {
			if r := recover(); r != nil {
				log.Debugf("Recovered from panic getting channel info for %s: %v", channelID, r)
			}
		}()

		b.mu.RLock()
		session := b.session
		b.mu.RUnlock()
		
		if session == nil {
			return
		}

		channel, err := session.Channel(channelID)
		if err != nil {
			log.Debugf("Failed to get channel info for %s: %v", channelID, err)
			return
		}

		if channel.Name != "" {
			channelInfo.Name = channel.Name
		}

		// Set channel type
		switch channel.Type {
		case discordgo.ChannelTypeDM:
			channelInfo.Type = "DMs"
		case discordgo.ChannelTypeGroupDM:
			channelInfo.Type = "group"
			channelInfo.IsGroup = true
		default:
			channelInfo.Type = "text"
		}

		// Only try to get guild info if we have a guild ID and it's not a DM
		if channel.GuildID != "" && channel.Type != discordgo.ChannelTypeDM && channel.Type != discordgo.ChannelTypeGroupDM {
			channelInfo.GuildID = channel.GuildID
			
			// Use a simplified approach for guild names to avoid the parsing issues
			// For now, just use the guild ID as a fallback if we can't get the name safely
			channelInfo.GuildName = "Server-" + channel.GuildID[:8] // Use first 8 chars of guild ID
			
			// Try to get the actual guild name, but don't fail if it doesn't work
			go func() {
				defer func() {
					if r := recover(); r != nil {
						log.Debugf("Recovered from panic getting guild name for %s: %v", channel.GuildID, r)
					}
				}()
				
				// This runs in background so it won't block or crash the main flow
				if guild, err := session.Guild(channel.GuildID); err == nil && guild != nil && guild.Name != "" {
					// Update the cached channel info with the real guild name
					channelInfo.GuildName = guild.Name
					b.channelCache.Store(channelID, channelInfo)
				}
			}()
		}
	}()

	// Cache and return
	b.channelCache.Store(channelID, channelInfo)
	return channelInfo
}

// isUserMentioned checks if the user is mentioned
func (b *SimpleBot) isUserMentioned(m *discordgo.Message) bool {
	b.mu.RLock()
	userID := b.userID
	b.mu.RUnlock()
	
	if userID == "" {
		return false
	}

	// Check direct mentions
	for _, user := range m.Mentions {
		if user.ID == userID {
			return true
		}
	}

	// Check if it's a reply to our message
	if m.MessageReference != nil && m.MessageReference.MessageID != "" {
		b.mu.RLock()
		session := b.session
		b.mu.RUnlock()
		
		if session != nil {
			if refMsg, err := session.ChannelMessage(m.ChannelID, m.MessageReference.MessageID); err == nil {
				if refMsg.Author.ID == userID {
					return true
				}
			}
		}
	}

	return false
}


// updatePresence updates the bot's presence
func (b *SimpleBot) updatePresence() {
	if !b.config.Presence.Enabled {
		return
	}

	var activity *discordgo.Activity
	if b.config.Presence.Name != "" {
		activity = &discordgo.Activity{
			Name: b.config.Presence.Name,
			Type: discordgo.ActivityType(b.config.Presence.Type),
		}

		if b.config.Presence.State != "" {
			activity.State = b.config.Presence.State
		}

		if b.config.Presence.Details != "" {
			activity.Details = b.config.Presence.Details
		}
	}

	var status discordgo.Status = discordgo.StatusDoNotDisturb
	if b.config.Presence.Status != "" {
		switch strings.ToLower(b.config.Presence.Status) {
		case "online":
			status = discordgo.StatusOnline
		case "idle":
			status = discordgo.StatusIdle
		case "dnd", "do not disturb":
			status = discordgo.StatusDoNotDisturb
		case "invisible":
			status = discordgo.StatusInvisible
		}
	}

	var activities []*discordgo.Activity
	if activity != nil {
		activities = append(activities, activity)
	}

	if err := b.session.UpdateStatusComplex(discordgo.UpdateStatusData{
		Status:     string(status),
		Activities: activities,
		AFK:        true,
	}); err != nil {
		log.Errorf("Failed to update presence: %v", err)
	}
}

// Interface implementation methods
func (b *SimpleBot) GetSession() *discordgo.Session {
	b.mu.RLock()
	defer b.mu.RUnlock()
	return b.session
}

func (b *SimpleBot) GetConfig() *config.Config {
	return b.config
}

func (b *SimpleBot) GetUserID() string {
	b.mu.RLock()
	defer b.mu.RUnlock()
	return b.userID
}

func (b *SimpleBot) GetUsername() string {
	b.mu.RLock()
	defer b.mu.RUnlock()
	return b.username
}

func (b *SimpleBot) GetDatabase() *database.SimpleDatabase {
	return b.database
}