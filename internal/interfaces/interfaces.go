package interfaces

import (
	"selfbot/internal/config"
	"selfbot/internal/database"

	"github.com/LightningDev1/discordgo"
)

// BotInterface defines what commands need from a bot
type BotInterface interface {
	GetSession() *discordgo.Session
	GetConfig() *config.Config
	GetUserID() string
	GetUsername() string
	GetDatabase() *database.SimpleDatabase
}