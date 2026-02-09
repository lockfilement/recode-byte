package config

import (
	"fmt"
	"strings"

	"github.com/spf13/viper"
)

// Config represents the application configuration
type Config struct {
	Tokens       []string     `mapstructure:"tokens"`
	DeveloperIDs []string     `mapstructure:"developer_ids"`
	CommandPrefix string      `mapstructure:"command_prefix"`
	Version      string       `mapstructure:"version"`
	Name         string       `mapstructure:"name"`
	Database     Database     `mapstructure:"database"`
	AutoDelete   AutoDelete   `mapstructure:"auto_delete"`
	Presence     Presence     `mapstructure:"presence"`
	NitroSniper  NitroSniper  `mapstructure:"nitro_sniper"`
}

// Database configuration
type Database struct {
	URI  string `mapstructure:"uri"`
	Name string `mapstructure:"name"`
}

// AutoDelete configuration
type AutoDelete struct {
	Enabled bool `mapstructure:"enabled"`
	Delay   int  `mapstructure:"delay"`
}

// Presence configuration
type Presence struct {
	Enabled       bool         `mapstructure:"enabled"`
	Name          string       `mapstructure:"name"`
	Type          int          `mapstructure:"type"`
	ApplicationID string       `mapstructure:"application_id"`
	State         string       `mapstructure:"state"`
	Details       string       `mapstructure:"details"`
	LargeImage    string       `mapstructure:"large_image"`
	SmallImage    string       `mapstructure:"small_image"`
	Button1       string       `mapstructure:"button1"`
	Button2       string       `mapstructure:"button2"`
	Status        string       `mapstructure:"status"`
	CustomStatus  CustomStatus `mapstructure:"custom_status"`
	Rotation      Rotation     `mapstructure:"rotation"`
}

// CustomStatus configuration
type CustomStatus struct {
	Text  string `mapstructure:"text"`
	Emoji Emoji  `mapstructure:"emoji"`
}

// Emoji configuration
type Emoji struct {
	ID   string `mapstructure:"id"`
	Name string `mapstructure:"name"`
}

// Rotation configuration
type Rotation struct {
	Enabled bool `mapstructure:"enabled"`
	Delay   int  `mapstructure:"delay"`
}

// NitroSniper configuration
type NitroSniper struct {
	Enabled bool         `mapstructure:"enabled"`
	Stats   NitroStats   `mapstructure:"stats"`
}

// NitroStats configuration
type NitroStats struct {
	TotalSeen       int `mapstructure:"total_seen"`
	InvalidLength   int `mapstructure:"invalid_length"`
	AlreadySeen     int `mapstructure:"already_seen"`
	AlreadyRedeemed int `mapstructure:"already_redeemed"`
	FailedRedeem    int `mapstructure:"failed_redeem"`
	SuccessfulRedeem int `mapstructure:"successful_redeem"`
	RateLimited     int `mapstructure:"rate_limited"`
}

// Load loads configuration from file
func Load(filename string) (*Config, error) {
	viper.SetConfigFile(filename)
	viper.SetConfigType("yaml")

	// Set defaults
	viper.SetDefault("command_prefix", ";")
	viper.SetDefault("version", "2.0.0")
	viper.SetDefault("name", "Selfbot")
	viper.SetDefault("database.uri", "mongodb://localhost:27017")
	viper.SetDefault("database.name", "selfbot")
	viper.SetDefault("auto_delete.enabled", true)
	viper.SetDefault("auto_delete.delay", 30)

	// Read config file
	if err := viper.ReadInConfig(); err != nil {
		return nil, fmt.Errorf("failed to read config file: %w", err)
	}

	var config Config
	if err := viper.Unmarshal(&config); err != nil {
		return nil, fmt.Errorf("failed to unmarshal config: %w", err)
	}

	// Validate required fields
	if len(config.Tokens) == 0 {
		return nil, fmt.Errorf("no tokens provided in configuration")
	}

	return &config, nil
}

// IsDeveloper checks if the given user ID is a developer
func (c *Config) IsDeveloper(userID string) bool {
	for _, devID := range c.DeveloperIDs {
		if devID == userID {
			return true
		}
	}
	return false
}

// GetRotationValues splits rotation values by periods
func GetRotationValues(value string) []string {
	if !strings.Contains(value, ".") {
		return []string{value}
	}
	return strings.Split(value, ".")
}