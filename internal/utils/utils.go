package utils

import (
	"regexp"
	"strings"
	"time"
)

// FormatDuration formats a time duration into a human-readable string
func FormatDuration(start, end time.Time) string {
	if start.IsZero() || end.IsZero() {
		return "Unknown"
	}

	duration := end.Sub(start)
	days := int(duration.Hours() / 24)
	hours := int(duration.Hours()) % 24
	minutes := int(duration.Minutes()) % 60
	seconds := int(duration.Seconds()) % 60

	if days > 0 {
		return formatTime(days, "d") + " " + formatTime(hours, "h") + " " + formatTime(minutes, "m")
	} else if hours > 0 {
		return formatTime(hours, "h") + " " + formatTime(minutes, "m")
	} else if minutes > 0 {
		return formatTime(minutes, "m") + " " + formatTime(seconds, "s")
	} else {
		return formatTime(seconds, "s")
	}
}

// formatTime helper for FormatDuration
func formatTime(value int, unit string) string {
	if value > 0 {
		return string(rune(value)) + unit
	}
	return ""
}

// CleanContent removes markdown and special characters for display
func CleanContent(content string) string {
	content = strings.ReplaceAll(content, "\\", "")
	content = strings.ReplaceAll(content, "```", "")
	content = strings.ReplaceAll(content, "`", "")
	content = strings.ReplaceAll(content, "|", "")
	content = strings.ReplaceAll(content, "*", "")
	return content
}

// TruncateContent truncates content to specified length with ellipsis
func TruncateContent(content string, maxLength int) string {
	if len(content) > maxLength {
		return content[:maxLength-3] + "..."
	}
	return content
}

// ExtractUserID extracts user ID from Discord mention format
func ExtractUserID(mention string) string {
	mention = strings.Trim(mention, "<@!>")
	return mention
}

// ExtractChannelID extracts channel ID from Discord channel mention format
func ExtractChannelID(mention string) string {
	mention = strings.Trim(mention, "<#>")
	return mention
}

// IsValidEmoji checks if a string is a valid Unicode emoji
func IsValidEmoji(s string) bool {
	// Basic emoji regex pattern
	emojiPattern := regexp.MustCompile(`[\x{1F600}-\x{1F64F}]|[\x{1F300}-\x{1F5FF}]|[\x{1F680}-\x{1F6FF}]|[\x{1F1E0}-\x{1F1FF}]|[\x{2600}-\x{26FF}]|[\x{2700}-\x{27BF}]`)
	return emojiPattern.MatchString(s)
}

// GetMaxMessageLength returns Discord's message length limit
func GetMaxMessageLength() int {
	return 2000
}

// FormatMessage adds quote blocks to message content
func FormatMessage(content string) string {
	lines := strings.Split(content, "\n")
	for i, line := range lines {
		lines[i] = "> " + line
	}
	return strings.Join(lines, "\n")
}

// SanitizeFilename removes invalid characters from filenames
func SanitizeFilename(filename string) string {
	invalidChars := regexp.MustCompile(`[<>:"/\\|?*]`)
	return invalidChars.ReplaceAllString(filename, "_")
}

// ParseMentions extracts all user mentions from message content
func ParseMentions(content string) []string {
	mentionPattern := regexp.MustCompile(`<@!?(\d+)>`)
	matches := mentionPattern.FindAllStringSubmatch(content, -1)
	
	var mentions []string
	for _, match := range matches {
		if len(match) > 1 {
			mentions = append(mentions, match[1])
		}
	}
	return mentions
}

// ParseChannelMentions extracts all channel mentions from message content
func ParseChannelMentions(content string) []string {
	channelPattern := regexp.MustCompile(`<#(\d+)>`)
	matches := channelPattern.FindAllStringSubmatch(content, -1)
	
	var channels []string
	for _, match := range matches {
		if len(match) > 1 {
			channels = append(channels, match[1])
		}
	}
	return channels
}

// IsDiscordID checks if a string looks like a Discord ID (18-19 digits)
func IsDiscordID(s string) bool {
	if len(s) < 17 || len(s) > 19 {
		return false
	}
	
	for _, char := range s {
		if char < '0' || char > '9' {
			return false
		}
	}
	return true
}

// Contains checks if a slice contains a specific string
func Contains(slice []string, item string) bool {
	for _, s := range slice {
		if s == item {
			return true
		}
	}
	return false
}

// RemoveDuplicates removes duplicate strings from a slice
func RemoveDuplicates(slice []string) []string {
	seen := make(map[string]bool)
	var result []string
	
	for _, item := range slice {
		if !seen[item] {
			seen[item] = true
			result = append(result, item)
		}
	}
	return result
}

// ChunkSlice splits a slice into chunks of specified size
func ChunkSlice(slice []string, chunkSize int) [][]string {
	var chunks [][]string
	for i := 0; i < len(slice); i += chunkSize {
		end := i + chunkSize
		if end > len(slice) {
			end = len(slice)
		}
		chunks = append(chunks, slice[i:end])
	}
	return chunks
}