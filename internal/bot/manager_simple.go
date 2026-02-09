package bot

import (
	"context"
	"fmt"
	"sync"
	"time"

	"selfbot/internal/config"
	"selfbot/internal/database"

	log "github.com/sirupsen/logrus"
)

// Manager manages multiple bot instances with simple, efficient patterns
type SimpleManager struct {
	config   *config.Config
	database *database.SimpleDatabase
	bots     map[string]*SimpleBot
	mu       sync.RWMutex
}

// NewSimpleManager creates a simplified bot manager
func NewSimpleManager(cfg *config.Config, db *database.SimpleDatabase) *SimpleManager {
	return &SimpleManager{
		config:   cfg,
		database: db,
		bots:     make(map[string]*SimpleBot),
	}
}

// StartAll starts all bot instances concurrently but simply
func (m *SimpleManager) StartAll(ctx context.Context) error {
	log.Infof("Starting %d bot instances...", len(m.config.Tokens))
	
	var wg sync.WaitGroup
	errChan := make(chan error, len(m.config.Tokens))
	
	for i, token := range m.config.Tokens {
		wg.Add(1)
		go func(token string, index int) {
			defer wg.Done()
			
			if err := m.startBot(token, index); err != nil {
				errChan <- fmt.Errorf("failed to start bot %d: %w", index, err)
				return
			}
			
			// Simple staggered startup
			time.Sleep(500 * time.Millisecond)
		}(token, i)
	}
	
	go func() {
		wg.Wait()
		close(errChan)
	}()
	
	// Collect errors
	var errors []error
	for err := range errChan {
		errors = append(errors, err)
		log.Error(err)
	}
	
	successCount := len(m.config.Tokens) - len(errors)
	log.Infof("Successfully started %d/%d bot instances", successCount, len(m.config.Tokens))
	
	if len(errors) > 0 {
		return fmt.Errorf("failed to start %d bots", len(errors))
	}
	
	return nil
}

// startBot starts a single bot instance
func (m *SimpleManager) startBot(token string, index int) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	
	bot := NewSimpleBot(m.config, m.database, token, index)
	m.bots[token] = bot
	
	return bot.Start(context.Background())
}

// StopAll gracefully stops all bot instances
func (m *SimpleManager) StopAll() {
	log.Info("Shutting down all bot instances...")
	
	m.mu.RLock()
	bots := make([]*SimpleBot, 0, len(m.bots))
	for _, bot := range m.bots {
		bots = append(bots, bot)
	}
	m.mu.RUnlock()
	
	var wg sync.WaitGroup
	for _, bot := range bots {
		wg.Add(1)
		go func(b *SimpleBot) {
			defer wg.Done()
			if err := b.Stop(); err != nil {
				log.Errorf("Error stopping bot: %v", err)
			}
		}(bot)
	}
	
	// Wait with timeout
	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()
	
	select {
	case <-done:
		log.Info("All bots stopped successfully")
	case <-time.After(10 * time.Second):
		log.Warn("Timeout waiting for bots to stop")
	}
	
	m.mu.Lock()
	m.bots = make(map[string]*SimpleBot)
	m.mu.Unlock()
}

// GetBot returns a bot instance by token
func (m *SimpleManager) GetBot(token string) (*SimpleBot, bool) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	bot, exists := m.bots[token]
	return bot, exists
}

// GetBotCount returns the number of active bots
func (m *SimpleManager) GetBotCount() int {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return len(m.bots)
}