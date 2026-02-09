package bot

import (
	"context"
	"fmt"
	"sync"
	"time"

	log "github.com/sirupsen/logrus"
)

// RestartManager handles automatic restart of bots when they crash
type RestartManager struct {
	bot           *SimpleBot
	ctx           context.Context
	cancel        context.CancelFunc
	restartCount  int
	maxRestarts   int
	restartDelay  time.Duration
	mu            sync.Mutex
	isRunning     bool
}

// NewRestartManager creates a new restart manager for a bot
func NewRestartManager(bot *SimpleBot, maxRestarts int) *RestartManager {
	ctx, cancel := context.WithCancel(context.Background())
	return &RestartManager{
		bot:          bot,
		ctx:          ctx,
		cancel:       cancel,
		maxRestarts:  maxRestarts,
		restartDelay: 5 * time.Second,
	}
}

// StartWithRestart starts the bot with automatic restart capability
func (rm *RestartManager) StartWithRestart() error {
	rm.mu.Lock()
	defer rm.mu.Unlock()
	
	if rm.isRunning {
		return nil
	}
	
	rm.isRunning = true
	go rm.monitorAndRestart()
	
	return nil
}

// monitorAndRestart monitors the bot and restarts it if it crashes
func (rm *RestartManager) monitorAndRestart() {
	defer func() {
		rm.mu.Lock()
		rm.isRunning = false
		rm.mu.Unlock()
	}()
	
	for rm.restartCount < rm.maxRestarts {
		select {
		case <-rm.ctx.Done():
			log.Info("Restart manager stopping due to context cancellation")
			return
		default:
		}
		
		log.Infof("Starting bot (attempt %d/%d)", rm.restartCount+1, rm.maxRestarts+1)
		
		// Create a new context for this bot instance
		botCtx, botCancel := context.WithCancel(rm.ctx)
		
		// Start the bot in a goroutine to catch panics
		startDone := make(chan error, 1)
		go func() {
			defer func() {
				if r := recover(); r != nil {
					log.Errorf("Bot panicked during startup: %v", r)
					startDone <- fmt.Errorf("bot panicked: %v", r)
				}
			}()
			
			err := rm.bot.Start(botCtx)
			startDone <- err
		}()
		
		// Wait for the bot to start or fail
		select {
		case err := <-startDone:
			if err != nil {
				log.Errorf("Bot failed to start: %v", err)
				botCancel()
				rm.restartCount++
				
				if rm.restartCount < rm.maxRestarts {
					log.Infof("Restarting bot in %v (attempt %d/%d)", rm.restartDelay, rm.restartCount+1, rm.maxRestarts+1)
					time.Sleep(rm.restartDelay)
					continue
				} else {
					log.Errorf("Bot failed to start after %d attempts, giving up", rm.maxRestarts+1)
					return
				}
			}
			
			// Bot started successfully, reset restart count
			rm.restartCount = 0
			log.Info("Bot started successfully - running indefinitely until stopped")
			
			// Wait for the bot to stop naturally or for shutdown signal
			select {
			case <-rm.ctx.Done():
				log.Info("Restart manager received shutdown signal")
				botCancel()
				return
			}
			
		case <-rm.ctx.Done():
			log.Info("Restart manager stopping during bot startup")
			botCancel()
			return
		}
		
		// Bot stopped, prepare for restart
		log.Warn("Bot stopped unexpectedly, preparing to restart")
		rm.restartCount++
		
		if rm.restartCount < rm.maxRestarts {
			log.Infof("Restarting bot in %v (attempt %d/%d)", rm.restartDelay, rm.restartCount+1, rm.maxRestarts+1)
			time.Sleep(rm.restartDelay)
		} else {
			log.Errorf("Bot stopped after %d restarts, giving up", rm.maxRestarts)
			return
		}
	}
}

// Stop stops the restart manager and the bot
func (rm *RestartManager) Stop() error {
	rm.cancel()
	
	// Give the bot a chance to stop gracefully
	time.Sleep(2 * time.Second)
	
	return rm.bot.Stop()
}

// GetRestartCount returns the current restart count
func (rm *RestartManager) GetRestartCount() int {
	rm.mu.Lock()
	defer rm.mu.Unlock()
	return rm.restartCount
}

// IsRunning returns whether the restart manager is currently running
func (rm *RestartManager) IsRunning() bool {
	rm.mu.Lock()
	defer rm.mu.Unlock()
	return rm.isRunning
}