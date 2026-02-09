package database

import (
	"context"
	"sync"
	"time"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"selfbot/internal/config"

	log "github.com/sirupsen/logrus"
)

// Database represents the database connection with optimized operations
type Database struct {
	client     *mongo.Client
	db         *mongo.Database
	config     *config.Database
	mu         sync.RWMutex
	collections map[string]*mongo.Collection
	
	// Connection pool and performance optimizations
	writeOptions *options.BulkWriteOptions
	readOptions  *options.FindOptions
	
	// Batch processing channels for high-throughput operations
	messageBatch    chan *MessageData
	deletedBatch    chan *DeletedMessageData
	editedBatch     chan *EditedMessageData
	mentionBatch    chan *MentionData
	
	// Batch processing control
	batchSize     int
	flushInterval time.Duration
	ctx           context.Context
	cancel        context.CancelFunc
	wg            sync.WaitGroup
}

// Message data structures optimized with pointers and proper types
type MessageData struct {
	ID          string                 `bson:"_id"`
	MessageID   string                 `bson:"message_id"`
	UserID      string                 `bson:"user_id"`
	Username    string                 `bson:"username"`
	Content     string                 `bson:"content"`
	CreatedAt   time.Time              `bson:"created_at"`
	ChannelID   string                 `bson:"channel_id"`
	ChannelName *string                `bson:"channel_name,omitempty"` // Pointer for optional fields
	ChannelType *string                `bson:"channel_type,omitempty"`
	IsGroup     bool                   `bson:"is_group"`
	Attachments []string               `bson:"attachments,omitempty"`
	InstanceID  string                 `bson:"instance_id"`
	IsSelf      bool                   `bson:"is_self"`
	GuildID     *string                `bson:"guild_id,omitempty"`
	GuildName   *string                `bson:"guild_name,omitempty"`
	ReplyTo     *ReplyInfo             `bson:"reply_to,omitempty"`
	Snapshots   []*MessageSnapshot     `bson:"snapshots,omitempty"`
}

type DeletedMessageData struct {
	MessageID   string             `bson:"message_id"`
	UserID      string             `bson:"user_id"`
	Username    *string            `bson:"username,omitempty"`
	Content     string             `bson:"content"`
	DeletedAt   time.Time          `bson:"deleted_at"`
	ChannelID   string             `bson:"channel_id"`
	ChannelName *string            `bson:"channel_name,omitempty"`
	ChannelType *string            `bson:"channel_type,omitempty"`
	IsGroup     bool               `bson:"is_group"`
	Attachments []string           `bson:"attachments,omitempty"`
	GuildID     *string            `bson:"guild_id,omitempty"`
	GuildName   *string            `bson:"guild_name,omitempty"`
	ReplyTo     *ReplyInfo         `bson:"reply_to,omitempty"`
	Snapshots   []*MessageSnapshot `bson:"snapshots,omitempty"`
}

type EditedMessageData struct {
	MessageID         string             `bson:"message_id"`
	UserID            string             `bson:"user_id"`
	Username          *string            `bson:"username,omitempty"`
	BeforeContent     string             `bson:"before_content"`
	AfterContent      string             `bson:"after_content"`
	BeforeAttachments []string           `bson:"before_attachments,omitempty"`
	AfterAttachments  []string           `bson:"after_attachments,omitempty"`
	EditedAt          time.Time          `bson:"edited_at"`
	ChannelID         string             `bson:"channel_id"`
	ChannelName       *string            `bson:"channel_name,omitempty"`
	ChannelType       *string            `bson:"channel_type,omitempty"`
	IsGroup           bool               `bson:"is_group"`
	GuildID           *string            `bson:"guild_id,omitempty"`
	GuildName         *string            `bson:"guild_name,omitempty"`
	ReplyTo           *ReplyInfo         `bson:"reply_to,omitempty"`
}

type MentionData struct {
	MessageID   string      `bson:"message_id"`
	AuthorID    string      `bson:"author_id"`
	AuthorName  string      `bson:"author_name"`
	Content     string      `bson:"content"`
	CreatedAt   time.Time   `bson:"created_at"`
	ChannelID   string      `bson:"channel_id"`
	ChannelName *string     `bson:"channel_name,omitempty"`
	Attachments []string    `bson:"attachments,omitempty"`
	ChannelType int         `bson:"channel_type"`
	IsGroup     bool        `bson:"is_group"`
	TargetID    string      `bson:"target_id"`
	GuildID     *string     `bson:"guild_id,omitempty"`
	GuildName   *string     `bson:"guild_name,omitempty"`
	ReplyTo     *ReplyInfo  `bson:"reply_to,omitempty"`
}

// Helper structures for efficient data handling
type ReplyInfo struct {
	UserID      string    `bson:"user_id"`
	Username    string    `bson:"username"`
	Content     string    `bson:"content"`
	Attachments []string  `bson:"attachments,omitempty"`
	IsSnapshot  bool      `bson:"is_snapshot,omitempty"`
}

type MessageSnapshot struct {
	ID          string    `bson:"id"`
	Content     string    `bson:"content"`
	CreatedAt   time.Time `bson:"created_at"`
	Attachments []string  `bson:"attachments,omitempty"`
	AuthorID    *string   `bson:"author_id,omitempty"`
	AuthorName  *string   `bson:"author_name,omitempty"`
}

// QueryFilter represents database query filters with optimized types
type QueryFilter struct {
	UserID      *string    `bson:"user_id,omitempty"`
	ChannelID   *string    `bson:"channel_id,omitempty"`
	GuildID     *string    `bson:"guild_id,omitempty"`
	TargetID    *string    `bson:"target_id,omitempty"`
	ExcludeUser *string    `bson:"user_id,omitempty"` // For $ne operations
	TimeRange   *TimeRange `bson:",inline,omitempty"`
}

type TimeRange struct {
	After  *time.Time `bson:"$gte,omitempty"`
	Before *time.Time `bson:"$lte,omitempty"`
}

// New creates an optimized database connection with batch processing
func New(cfg *config.Database) (*Database, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	// Enhanced connection options for performance
	clientOptions := options.Client().
		ApplyURI(cfg.URI).
		SetMaxPoolSize(100). // Increased pool size
		SetMinPoolSize(10).
		SetMaxConnIdleTime(30 * time.Second).
		SetServerSelectionTimeout(5 * time.Second).
		SetSocketTimeout(10 * time.Second)

	client, err := mongo.Connect(ctx, clientOptions)
	if err != nil {
		return nil, err
	}

	if err := client.Ping(ctx, nil); err != nil {
		return nil, err
	}

	// Create database instance with optimizations
	dbCtx, dbCancel := context.WithCancel(context.Background())
	db := &Database{
		client:      client,
		db:          client.Database(cfg.Name),
		config:      cfg,
		collections: make(map[string]*mongo.Collection),
		
		// Optimized batch settings
		batchSize:     1000,  // Process in larger batches
		flushInterval: 5 * time.Second,
		ctx:           dbCtx,
		cancel:        dbCancel,
		
		// Pre-configured options for better performance
		writeOptions: options.BulkWrite().SetOrdered(false), // Unordered for speed
		readOptions:  options.Find().SetBatchSize(100),
		
		// Batch channels with appropriate buffer sizes
		messageBatch: make(chan *MessageData, 2000),
		deletedBatch: make(chan *DeletedMessageData, 1000),
		editedBatch:  make(chan *EditedMessageData, 1000),
		mentionBatch: make(chan *MentionData, 500),
	}

	// Pre-create collection references for faster access
	db.collections["user_messages"] = db.db.Collection("user_messages")
	db.collections["deleted_messages"] = db.db.Collection("deleted_messages")
	db.collections["edited_messages"] = db.db.Collection("edited_messages")
	db.collections["mentions"] = db.db.Collection("mentions")

	// Start batch processors
	db.startBatchProcessors()

	log.Info("Connected to MongoDB with optimized configuration")
	return db, nil
}

// startBatchProcessors starts goroutines for efficient batch processing
func (d *Database) startBatchProcessors() {
	d.wg.Add(4)
	
	go d.processBatch("user_messages", d.messageBatch)
	go d.processBatch("deleted_messages", d.deletedBatch)
	go d.processBatch("edited_messages", d.editedBatch)
	go d.processBatch("mentions", d.mentionBatch)
}

// processBatch handles batch processing for any message type using type switching
func (d *Database) processBatch(collectionName string, batchChan interface{}) {
	defer d.wg.Done()
	
	ticker := time.NewTicker(d.flushInterval)
	defer ticker.Stop()
	
	var batch []interface{}
	
	for {
		select {
		case <-d.ctx.Done():
			// Flush remaining batch before exiting
			if len(batch) > 0 {
				d.flushBatch(collectionName, batch)
			}
			return
			
		case <-ticker.C:
			// Periodic flush
			if len(batch) > 0 {
				d.flushBatch(collectionName, batch)
				batch = batch[:0] // Reset slice but keep capacity
			}
			
		default:
			// Process based on channel type
			switch ch := batchChan.(type) {
			case chan *MessageData:
				select {
				case msg := <-ch:
					batch = append(batch, msg)
				case <-time.After(10 * time.Millisecond): // Small timeout to prevent blocking
					continue
				}
			case chan *DeletedMessageData:
				select {
				case msg := <-ch:
					batch = append(batch, msg)
				case <-time.After(10 * time.Millisecond):
					continue
				}
			case chan *EditedMessageData:
				select {
				case msg := <-ch:
					batch = append(batch, msg)
				case <-time.After(10 * time.Millisecond):
					continue
				}
			case chan *MentionData:
				select {
				case msg := <-ch:
					batch = append(batch, msg)
				case <-time.After(10 * time.Millisecond):
					continue
				}
			}
			
			// Flush when batch is full
			if len(batch) >= d.batchSize {
				d.flushBatch(collectionName, batch)
				batch = batch[:0]
			}
		}
	}
}

// flushBatch efficiently writes a batch to MongoDB
func (d *Database) flushBatch(collectionName string, batch []interface{}) {
	if len(batch) == 0 {
		return
	}
	
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	
	collection := d.getCollection(collectionName)
	
	// Use bulk operations for maximum efficiency
	_, err := collection.InsertMany(ctx, batch, options.InsertMany().SetOrdered(false))
	if err != nil {
		// Handle bulk write errors gracefully
		if bulkErr, ok := err.(mongo.BulkWriteException); ok {
			// Log only non-duplicate errors
			nonDuplicates := 0
			for _, writeErr := range bulkErr.WriteErrors {
				if writeErr.Code != 11000 { // Not a duplicate key error
					nonDuplicates++
				}
			}
			if nonDuplicates > 0 {
				log.Errorf("Bulk write failed for %s: %d non-duplicate errors", collectionName, nonDuplicates)
			}
		} else {
			log.Errorf("Failed to insert batch for %s: %v", collectionName, err)
		}
	} else {
		log.Debugf("Successfully inserted %d documents to %s", len(batch), collectionName)
	}
}

// Optimized storage methods using batch channels
func (d *Database) StoreMessage(msg *MessageData) {
	select {
	case d.messageBatch <- msg:
		// Successfully queued
	default:
		// Channel full, log warning but don't block
		log.Warn("Message batch channel full, dropping message")
	}
}

func (d *Database) StoreDeletedMessage(msg *DeletedMessageData) {
	select {
	case d.deletedBatch <- msg:
	default:
		log.Warn("Deleted message batch channel full, dropping message")
	}
}

func (d *Database) StoreEditedMessage(msg *EditedMessageData) {
	select {
	case d.editedBatch <- msg:
	default:
		log.Warn("Edited message batch channel full, dropping message")
	}
}

func (d *Database) StoreMention(mention *MentionData) {
	select {
	case d.mentionBatch <- mention:
	default:
		log.Warn("Mention batch channel full, dropping message")
	}
}

// Optimized query methods with proper pointer usage and caching
func (d *Database) GetDeletedMessages(filter *QueryFilter, limit int64) ([]*DeletedMessageData, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	collection := d.getCollection("deleted_messages")
	
	// Build optimized query
	query := d.buildQuery(filter)
	opts := options.Find().
		SetSort(bson.D{{"deleted_at", -1}}).
		SetLimit(limit).
		SetBatchSize(int32(min(limit, 100)))

	cursor, err := collection.Find(ctx, query, opts)
	if err != nil {
		return nil, err
	}
	defer cursor.Close(ctx)

	// Pre-allocate slice with known capacity for efficiency
	messages := make([]*DeletedMessageData, 0, limit)
	
	for cursor.Next(ctx) {
		var msg DeletedMessageData
		if err := cursor.Decode(&msg); err != nil {
			log.Errorf("Failed to decode deleted message: %v", err)
			continue
		}
		messages = append(messages, &msg)
	}

	return messages, cursor.Err()
}

func (d *Database) GetEditedMessages(filter *QueryFilter, limit int64) ([]*EditedMessageData, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	collection := d.getCollection("edited_messages")
	query := d.buildQuery(filter)
	opts := options.Find().
		SetSort(bson.D{{"edited_at", -1}}).
		SetLimit(limit).
		SetBatchSize(int32(min(limit, 100)))

	cursor, err := collection.Find(ctx, query, opts)
	if err != nil {
		return nil, err
	}
	defer cursor.Close(ctx)

	messages := make([]*EditedMessageData, 0, limit)
	
	for cursor.Next(ctx) {
		var msg EditedMessageData
		if err := cursor.Decode(&msg); err != nil {
			log.Errorf("Failed to decode edited message: %v", err)
			continue
		}
		messages = append(messages, &msg)
	}

	return messages, cursor.Err()
}

func (d *Database) GetMentions(filter *QueryFilter, limit int64) ([]*MentionData, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	collection := d.getCollection("mentions")
	query := d.buildQuery(filter)
	opts := options.Find().
		SetSort(bson.D{{"created_at", -1}}).
		SetLimit(limit).
		SetBatchSize(int32(min(limit, 100)))

	cursor, err := collection.Find(ctx, query, opts)
	if err != nil {
		return nil, err
	}
	defer cursor.Close(ctx)

	mentions := make([]*MentionData, 0, limit)
	
	for cursor.Next(ctx) {
		var mention MentionData
		if err := cursor.Decode(&mention); err != nil {
			log.Errorf("Failed to decode mention: %v", err)
			continue
		}
		mentions = append(mentions, &mention)
	}

	return mentions, cursor.Err()
}

// buildQuery efficiently builds MongoDB queries from filter
func (d *Database) buildQuery(filter *QueryFilter) bson.M {
	query := bson.M{}
	
	if filter == nil {
		return query
	}
	
	if filter.UserID != nil {
		query["user_id"] = *filter.UserID
	}
	
	if filter.ChannelID != nil {
		query["channel_id"] = *filter.ChannelID
	}
	
	if filter.GuildID != nil {
		query["guild_id"] = *filter.GuildID
	}
	
	if filter.TargetID != nil {
		query["target_id"] = *filter.TargetID
	}
	
	if filter.ExcludeUser != nil {
		query["user_id"] = bson.M{"$ne": *filter.ExcludeUser}
	}
	
	// Add time range if specified
	if filter.TimeRange != nil {
		timeQuery := bson.M{}
		if filter.TimeRange.After != nil {
			timeQuery["$gte"] = *filter.TimeRange.After
		}
		if filter.TimeRange.Before != nil {
			timeQuery["$lte"] = *filter.TimeRange.Before
		}
		if len(timeQuery) > 0 {
			// Determine the time field based on collection context
			query["created_at"] = timeQuery
		}
	}
	
	return query
}

// getCollection returns cached collection reference
func (d *Database) getCollection(name string) *mongo.Collection {
	d.mu.RLock()
	defer d.mu.RUnlock()
	return d.collections[name]
}

// Close gracefully shuts down the database connection
func (d *Database) Close() error {
	log.Info("Shutting down database...")
	
	// Cancel context to stop batch processors
	d.cancel()
	
	// Wait for all batch processors to finish
	d.wg.Wait()
	
	// Close channels
	close(d.messageBatch)
	close(d.deletedBatch)
	close(d.editedBatch)
	close(d.mentionBatch)
	
	// Disconnect from MongoDB
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	
	return d.client.Disconnect(ctx)
}

// Helper function for min since Go doesn't have built-in
func min(a, b int64) int64 {
	if a < b {
		return a
	}
	return b
}

// Wrapper methods for command interface compatibility
func (d *Database) GetDeletedMessagesInterface(filter interface{}, limit int64) ([]interface{}, error) {
	var queryFilter *QueryFilter
	if filter != nil {
		if bsonFilter, ok := filter.(bson.M); ok {
			queryFilter = &QueryFilter{}
			// Convert bson.M to QueryFilter
			if userID, exists := bsonFilter["user_id"]; exists {
				if userIDStr, ok := userID.(string); ok {
					queryFilter.UserID = &userIDStr
				} else if userIDCondition, ok := userID.(bson.M); ok {
					if neValue, exists := userIDCondition["$ne"]; exists {
						if neStr, ok := neValue.(string); ok {
							queryFilter.ExcludeUser = &neStr
						}
					}
				}
			}
			if channelID, exists := bsonFilter["channel_id"]; exists {
				if channelIDStr, ok := channelID.(string); ok {
					queryFilter.ChannelID = &channelIDStr
				}
			}
		}
	}
	
	messages, err := d.GetDeletedMessages(queryFilter, limit)
	if err != nil {
		return nil, err
	}
	
	result := make([]interface{}, len(messages))
	for i, msg := range messages {
		result[i] = msg
	}
	return result, nil
}

func (d *Database) GetEditedMessagesInterface(filter interface{}, limit int64) ([]interface{}, error) {
	var queryFilter *QueryFilter
	if filter != nil {
		if bsonFilter, ok := filter.(bson.M); ok {
			queryFilter = &QueryFilter{}
			// Convert bson.M to QueryFilter
			if userID, exists := bsonFilter["user_id"]; exists {
				if userIDStr, ok := userID.(string); ok {
					queryFilter.UserID = &userIDStr
				} else if userIDCondition, ok := userID.(bson.M); ok {
					if neValue, exists := userIDCondition["$ne"]; exists {
						if neStr, ok := neValue.(string); ok {
							queryFilter.ExcludeUser = &neStr
						}
					}
				}
			}
			if channelID, exists := bsonFilter["channel_id"]; exists {
				if channelIDStr, ok := channelID.(string); ok {
					queryFilter.ChannelID = &channelIDStr
				}
			}
		}
	}
	
	messages, err := d.GetEditedMessages(queryFilter, limit)
	if err != nil {
		return nil, err
	}
	
	result := make([]interface{}, len(messages))
	for i, msg := range messages {
		result[i] = msg
	}
	return result, nil
}

func (d *Database) GetMentionsInterface(filter interface{}, limit int64) ([]interface{}, error) {
	var queryFilter *QueryFilter
	if filter != nil {
		if bsonFilter, ok := filter.(bson.M); ok {
			queryFilter = &QueryFilter{}
			// Convert bson.M to QueryFilter
			if targetID, exists := bsonFilter["target_id"]; exists {
				if targetIDStr, ok := targetID.(string); ok {
					queryFilter.TargetID = &targetIDStr
				}
			}
		}
	}
	
	mentions, err := d.GetMentions(queryFilter, limit)
	if err != nil {
		return nil, err
	}
	
	result := make([]interface{}, len(mentions))
	for i, mention := range mentions {
		result[i] = mention
	}
	return result, nil
}