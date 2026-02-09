package database

import (
	"context"
	"time"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"selfbot/internal/config"

	log "github.com/sirupsen/logrus"
)

// SimpleDatabase provides straightforward database operations
type SimpleDatabase struct {
	client *mongo.Client
	db     *mongo.Database
}

// Simple data structures without unnecessary pointers
type SimpleMessageData struct {
	ID          string    `bson:"_id"`
	MessageID   string    `bson:"message_id"`
	UserID      string    `bson:"user_id"`
	Username    string    `bson:"username"`
	Content     string    `bson:"content"`
	CreatedAt   time.Time `bson:"created_at"`
	ChannelID   string    `bson:"channel_id"`
	ChannelName string    `bson:"channel_name,omitempty"`
	ChannelType string    `bson:"channel_type,omitempty"`
	IsGroup     bool      `bson:"is_group"`
	Attachments []string  `bson:"attachments,omitempty"`
	InstanceID  string    `bson:"instance_id"`
	IsSelf      bool      `bson:"is_self"`
	GuildID     string    `bson:"guild_id,omitempty"`
	GuildName   string    `bson:"guild_name,omitempty"`
}

type SimpleDeletedMessageData struct {
	MessageID   string    `bson:"message_id"`
	UserID      string    `bson:"user_id"`
	Username    string    `bson:"username,omitempty"`
	Content     string    `bson:"content"`
	DeletedAt   time.Time `bson:"deleted_at"`
	ChannelID   string    `bson:"channel_id"`
	ChannelName string    `bson:"channel_name,omitempty"`
	ChannelType string    `bson:"channel_type,omitempty"`
	IsGroup     bool      `bson:"is_group"`
	Attachments []string  `bson:"attachments,omitempty"`
	GuildID     string    `bson:"guild_id,omitempty"`
	GuildName   string    `bson:"guild_name,omitempty"`
}

type SimpleEditedMessageData struct {
	MessageID         string    `bson:"message_id"`
	UserID            string    `bson:"user_id"`
	Username          string    `bson:"username,omitempty"`
	BeforeContent     string    `bson:"before_content"`
	AfterContent      string    `bson:"after_content"`
	BeforeAttachments []string  `bson:"before_attachments,omitempty"`
	AfterAttachments  []string  `bson:"after_attachments,omitempty"`
	EditedAt          time.Time `bson:"edited_at"`
	ChannelID         string    `bson:"channel_id"`
	ChannelName       string    `bson:"channel_name,omitempty"`
	ChannelType       string    `bson:"channel_type,omitempty"`
	IsGroup           bool      `bson:"is_group"`
	GuildID           string    `bson:"guild_id,omitempty"`
	GuildName         string    `bson:"guild_name,omitempty"`
}

type SimpleMentionData struct {
	MessageID   string    `bson:"message_id"`
	AuthorID    string    `bson:"author_id"`
	AuthorName  string    `bson:"author_name"`
	Content     string    `bson:"content"`
	CreatedAt   time.Time `bson:"created_at"`
	ChannelID   string    `bson:"channel_id"`
	ChannelName string    `bson:"channel_name,omitempty"`
	Attachments []string  `bson:"attachments,omitempty"`
	ChannelType int       `bson:"channel_type"`
	IsGroup     bool      `bson:"is_group"`
	TargetID    string    `bson:"target_id"`
	GuildID     string    `bson:"guild_id,omitempty"`
	GuildName   string    `bson:"guild_name,omitempty"`
}

// NewSimpleDatabase creates a straightforward database connection
func NewSimpleDatabase(cfg *config.Database) (*SimpleDatabase, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	clientOptions := options.Client().
		ApplyURI(cfg.URI).
		SetMaxPoolSize(20).
		SetMinPoolSize(5)

	client, err := mongo.Connect(ctx, clientOptions)
	if err != nil {
		return nil, err
	}

	if err := client.Ping(ctx, nil); err != nil {
		return nil, err
	}

	db := &SimpleDatabase{
		client: client,
		db:     client.Database(cfg.Name),
	}

	log.Info("Connected to MongoDB")
	return db, nil
}

// Direct storage methods - simple and efficient
func (d *SimpleDatabase) StoreMessage(msg *SimpleMessageData) error {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	_, err := d.db.Collection("user_messages").InsertOne(ctx, msg)
	if err != nil && !isDuplicateError(err) {
		log.Errorf("Failed to store message: %v", err)
		return err
	}
	return nil
}

func (d *SimpleDatabase) StoreDeletedMessage(msg *SimpleDeletedMessageData) error {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	_, err := d.db.Collection("deleted_messages").InsertOne(ctx, msg)
	if err != nil && !isDuplicateError(err) {
		log.Errorf("Failed to store deleted message: %v", err)
		return err
	}
	return nil
}

func (d *SimpleDatabase) StoreEditedMessage(msg *SimpleEditedMessageData) error {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	_, err := d.db.Collection("edited_messages").InsertOne(ctx, msg)
	if err != nil && !isDuplicateError(err) {
		log.Errorf("Failed to store edited message: %v", err)
		return err
	}
	return nil
}

func (d *SimpleDatabase) StoreMention(mention *SimpleMentionData) error {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	_, err := d.db.Collection("mentions").InsertOne(ctx, mention)
	if err != nil && !isDuplicateError(err) {
		log.Errorf("Failed to store mention: %v", err)
		return err
	}
	return nil
}

// Simple query methods with proper Go idioms
func (d *SimpleDatabase) GetDeletedMessages(filter bson.M, limit int64) ([]SimpleDeletedMessageData, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	opts := options.Find().
		SetSort(bson.D{{"deleted_at", -1}}).
		SetLimit(limit)

	cursor, err := d.db.Collection("deleted_messages").Find(ctx, filter, opts)
	if err != nil {
		return nil, err
	}
	defer cursor.Close(ctx)

	var messages []SimpleDeletedMessageData
	if err := cursor.All(ctx, &messages); err != nil {
		return nil, err
	}

	return messages, nil
}

func (d *SimpleDatabase) GetEditedMessages(filter bson.M, limit int64) ([]SimpleEditedMessageData, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	opts := options.Find().
		SetSort(bson.D{{"edited_at", -1}}).
		SetLimit(limit)

	cursor, err := d.db.Collection("edited_messages").Find(ctx, filter, opts)
	if err != nil {
		return nil, err
	}
	defer cursor.Close(ctx)

	var messages []SimpleEditedMessageData
	if err := cursor.All(ctx, &messages); err != nil {
		return nil, err
	}

	return messages, nil
}

func (d *SimpleDatabase) GetMentions(filter bson.M, limit int64) ([]SimpleMentionData, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	opts := options.Find().
		SetSort(bson.D{{"created_at", -1}}).
		SetLimit(limit)

	cursor, err := d.db.Collection("mentions").Find(ctx, filter, opts)
	if err != nil {
		return nil, err
	}
	defer cursor.Close(ctx)

	var mentions []SimpleMentionData
	if err := cursor.All(ctx, &mentions); err != nil {
		return nil, err
	}

	return mentions, nil
}

// Close the database connection
func (d *SimpleDatabase) Close() error {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	return d.client.Disconnect(ctx)
}

// Helper function to check for duplicate key errors
func isDuplicateError(err error) bool {
	if mongoErr, ok := err.(mongo.WriteException); ok {
		for _, writeErr := range mongoErr.WriteErrors {
			if writeErr.Code == 11000 { // Duplicate key error
				return true
			}
		}
	}
	return false
}