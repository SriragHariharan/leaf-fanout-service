package events

const (
	EventPostCreated = "post.created"
	EventPostEdited  = "post.edited"
	EventPostDeleted = "post.deleted"
)

// PostEvent is the Kafka payload for topic post.events.
type PostEvent struct {
	EventType string  `json:"eventType"`
	PostID    string  `json:"postID"`
	ImageURL  *string `json:"imageURL,omitempty"`
	Content   string  `json:"content,omitempty"`
	OwnerID   string  `json:"ownerID,omitempty"`
}
