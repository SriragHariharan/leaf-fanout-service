package events

// PostCreated is the Kafka payload for topic post.created (matches post-service producer).
type PostCreated struct {
	PostID   string  `json:"postID"`
	ImageURL *string `json:"imageURL"`
	Content  string  `json:"content"`
	OwnerID  string  `json:"ownerID"`
}
