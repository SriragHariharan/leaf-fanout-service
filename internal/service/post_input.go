package service

// PostCreatedInput is the data needed to save a post and fan it out to friends.
type PostCreatedInput struct {
	PostID   string
	Content  string
	MediaURL string // empty string when there is no image
	OwnerID  string
}
