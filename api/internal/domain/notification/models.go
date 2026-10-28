package notification

import (
	"fmt"
	"net/mail"
	"regexp"
	"slices"
	"time"

	apipub "github.com/barkin94/notification-system/api/public"
)

// E.164: + followed by 7–15 digits, leading digit non-zero.
var phoneRE = regexp.MustCompile(`^\+[1-9]\d{6,14}$`)

var validTransitions = map[apipub.Status][]apipub.Status{
	apipub.StatusPending:   {apipub.StatusDelivered, apipub.StatusFailed, apipub.StatusCancelled},
	apipub.StatusFailed:    {apipub.StatusPending}, // retry
	apipub.StatusDelivered: {},                     // terminal
	apipub.StatusCancelled: {},                     // terminal
}

type Notification struct {
	channel      apipub.Channel
	recipient    string
	content      string
	priority     apipub.Priority
	status       apipub.Status
	deliverAfter *time.Time
	maxAttempts  int
}

func (n *Notification) SetChannel(ch apipub.Channel) error {
	switch ch {
	case apipub.ChannelSMS, apipub.ChannelEmail, apipub.ChannelPush:
		n.channel = ch
		return nil
	default:
		return ErrInvalidChannel()
	}
}

// SetRecipient must be called after SetChannel; format is validated per channel.
func (n *Notification) SetRecipient(r string) error {
	if r == "" {
		return ErrRecipientRequired()
	}
	switch n.channel {
	case apipub.ChannelEmail:
		if _, err := mail.ParseAddress(r); err != nil {
			return ErrInvalidEmail()
		}
	case apipub.ChannelSMS:
		if !phoneRE.MatchString(r) {
			return ErrInvalidPhone()
		}
	case apipub.ChannelPush:
		// no format restriction beyond non-empty
	default:
		return ErrChannelNotSet()
	}
	n.recipient = r
	return nil
}

func (n *Notification) SetContent(s string) error {
	if s == "" {
		return ErrContentRequired()
	}
	n.content = s
	return nil
}

func (n *Notification) SetPriority(p apipub.Priority) error {
	switch p {
	case apipub.PriorityHigh, apipub.PriorityNormal, apipub.PriorityLow:
		n.priority = p
		return nil
	default:
		return ErrInvalidPriority()
	}
}

func (n *Notification) SetDeliverAfter(t *time.Time) {
	n.deliverAfter = t
}

func (n *Notification) Transition(to apipub.Status) error {
	if n.status == to {
		return nil
	}

	allowed, ok := validTransitions[n.status]
	if !ok {
		return fmt.Errorf("%w: %q", ErrUnknownStatus(), n.status)
	}
	if slices.Contains(allowed, to) {
		n.status = to
		return nil
	}
	return fmt.Errorf("%w: %s → %s", ErrInvalidTransition(), n.status, to)
}

func (n *Notification) SetMaxAttempts(v int) error {
	if v <= 0 {
		return ErrInvalidMaxAttempts()
	}
	n.maxAttempts = v
	return nil
}

// New creates a Notification directly from its field values, bypassing setter validation.
func New(channel apipub.Channel, recipient, content string, priority apipub.Priority, status apipub.Status, deliverAfter *time.Time, maxAttempts int) *Notification {
	return &Notification{
		channel:      channel,
		recipient:    recipient,
		content:      content,
		priority:     priority,
		status:       status,
		deliverAfter: deliverAfter,
		maxAttempts:  maxAttempts,
	}
}

func (n Notification) GetChannel() apipub.Channel   { return n.channel }
func (n Notification) GetRecipient() string         { return n.recipient }
func (n Notification) GetContent() string           { return n.content }
func (n Notification) GetPriority() apipub.Priority { return n.priority }
func (n Notification) GetStatus() apipub.Status     { return n.status }
func (n Notification) GetDeliverAfter() *time.Time  { return n.deliverAfter }
func (n Notification) GetMaxAttempts() int {
	if n.maxAttempts == 0 {
		return 1
	}
	return n.maxAttempts
}
