// Package tandem is a proof of concept of the Go write-side SDK (HLD-write-side-sdks). It exists to
// check the contract against the Java writer, not as the SDK's design.
package tandem

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"hash/fnv"
	"slices"
	"strconv"
	"strings"
	"unicode/utf16"
	"unicode/utf8"
)

const (
	maxColumn       = 255
	headerCorrID    = "correlation-id"
	headerType      = "content-type"
	headerSchema    = "dataschema"
	wakeupChannel   = "tandem_wakeup"
	defaultBuckets  = 256
	sqlStateUnique  = "23505"
	insertSQL       = "INSERT INTO tandem_outbox (aggregate_id, aggregate_type, type, bucket, seq, seq_source, payload, headers, correlation_id) VALUES ($1, $2, $3, $4, $5, $6, CAST($7 AS jsonb), CAST($8 AS jsonb), $9)"
	insertManagedSQ = "INSERT INTO tandem_outbox (aggregate_id, aggregate_type, type, bucket, seq_source, payload, headers, correlation_id) VALUES ($1, $2, $3, $4, $5, CAST($6 AS jsonb), CAST($7 AS jsonb), $8)"
	lockSQL         = "SELECT pg_advisory_xact_lock(hashtext($1))"
	notifySQL       = "SELECT pg_notify('" + wakeupChannel + "', bucket::text) FROM unnest(CAST($1 AS int[])) AS bucket"
)

var (
	ErrInvalidMessage = errors.New("tandem: invalid message")
	ErrDuplicateSeq   = errors.New("tandem: duplicate (aggregate_id, seq)")
	ErrInsert         = errors.New("tandem: outbox insert failed")
	ErrBucketCount    = errors.New("tandem: bucket count conflict")
)

// Seq is the sequence mode of a message. Its zero value states no mode, which Insert refuses.
type Seq struct {
	source int8 // 0 unset, 1 APPLICATION, 2 MANAGED, 3 NONE
	value  int64
}

func SeqOf(v int64) Seq      { return Seq{source: 1, value: v} }
func ManagedSeq() Seq        { return Seq{source: 2} }
func Unsequenced() Seq       { return Seq{source: 3} }
func (s Seq) dbSource() int8 { return s.source - 1 }

func (s Seq) String() string {
	switch s.source {
	case 1:
		return strconv.FormatInt(s.value, 10)
	case 2:
		return "MANAGED"
	case 3:
		return "NONE"
	}
	return "UNSET"
}

type Message struct {
	AggregateID   string
	AggregateType string
	Type          string
	Seq           Seq
	Payload       []byte
	ContentType   string
	Headers       map[string]string
	LockedWrite   bool
}

// BucketFor is FNV-1a 64 over the UTF-8 bytes, reduced with the floor modulus of the SIGNED value.
func BucketFor(aggregateID string, bucketCount int32) int32 {
	h := fnv.New64a()
	h.Write([]byte(aggregateID))
	n := int64(bucketCount)
	m := int64(h.Sum64()) % n
	if m < 0 {
		m += n
	}
	return int32(m)
}

// IsJavaWhitespace is Character.isWhitespace(int): the set String.isBlank uses.
func IsJavaWhitespace(r rune) bool {
	switch {
	case r >= 0x09 && r <= 0x0D, r >= 0x1C && r <= 0x20:
		return true
	case r == 0x1680, r >= 0x2000 && r <= 0x2006, r >= 0x2008 && r <= 0x200A,
		r == 0x2028, r == 0x2029, r == 0x205F, r == 0x3000:
		return true
	}
	return false
}

func isBlank(s string) bool {
	for _, r := range s {
		if !IsJavaWhitespace(r) {
			return false
		}
	}
	return true
}

// CompareUTF16 orders strings as Java's String.compareTo does, by UTF-16 code units.
func CompareUTF16(a, b string) int {
	return slices.Compare(utf16.Encode([]rune(a)), utf16.Encode([]rune(b)))
}

func validText(field, v string, required bool) error {
	if required && isBlank(v) {
		return fmt.Errorf("%w: %s must not be blank", ErrInvalidMessage, field)
	}
	if !utf8.ValidString(v) || strings.ContainsRune(v, 0) {
		return fmt.Errorf("%w: %s is not valid UTF-8 text", ErrInvalidMessage, field)
	}
	if utf8.RuneCountInString(v) > maxColumn {
		return fmt.Errorf("%w: %s exceeds %d characters", ErrInvalidMessage, field, maxColumn)
	}
	return nil
}

func validate(m Message) error {
	if err := validText("aggregate_id", m.AggregateID, true); err != nil {
		return err
	}
	if err := validText("aggregate_type", m.AggregateType, true); err != nil {
		return err
	}
	if err := validText("type", m.Type, false); err != nil {
		return err
	}
	if m.Payload == nil {
		return fmt.Errorf("%w: no payload", ErrInvalidMessage)
	}
	if m.Seq.source == 0 {
		return fmt.Errorf("%w: no seq mode stated: SeqOf(n), ManagedSeq() or Unsequenced()", ErrInvalidMessage)
	}
	for k, v := range m.Headers {
		if !utf8.ValidString(k) || !utf8.ValidString(v) || strings.ContainsRune(k, 0) || strings.ContainsRune(v, 0) {
			return fmt.Errorf("%w: header is not valid UTF-8 text", ErrInvalidMessage)
		}
	}
	if s, ok := m.Headers[headerSchema]; ok && !IsDataSchemaURI(s) {
		return fmt.Errorf("%w: the dataschema header is not a URI", ErrInvalidMessage)
	}
	return nil
}

// Writer inserts outbox rows inside the caller's transaction.
type Writer struct {
	bucketCount int32
	wakeup      bool
}

type Option func(*Writer)

func WithBucketCount(n int32) Option { return func(w *Writer) { w.bucketCount = n } }
func WithWakeup() Option             { return func(w *Writer) { w.wakeup = true } }

// NewWriter reconciles the bucket count with tandem_meta (seed or validate), outside any transaction.
func NewWriter(ctx context.Context, db *sql.DB, opts ...Option) (*Writer, error) {
	w := &Writer{bucketCount: defaultBuckets}
	for _, o := range opts {
		o(w)
	}
	if w.bucketCount <= 0 {
		return nil, fmt.Errorf("tandem: bucket count must be positive")
	}
	want := strconv.Itoa(int(w.bucketCount))
	if _, err := db.ExecContext(ctx, "INSERT INTO tandem_meta (key, value) VALUES ('bucket_count', $1) ON CONFLICT (key) DO NOTHING", want); err != nil {
		return nil, fmt.Errorf("tandem: seeding the bucket count: %w", err)
	}
	var stored string
	if err := db.QueryRowContext(ctx, "SELECT value FROM tandem_meta WHERE key = 'bucket_count'").Scan(&stored); err != nil {
		return nil, fmt.Errorf("tandem: reading the bucket count: %w", err)
	}
	if stored != want {
		return nil, fmt.Errorf("%w: configured %s, stored %s, and the stored value cannot change", ErrBucketCount, want, stored)
	}
	return w, nil
}

func (w *Writer) Insert(ctx context.Context, tx *sql.Tx, m Message) error {
	return w.InsertAll(ctx, tx, []Message{m})
}

func (w *Writer) InsertAll(ctx context.Context, tx *sql.Tx, msgs []Message) error {
	for _, m := range msgs {
		if err := validate(m); err != nil {
			return err
		}
	}
	var locks []string
	for _, m := range msgs {
		if m.LockedWrite && !slices.Contains(locks, m.AggregateID) {
			locks = append(locks, m.AggregateID)
		}
	}
	slices.SortFunc(locks, CompareUTF16)
	for _, id := range locks {
		if _, err := tx.ExecContext(ctx, lockSQL, id); err != nil {
			return &InsertError{AggregateID: id, Seq: "lock", cause: err}
		}
	}
	buckets := map[int32]bool{}
	var order []int32
	for _, m := range msgs {
		b := BucketFor(m.AggregateID, w.bucketCount)
		if err := w.insertOne(ctx, tx, m, b); err != nil {
			return err
		}
		if !buckets[b] {
			buckets[b] = true
			order = append(order, b)
		}
	}
	if w.wakeup && len(order) > 0 {
		if _, err := tx.ExecContext(ctx, notifySQL, IntArrayLiteral(order)); err != nil {
			return &InsertError{Seq: "wakeup", cause: err}
		}
	}
	return nil
}

// IntArrayLiteral renders a PostgreSQL array literal, bound as text so no driver array type is needed.
func IntArrayLiteral(v []int32) string {
	parts := make([]string, len(v))
	for i, b := range v {
		parts[i] = strconv.Itoa(int(b))
	}
	return "{" + strings.Join(parts, ",") + "}"
}

func (w *Writer) insertOne(ctx context.Context, tx *sql.Tx, m Message, bucket int32) error {
	headers := make(map[string]string, len(m.Headers)+1)
	for k, v := range m.Headers {
		headers[k] = v
	}
	if m.ContentType != "" {
		headers[headerType] = m.ContentType
	}
	hj, err := json.Marshal(headers)
	if err != nil {
		return fmt.Errorf("%w: headers: %v", ErrInvalidMessage, err)
	}
	var typ, corr any
	if m.Type != "" {
		typ = m.Type
	}
	if c, ok := headers[headerCorrID]; ok {
		corr = TruncateCodePoints(c, maxColumn)
	}
	// The payload is bound as a string: a []byte goes out as bytea, and bytea does not cast to jsonb.
	payload := string(m.Payload)
	if m.Seq.source == 2 {
		_, err = tx.ExecContext(ctx, insertManagedSQ, m.AggregateID, m.AggregateType, typ, bucket,
			m.Seq.dbSource(), payload, string(hj), corr)
	} else {
		var seq any
		if m.Seq.source == 1 {
			seq = m.Seq.value
		}
		_, err = tx.ExecContext(ctx, insertSQL, m.AggregateID, m.AggregateType, typ, bucket, seq,
			m.Seq.dbSource(), payload, string(hj), corr)
	}
	if err != nil {
		return &InsertError{AggregateType: m.AggregateType, AggregateID: m.AggregateID, Seq: m.Seq.String(), cause: err}
	}
	return nil
}

// TruncateCodePoints keeps the first n code points.
func TruncateCodePoints(s string, n int) string {
	i := 0
	for pos := range s {
		if i == n {
			return s[:pos]
		}
		i++
	}
	return s
}

// InsertError never renders the driver's text, which can quote bound values; Unwrap still reaches it.
type InsertError struct {
	AggregateType, AggregateID, Seq string
	cause                           error
}

func (e *InsertError) SQLState() string {
	var s interface{ SQLState() string }
	if errors.As(e.cause, &s) {
		return s.SQLState()
	}
	return ""
}

func (e *InsertError) Error() string {
	what := "outbox insert failed"
	if e.SQLState() == sqlStateUnique {
		what = "duplicate (aggregate_id, seq)"
	}
	return fmt.Sprintf("tandem: %s aggregateType:%s, aggregateId:%s, seq:%s, sqlState:%s",
		what, e.AggregateType, e.AggregateID, e.Seq, e.SQLState())
}

func (e *InsertError) Is(target error) bool {
	if target == ErrDuplicateSeq {
		return e.SQLState() == sqlStateUnique
	}
	return target == ErrInsert
}

func (e *InsertError) Unwrap() error { return e.cause }
