package tandem_test

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"net/url"
	"os"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"
	"unicode"

	"example.com/tandem-go-poc/tandem"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	_ "github.com/jackc/pgx/v5/stdlib"
	"github.com/lib/pq"
)

const dsn = "postgres://poc:poc@localhost:55432/poc?sslmode=disable"

var drivers = []string{"pgx", "postgres"} // pgx/v5/stdlib and lib/pq

type javaFacts struct {
	Buckets []struct {
		ID     string `json:"id"`
		Count  int32  `json:"count"`
		Bucket int32  `json:"bucket"`
	} `json:"buckets"`
	Whitespace []rune `json:"whitespace"`
	URI        []struct {
		S  string `json:"s"`
		OK bool   `json:"ok"`
	} `json:"uri"`
	SortedIDs []string `json:"sortedIds"`
	Lengths   []struct {
		ID            string `json:"id"`
		AggregateID   bool   `json:"aggregateId"`
		AggregateType bool   `json:"aggregateType"`
		Type          bool   `json:"type"`
	} `json:"lengths"`
	Rows []struct {
		Name     string             `json:"name"`
		Row      map[string]*string `json:"row"`
		Error    string             `json:"error"`
		SQLState string             `json:"sqlState"`
	} `json:"rows"`
}

func facts(t *testing.T) javaFacts {
	t.Helper()
	b, err := os.ReadFile("../java-facts.json")
	if err != nil {
		t.Fatal(err)
	}
	var f javaFacts
	if err := json.Unmarshal(b, &f); err != nil {
		t.Fatal(err)
	}
	return f
}

func open(t *testing.T, driver string) *sql.DB {
	t.Helper()
	db, err := sql.Open(driver, dsn)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { db.Close() })
	return db
}

func resetBucketCount(t *testing.T, db *sql.DB, value string) {
	t.Helper()
	if _, err := db.Exec("DELETE FROM tandem_meta WHERE key = 'bucket_count'"); err != nil {
		t.Fatal(err)
	}
	if value != "" {
		if _, err := db.Exec("INSERT INTO tandem_meta (key, value) VALUES ('bucket_count', $1)", value); err != nil {
			t.Fatal(err)
		}
	}
}

// --- pure logic -------------------------------------------------------------------------------

func TestBucketsMatchJava(t *testing.T) {
	f := facts(t)
	for _, c := range f.Buckets {
		if got := tandem.BucketFor(c.ID, c.Count); got != c.Bucket {
			t.Errorf("id:%q count:%d go:%d java:%d", c.ID, c.Count, got, c.Bucket)
		}
	}
	t.Logf("%d bucket cases match", len(f.Buckets))
	// The unsigned trap of HLD §4.2: same at 256, different at 100.
	h := fnvUnsigned("order-7")
	t.Logf("order-7 unsigned%%256=%d unsigned%%100=%d signed floorMod 100=%d", h%256, h%100, tandem.BucketFor("order-7", 100))
}

func fnvUnsigned(s string) uint64 {
	h := uint64(0xcbf29ce484222325)
	for i := 0; i < len(s); i++ {
		h ^= uint64(s[i])
		h *= 0x100000001b3
	}
	return h
}

func TestBlankDefinition(t *testing.T) {
	f := facts(t)
	java := map[rune]bool{}
	for _, r := range f.Whitespace {
		java[r] = true
	}
	var goOnly, javaOnly, mine []string
	for r := rune(0); r <= unicode.MaxRune; r++ {
		if unicode.IsSpace(r) && !java[r] {
			goOnly = append(goOnly, fmt.Sprintf("U+%04X", r))
		}
		if java[r] && !unicode.IsSpace(r) {
			javaOnly = append(javaOnly, fmt.Sprintf("U+%04X", r))
		}
		if tandem.IsJavaWhitespace(r) != java[r] {
			mine = append(mine, fmt.Sprintf("U+%04X", r))
		}
	}
	t.Logf("unicode.IsSpace but not Java whitespace: %v", goOnly)
	t.Logf("Java whitespace but not unicode.IsSpace: %v", javaOnly)
	if len(mine) > 0 {
		t.Errorf("IsJavaWhitespace disagrees with Java on %v", mine)
	}
}

func TestDataSchemaURI(t *testing.T) {
	f := facts(t)
	var urlLooser, ruleLooser, ruleStricter []string
	for _, c := range f.URI {
		_, err := url.Parse(c.S)
		if err == nil && !c.OK {
			urlLooser = append(urlLooser, fmt.Sprintf("%q", c.S))
		}
		rule := tandem.IsDataSchemaURI(c.S)
		if rule && !c.OK {
			ruleLooser = append(ruleLooser, fmt.Sprintf("%q", c.S))
		}
		if !rule && c.OK {
			ruleStricter = append(ruleStricter, fmt.Sprintf("%q", c.S))
		}
	}
	t.Logf("net/url.Parse accepts, java.net.URI refuses (%d): %s", len(urlLooser), strings.Join(urlLooser, " "))
	t.Logf("candidate rule refuses, Java accepts (%d): %s", len(ruleStricter), strings.Join(ruleStricter, " "))
	if len(ruleLooser) > 0 {
		t.Errorf("candidate rule accepts what Java refuses: %s", strings.Join(ruleLooser, " "))
	}
}

func TestLockOrderMatchesJava(t *testing.T) {
	f := facts(t)
	ids := slices.Clone(f.SortedIDs)
	slices.Reverse(ids)
	bytesOrder := slices.Clone(ids)
	slices.Sort(bytesOrder)
	utf16Order := slices.Clone(ids)
	slices.SortFunc(utf16Order, tandem.CompareUTF16)
	t.Logf("Java order:      %q", f.SortedIDs)
	t.Logf("Go byte order:   %q (equal: %v)", bytesOrder, slices.Equal(bytesOrder, f.SortedIDs))
	if !slices.Equal(utf16Order, f.SortedIDs) {
		t.Errorf("UTF-16 order %q differs from Java", utf16Order)
	}
}

// --- database ---------------------------------------------------------------------------------

func TestLengthsInCodePoints(t *testing.T) {
	f := facts(t)
	for _, driver := range drivers {
		db := open(t, driver)
		for _, c := range f.Lengths {
			for _, col := range []string{"aggregate_id", "aggregate_type", "type"} {
				tx, _ := db.Begin()
				_, err := tx.Exec("INSERT INTO tandem_outbox (aggregate_id, aggregate_type, type, bucket, seq, seq_source, payload, headers) VALUES ($1, $2, $3, 0, NULL, 2, '{}', '{}')",
					pick(col, "aggregate_id", c.ID, "a"), pick(col, "aggregate_type", c.ID, "T"), pick(col, "type", c.ID, "T"))
				tx.Rollback()
				java := map[string]bool{"aggregate_id": c.AggregateID, "aggregate_type": c.AggregateType, "type": c.Type}[col]
				if (err == nil) != java {
					t.Logf("%s %s: %d code points, %d UTF-16 units: database accepts=%v, Java accepts=%v",
						driver, col, len([]rune(c.ID)), len(utf16Len(c.ID)), err == nil, java)
				}
			}
		}
	}
}

func pick(col, want, v, otherwise string) string {
	if col == want {
		return v
	}
	return otherwise
}

func utf16Len(s string) []struct{} {
	n := 0
	for _, r := range s {
		n++
		if r >= 0x10000 {
			n++
		}
	}
	return make([]struct{}, n)
}

const emoji = "\U0001F600"

// goCases mirrors RowCase.ALL in JavaFacts.java.
func goCases() map[string]struct {
	count int32
	msg   tandem.Message
} {
	base := func(id string, seq tandem.Seq) tandem.Message {
		return tandem.Message{AggregateID: id, AggregateType: "PocAggregate", Seq: seq, Payload: []byte(`{"k":1}`)}
	}
	type c = struct {
		count int32
		msg   tandem.Message
	}
	withH := func(m tandem.Message, h map[string]string) tandem.Message { m.Headers = h; return m }
	headers := base("poc-headers", tandem.Unsequenced())
	headers.Type, headers.ContentType = "OrderPlaced", "application/json"
	headers.Headers = map[string]string{"content-type": "text/plain", "dataschema": "https://example.com/s.json", "x-html": "<a&b>"}
	bad := base("poc-bad-json", tandem.Unsequenced())
	bad.Payload = []byte("{nope")
	uni := base("poc-unicode", tandem.Unsequenced())
	uni.Payload = []byte(`{"s":"café é <&>","n":1.0e2,"k":{"b":1,"a":2}}`)
	typ := base("poc-type-emoji", tandem.Unsequenced())
	typ.AggregateType = strings.Repeat(emoji, 128)
	return map[string]c{
		"application":              {256, base("poc-app", tandem.SeqOf(7))},
		"managed":                  {256, base("poc-managed", tandem.ManagedSeq())},
		"none":                     {256, base("poc-none", tandem.Unsequenced())},
		"bucket-100-top-bit":       {100, base("order-7", tandem.Unsequenced())},
		"headers-and-type":         {256, headers},
		"correlation-ascii-300":    {256, withH(base("poc-cid-ascii", tandem.Unsequenced()), map[string]string{"correlation-id": strings.Repeat("c", 300)})},
		"correlation-emoji-at-255": {256, withH(base("poc-cid-emoji", tandem.Unsequenced()), map[string]string{"correlation-id": strings.Repeat("c", 254) + emoji + "tail"})},
		"correlation-emoji-only":   {256, withH(base("poc-cid-emoji-only", tandem.Unsequenced()), map[string]string{"correlation-id": strings.Repeat(emoji, 200)})},
		"payload-not-json":         {256, bad},
		"payload-unicode":          {256, uni},
		"aggregate-type-128-emoji": {256, typ},
	}
}

const readRow = `SELECT aggregate_id, aggregate_type, type, bucket::text, seq::text, seq_source::text, payload::text,
	headers::text, correlation_id, octet_length(correlation_id)::text, char_length(correlation_id)::text
	FROM tandem_outbox WHERE aggregate_id = $1`

var rowCols = []string{"aggregate_id", "aggregate_type", "type", "bucket", "seq", "seq_source", "payload", "headers", "correlation_id", "cid_bytes", "cid_chars"}

func TestRowsMatchJava(t *testing.T) {
	f := facts(t)
	cases := goCases()
	ctx := context.Background()
	for _, driver := range drivers {
		db := open(t, driver)
		for _, jr := range f.Rows {
			c := cases[jr.Name]
			resetBucketCount(t, db, "")
			w, err := tandem.NewWriter(ctx, db, tandem.WithBucketCount(c.count))
			if err != nil {
				t.Fatal(err)
			}
			tx, _ := db.Begin()
			err = w.Insert(ctx, tx, c.msg)
			if err != nil {
				var st interface{ SQLState() string }
				state := ""
				if errors.As(err, &st) {
					state = st.SQLState()
				}
				if jr.Error == "" || state != jr.SQLState {
					t.Logf("%s %s: Go error %q, Java error %q sqlState %s", driver, jr.Name, err, jr.Error, jr.SQLState)
				}
				tx.Rollback()
				continue
			}
			vals := make([]sql.NullString, len(rowCols))
			ptrs := make([]any, len(vals))
			for i := range vals {
				ptrs[i] = &vals[i]
			}
			if err := tx.QueryRow(readRow, c.msg.AggregateID).Scan(ptrs...); err != nil {
				t.Fatal(err)
			}
			tx.Rollback()
			if jr.Row == nil {
				t.Logf("%s %s: Go stored the row, Java refused: %s", driver, jr.Name, jr.Error)
				continue
			}
			for i, col := range rowCols {
				j := jr.Row[col]
				if col == "seq" && jr.Name == "managed" {
					continue // a drawn number, different on every insert
				}
				if (j == nil) != !vals[i].Valid || (j != nil && *j != vals[i].String) {
					t.Logf("%s %s: column %s differs: go=%s java=%s", driver, jr.Name, col, short(vals[i]), shortp(j))
				}
			}
		}
		resetBucketCount(t, db, "256")
	}
}

func short(v sql.NullString) string {
	if !v.Valid {
		return "NULL"
	}
	return tail(v.String)
}

func shortp(v *string) string {
	if v == nil {
		return "NULL"
	}
	return tail(*v)
}

func tail(s string) string {
	r := []rune(s)
	if len(r) > 6 {
		return fmt.Sprintf("…%q (%d chars)", string(r[len(r)-4:]), len(r))
	}
	return fmt.Sprintf("%q", s)
}

func TestDriverErrorTextAndSQLState(t *testing.T) {
	ctx := context.Background()
	const secret = "S3CRET-PAYLOAD"
	for _, driver := range drivers {
		db := open(t, driver)
		resetBucketCount(t, db, "256")
		w, err := tandem.NewWriter(ctx, db)
		if err != nil {
			t.Fatal(err)
		}
		// Invalid JSON whose text carries the secret.
		tx, _ := db.Begin()
		err = w.Insert(ctx, tx, tandem.Message{AggregateID: "poc-secret", AggregateType: "T", Seq: tandem.Unsequenced(),
			Payload: []byte(`{"card":"` + secret + `" oops}`)})
		tx.Rollback()
		report(t, driver, "invalid JSON", err, secret)
		// Duplicate seq: the driver's detail quotes the key.
		tx, _ = db.Begin()
		m := tandem.Message{AggregateID: "poc-dup", AggregateType: "T", Seq: tandem.SeqOf(1), Payload: []byte(`{}`)}
		_ = w.Insert(ctx, tx, m)
		err = w.Insert(ctx, tx, m)
		tx.Rollback()
		report(t, driver, "duplicate seq", err, "poc-dup, 1")
		if !errors.Is(err, tandem.ErrDuplicateSeq) {
			t.Errorf("%s: duplicate not recognised: %v", driver, err)
		}
	}
}

func report(t *testing.T, driver, what string, err error, secret string) {
	t.Helper()
	cause := errors.Unwrap(err)
	var detail string
	var pg *pgconn.PgError
	var pqe *pq.Error
	switch {
	case errors.As(err, &pg):
		detail = pg.Detail + " | " + pg.Where
	case errors.As(err, &pqe):
		detail = pqe.Detail + " | " + pqe.Where
	}
	t.Logf("%s %s: driver Detail/Where %q", driver, what, detail)
	t.Logf("%s %s: driver Error() %q; secret in driver Error(): %v; secret in driver Detail/Where: %v; secret in SDK error: %v",
		driver, what, cause.Error(), strings.Contains(cause.Error(), secret), strings.Contains(detail, secret), strings.Contains(err.Error(), secret))
	if what != "duplicate seq" && strings.Contains(err.Error(), secret) {
		t.Errorf("%s %s: SDK error leaks: %s", driver, what, err)
	}
}

func TestPayloadBoundAsBytes(t *testing.T) {
	for _, driver := range drivers {
		db := open(t, driver)
		tx, _ := db.Begin()
		_, err := tx.Exec("INSERT INTO tandem_outbox (aggregate_id, aggregate_type, bucket, seq, seq_source, payload, headers) VALUES ('poc-bytes', 'T', 0, NULL, 2, CAST($1 AS jsonb), '{}')", []byte(`{"k":1}`))
		var stored string
		if err == nil {
			_ = tx.QueryRow("SELECT payload::text FROM tandem_outbox WHERE aggregate_id = 'poc-bytes'").Scan(&stored)
		}
		tx.Rollback()
		t.Logf("%s: payload bound as []byte: err=%v stored=%q", driver, err, stored)
	}
}

func TestInvalidTextReachingTheDatabase(t *testing.T) {
	inputs := map[string]string{"invalid UTF-8": "poc-\xff\xfe", "NUL": "poc-\x00-nul"}
	for _, driver := range drivers {
		db := open(t, driver)
		for name, id := range inputs {
			tx, _ := db.Begin()
			_, err := tx.Exec("INSERT INTO tandem_outbox (aggregate_id, aggregate_type, bucket, seq, seq_source, payload, headers) VALUES ($1, 'T', 0, NULL, 2, '{}', '{}')", id)
			tx.Rollback()
			t.Logf("%s %s in aggregate_id: %v", driver, name, err)
		}
	}
}

func TestSeqModeTrap(t *testing.T) {
	db := open(t, "pgx")
	tx, _ := db.Begin()
	defer tx.Rollback()
	var seq sql.NullInt64
	_, err := tx.Exec("INSERT INTO tandem_outbox (aggregate_id, aggregate_type, bucket, seq_source, payload, headers) VALUES ('poc-trap', 'T', 0, 0, '{}', '{}')")
	if err == nil {
		_ = tx.QueryRow("SELECT seq FROM tandem_outbox WHERE aggregate_id = 'poc-trap'").Scan(&seq)
	}
	t.Logf("APPLICATION with seq omitted: err=%v, stored seq=%v (the trap HLD §4.3 describes)", err, seq)
	_, err = tx.Exec("SAVEPOINT s")
	_, err = tx.Exec("INSERT INTO tandem_outbox (aggregate_id, aggregate_type, bucket, seq, seq_source, payload, headers) VALUES ('poc-trap2', 'T', 0, NULL, 1, '{}', '{}')")
	t.Logf("MANAGED with seq bound to NULL: %v", err)
}

func TestMultiRowValuesOrder(t *testing.T) {
	db := open(t, "pgx")
	const rows, rounds = 200, 20
	violations := 0
	for r := 0; r < rounds; r++ {
		var sb strings.Builder
		sb.WriteString("INSERT INTO tandem_outbox (aggregate_id, aggregate_type, bucket, seq_source, payload, headers) VALUES ")
		for i := 0; i < rows; i++ {
			if i > 0 {
				sb.WriteString(",")
			}
			fmt.Fprintf(&sb, "('poc-multi-%04d', 'T', %d, 1, '{}', '{}')", i, i%7)
		}
		tx, _ := db.Begin()
		if _, err := tx.Exec(sb.String()); err != nil {
			t.Fatal(err)
		}
		rs, _ := tx.Query("SELECT aggregate_id FROM tandem_outbox WHERE aggregate_id LIKE 'poc-multi-%' ORDER BY id")
		var byID []string
		for rs.Next() {
			var s string
			rs.Scan(&s)
			byID = append(byID, s)
		}
		rs.Close()
		rs, _ = tx.Query("SELECT aggregate_id FROM tandem_outbox WHERE aggregate_id LIKE 'poc-multi-%' ORDER BY seq")
		var bySeq []string
		for rs.Next() {
			var s string
			rs.Scan(&s)
			bySeq = append(bySeq, s)
		}
		rs.Close()
		tx.Rollback()
		if !slices.IsSorted(byID) || !slices.IsSorted(bySeq) {
			violations++
		}
	}
	t.Logf("multi-row VALUES: %d of %d rounds of %d rows broke list order in id or seq", violations, rounds, rows)
}

func TestWakeupAsTextArray(t *testing.T) {
	ctx := context.Background()
	listener, err := pgx.Connect(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Close(ctx)
	if _, err := listener.Exec(ctx, "LISTEN tandem_wakeup"); err != nil {
		t.Fatal(err)
	}
	for _, driver := range drivers {
		db := open(t, driver)
		resetBucketCount(t, db, "256")
		w, err := tandem.NewWriter(ctx, db, tandem.WithWakeup())
		if err != nil {
			t.Fatal(err)
		}
		msgs := []tandem.Message{
			{AggregateID: "poc-wake-a", AggregateType: "T", Seq: tandem.Unsequenced(), Payload: []byte(`{}`)},
			{AggregateID: "poc-wake-b", AggregateType: "T", Seq: tandem.Unsequenced(), Payload: []byte(`{}`)},
			{AggregateID: "poc-wake-a", AggregateType: "T", Seq: tandem.Unsequenced(), Payload: []byte(`{}`)},
		}
		tx, _ := db.Begin()
		if err := w.InsertAll(ctx, tx, msgs); err != nil {
			t.Fatalf("%s: %v", driver, err)
		}
		tx.Commit()
		var got []string
		for {
			wctx, cancel := context.WithTimeout(ctx, 500*time.Millisecond)
			n, err := listener.WaitForNotification(wctx)
			cancel()
			if err != nil {
				break
			}
			got = append(got, n.Payload)
		}
		want := []string{fmt.Sprint(tandem.BucketFor("poc-wake-a", 256)), fmt.Sprint(tandem.BucketFor("poc-wake-b", 256))}
		t.Logf("%s: notifications %v, expected buckets %v", driver, got, want)
		if !slices.Equal(got, want) {
			t.Errorf("%s: wakeup payloads differ", driver)
		}
		db.Exec("DELETE FROM tandem_outbox WHERE aggregate_id LIKE 'poc-wake-%'")
	}
}

func TestBucketCountReconciliation(t *testing.T) {
	ctx := context.Background()
	db := open(t, "pgx")
	resetBucketCount(t, db, "")
	if _, err := tandem.NewWriter(ctx, db, tandem.WithBucketCount(100)); err != nil {
		t.Fatalf("seeding a fresh database: %v", err)
	}
	_, err := tandem.NewWriter(ctx, db)
	t.Logf("default 256 against stored 100: %v", err)
	if !errors.Is(err, tandem.ErrBucketCount) {
		t.Error("conflict not reported")
	}
	// Two writers seeding a fresh database at once with different counts: exactly one must fail.
	bad := 0
	for round := 0; round < 30; round++ {
		resetBucketCount(t, db, "")
		var wg sync.WaitGroup
		errs := make([]error, 2)
		for i, n := range []int32{100, 256} {
			wg.Add(1)
			go func() {
				defer wg.Done()
				_, errs[i] = tandem.NewWriter(ctx, db, tandem.WithBucketCount(n))
			}()
		}
		wg.Wait()
		if (errs[0] == nil) == (errs[1] == nil) {
			bad++
		}
	}
	t.Logf("concurrent seeding: %d of 30 rounds did not have exactly one failure", bad)
	resetBucketCount(t, db, "256")
}

func TestLockedWriteSerialises(t *testing.T) {
	ctx := context.Background()
	for _, driver := range drivers {
		db := open(t, driver)
		resetBucketCount(t, db, "256")
		w, _ := tandem.NewWriter(ctx, db)
		m := tandem.Message{AggregateID: "poc-locked", AggregateType: "T", Seq: tandem.ManagedSeq(), Payload: []byte(`{}`), LockedWrite: true}
		tx1, _ := db.Begin()
		if err := w.Insert(ctx, tx1, m); err != nil {
			t.Fatal(err)
		}
		done := make(chan time.Time)
		go func() {
			tx2, _ := db.Begin()
			_ = w.Insert(ctx, tx2, m)
			done <- time.Now()
			tx2.Rollback()
		}()
		time.Sleep(300 * time.Millisecond)
		released := time.Now()
		tx1.Rollback()
		finished := <-done
		t.Logf("%s: second writer finished %v after the first released (blocked until then: %v)", driver, finished.Sub(released).Round(time.Millisecond), finished.After(released))
		if finished.Before(released) {
			t.Errorf("%s: the second writer did not wait", driver)
		}
	}
}
