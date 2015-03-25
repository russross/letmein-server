package letmein

import (
	"bytes"
	"compress/gzip"
	"encoding/gob"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"sort"
	"strings"
	"time"

	"appengine"
	"appengine/datastore"
)

func init() {
	http.HandleFunc("/api/v1noauth/sync", syncNoAuthHandler)
}

const (
	maxClockDrift        = time.Hour
	maxVerifyLen         = 32
	deletedRecordTimeout = time.Hour * 24 * 365
)

type SyncRecord struct {
	Verify       string    `datastore:"verify,noindex"`
	CreatedAt    time.Time `datastore:"created_at,noindex"`
	ModifiedAt   time.Time `datastore:"modified_at,noindex"`
	ModifyCount  int       `datastore:"modify_count,noindex"`
	AccessedAt   time.Time `datastore:"accessed_at,noindex"`
	AccessCount  int       `datastore:"access_count,noindex"`
	ProfileCount int       `datastore:"profile_count,noindex"`
	DeletedCount int       `datastore:"deleted_count,noindex"`
}

type Client struct {
	Name     string     `json:"name" datastore:"-"`
	Verify   string     `json:"verify" datastore:"-"`
	Profiles []*Profile `json:"profiles,omitempty" datastore:"profiles,noindex"`

	ModifiedAt     *time.Time `json:"modified_at,omitempty" datastore:"-"`
	SyncedAt       *time.Time `json:"synced_at,omitempty" datastore:"-"`
	PreviousSyncAt *time.Time `json:"previous_sync_at,omitempty" datastore:"-"`
}

func (elt *Client) Load(c <-chan datastore.Property) error {
	// make sure channel is properly drained
	defer func() {
		for _ = range c {
		}
	}()

	for p := range c {
		switch p.Name {
		case "profiles":
			elt.Profiles = nil
			unzipper, err := gzip.NewReader(bytes.NewReader(p.Value.([]byte)))
			if err != nil {
				return err
			}
			decoder := gob.NewDecoder(unzipper)
			if err := decoder.Decode(&elt.Profiles); err != nil {
				unzipper.Close()
				return err
			}
			if err := unzipper.Close(); err != nil {
				return err
			}
		}
	}
	return nil
}

func (elt *Client) Save(c chan<- datastore.Property) error {
	defer close(c)

	buf := new(bytes.Buffer)
	zipper := gzip.NewWriter(buf)
	encoder := gob.NewEncoder(zipper)
	if err := encoder.Encode(elt.Profiles); err != nil {
		zipper.Close()
		return err
	}
	if err := zipper.Close(); err != nil {
		return err
	}

	c <- datastore.Property{Name: "profiles", Value: buf.Bytes(), NoIndex: true}
	return nil
}

type Profile struct {
	UUID        string    `json:"uuid"`
	Name        string    `json:"name,omitempty"`
	Username    string    `json:"username,omitempty"`
	URL         string    `json:"url,omitempty"`
	Generation  int       `json:"generation,omitempty"`
	Length      int       `json:"length,omitempty"`
	Lower       bool      `json:"lower,omitempty"`
	Upper       bool      `json:"upper,omitempty"`
	Digits      bool      `json:"digits,omitempty"`
	Punctuation bool      `json:"punctuation,omitempty"`
	Spaces      bool      `json:"spaces,omitempty"`
	Include     string    `json:"include,omitempty"`
	Exclude     string    `json:"exclude,omitempty"`
	ModifiedAt  time.Time `json:"modified_at"`
}

func (p *Profile) String() string {
	if p.Length == 0 {
		return "[deleted]"
	}

	charset := ""
	if p.Lower {
		charset += "a–z"
	}
	if p.Upper {
		charset += "A–Z"
	}
	if p.Digits {
		charset += "0–9"
	}
	if p.Punctuation {
		charset += "[punct]"
	}
	if p.Spaces {
		charset += "[space]"
	}
	if p.Include != "" {
		charset += "+[" + p.Include + "]"
	}
	if p.Exclude != "" {
		charset += "-[" + p.Exclude + "]"
	}
	return fmt.Sprintf("[%s] user:%s gen:%d len:%d chars:%s", p.URL, p.Username, p.Generation, p.Length, charset)
}

var never = time.Date(1970, 1, 1, 0, 0, 0, 0, time.UTC)

func syncNoAuthHandler(w http.ResponseWriter, r *http.Request) {
	ctx := appengine.NewContext(r)
	now := time.Now().Round(time.Millisecond)

	// parse request
	if !strings.Contains(r.Header.Get("Content-Type"), "application/json") {
		ctx.Errorf("request Content-Type must be application/json")
		http.Error(w, "request Content-Type must be application/json", http.StatusBadRequest)
		return
	}
	client := new(Client)
	decoder := json.NewDecoder(r.Body)
	defer r.Body.Close()
	if err := decoder.Decode(client); err != nil {
		ctx.Errorf("decoding request: %v", err)
		http.Error(w, "error decoding request", http.StatusBadRequest)
		return
	}
	if client.ModifiedAt != nil {
		*client.ModifiedAt = client.ModifiedAt.Round(time.Millisecond)
	}
	if client.SyncedAt == nil {
		client.SyncedAt = &now
	} else {
		*client.SyncedAt = client.SyncedAt.Round(time.Millisecond)
	}
	if client.PreviousSyncAt != nil {
		*client.PreviousSyncAt = client.PreviousSyncAt.Round(time.Millisecond)
	}

	if len(client.Verify) == 0 || len(client.Verify) > maxVerifyLen {
		ctx.Errorf("verify field must be between 1 and %d characters long", maxVerifyLen)
		http.Error(w, "verify field of invalid length", http.StatusBadRequest)
		return
	}

	// for now, take the client key to be the Name field
	clientKey := client.Name
	ctx.Infof("client key is %q", clientKey)

	// adjust for clock drift, but not if it is too far off
	delta := now.Sub(*client.SyncedAt)
	if delta > maxClockDrift || -delta > maxClockDrift {
		delta = 0
	} else {
		// adjust all client-supplied timestamps
		ctx.Infof("delta %v\n", delta)
		if client.ModifiedAt != nil {
			*client.ModifiedAt = client.ModifiedAt.Add(delta)
		}
		*client.SyncedAt = client.SyncedAt.Add(delta)
		// client.PreviousSyncAt came from us, so do not adjust it
		for _, elt := range client.Profiles {
			elt.ModifiedAt = elt.ModifiedAt.Add(delta)
		}
	}

	// clean up profiles
	for _, elt := range client.Profiles {
		// round all client-supplied timestamps
		elt.ModifiedAt = elt.ModifiedAt.Round(time.Millisecond)
		if elt.Length <= 0 {
			// deleted record
			elt.Name = ""
			elt.Username = ""
			elt.URL = ""
			elt.Generation = 0
			elt.Length = 0
			elt.Lower = false
			elt.Upper = false
			elt.Digits = false
			elt.Punctuation = false
			elt.Spaces = false
			elt.Include = ""
			elt.Exclude = ""
		}
	}

	topts := &datastore.TransactionOptions{}
	err := datastore.RunInTransaction(ctx, func(c appengine.Context) error {
		// start with the sync record
		syncKey := datastore.NewKey(c, "SyncRecord_v1noauth", clientKey, 0, nil)
		sync := new(SyncRecord)
		if err := datastore.Get(c, syncKey, sync); err != nil {
			if err != datastore.ErrNoSuchEntity {
				c.Errorf("DB error getting client sync record: %v", err)
				return err
			}

			// special case: new client
			c.Infof("new client sync record")
			sync = &SyncRecord{
				Verify:       client.Verify,
				CreatedAt:    now,
				ModifiedAt:   never,
				ModifyCount:  0,
				AccessedAt:   never,
				AccessCount:  0,
				ProfileCount: 0,
				DeletedCount: 0,
			}
		}

		if client.Verify != sync.Verify {
			c.Infof("verify mismatch: client says %s, expecting %s", client.Verify, sync.Verify)
			return errors.New("Verify mismatch: check master password")
		}

		sync.AccessedAt = now
		sync.AccessCount++

		// no action required?
		if len(client.Profiles) == 0 && client.PreviousSyncAt.After(sync.ModifiedAt) {
			// write back the sync record and quit
			if _, err := datastore.Put(c, syncKey, sync); err != nil {
				c.Errorf("DB error putting client sync record: %v", err)
				return err
			}

			// tell the client not to make any changes
			client.Profiles = []*Profile{}
			client.ModifiedAt = nil
			client.SyncedAt = nil
			client.PreviousSyncAt = &now

			return nil
		}

		// fetch the user's profile list
		profileKey := datastore.NewKey(c, "Profiles_v1noauth", clientKey, 0, syncKey)
		server := new(Client)
		if err := datastore.Get(c, profileKey, server); err != nil {
			if err != datastore.ErrNoSuchEntity {
				c.Errorf("DB error getting client record: %v", err)
				return err
			}

			// special case: new client
			c.Infof("new client record")
			server = &Client{
				Profiles: []*Profile{},
			}
		}

		// put everything in maps
		uuidmap := make(map[string]bool)
		uuids := []string{}
		clientMap := make(map[string]*Profile)
		serverMap := make(map[string]*Profile)
		for _, p := range client.Profiles {
			if !p.ModifiedAt.After(never) {
				// ignore this profile with no valid ModifiedAt timestamp
				c.Debugf("profile %q [uuid=%s] uploaded with invalid ModifiedAt timestamp", clientKey, p.UUID)
				continue
			}
			if client.PreviousSyncAt != nil && !p.ModifiedAt.After(*client.PreviousSyncAt) {
				c.Warningf("profile %q [uuid=%s] has ModifiedAt timestamp [%v] before PreviousSyncAt [%v]",
					clientKey, p.UUID, p.ModifiedAt, client.PreviousSyncAt)
			}
			if !uuidmap[p.UUID] {
				uuidmap[p.UUID] = true
				uuids = append(uuids, p.UUID)
			}
			clientMap[p.UUID] = p
		}
		for _, p := range server.Profiles {
			if !uuidmap[p.UUID] {
				uuidmap[p.UUID] = true
				uuids = append(uuids, p.UUID)
			}
			serverMap[p.UUID] = p
		}
		uuidmap = nil
		sort.Strings(uuids)

		// sync one profile at a time
		serverResult := []*Profile{}
		clientResult := []*Profile{}
		modified := false
		count, deletedCount := 0, 0
		for _, id := range uuids {
			pc, ps := clientMap[id], serverMap[id]
			var merged *Profile
			switch {
			case pc == nil && ps == nil:
				// nothing to see here
				ctx.Warningf("merging uuid=%s, but neither server nor client knows about it", id)
				continue

			case pc == nil:
				// client missing this profile
				merged = ps

				// should we send this to the client?
				if client.PreviousSyncAt == nil || merged.ModifiedAt.After(*client.PreviousSyncAt) {
					// exception: skip delete records for new clients
					if merged.Length > 0 || client.PreviousSyncAt != nil {
						c.Infof("client getting new profile: %s", merged)
						clientResult = append(clientResult, merged)
					}
				}

			case ps == nil:
				// server missing this profile
				merged = pc
				merged.ModifiedAt = now
				modified = true
				c.Infof("server getting new profile: %s", merged)

			default:
				// both have the profile
				if ps.Length > 0 && pc.ModifiedAt.After(ps.ModifiedAt) {
					merged = pc
					merged.ModifiedAt = now
					modified = true
					c.Infof("server getting updated profile: %s", merged)
				} else {
					merged = ps
					clientResult = append(clientResult, merged)
					c.Infof("client getting updated profile: %s", merged)
				}
			}
			if merged.ModifiedAt.After(now) {
				merged.ModifiedAt = now
			}

			serverResult = append(serverResult, merged)
			if merged.Length > 0 {
				count++
			} else {
				deletedCount++
			}
		}

		// save the changes
		if modified {
			sync.ModifiedAt = now
			sync.ModifyCount++
		}
		sync.ProfileCount = count
		sync.DeletedCount = deletedCount
		server.Profiles = serverResult

		if _, err := datastore.Put(c, syncKey, sync); err != nil {
			c.Errorf("DB error putting client sync record: %v", err)
			return err
		}
		if modified {
			if _, err := datastore.Put(c, profileKey, server); err != nil {
				c.Errorf("DB error putting profiles: %v", err)
				return err
			}
		}

		// prepare client result
		client.ModifiedAt = nil
		client.SyncedAt = nil
		client.PreviousSyncAt = &now
		client.Profiles = clientResult
		for _, elt := range client.Profiles {
			elt.ModifiedAt = now
		}

		return nil
	}, topts)
	if err != nil {
		ctx.Errorf("Error in syncNoAuthHandler transaction: %v", err)
		http.Error(w, "error handling sync", http.StatusInternalServerError)
		return
	}

	// write out the JSON response
	w.Header().Add("Content-Type", "application/json")
	encoder := json.NewEncoder(w)
	if err := encoder.Encode(client); err != nil {
		ctx.Errorf("Error writing/encoding response: %v", err)
	}
}
