package letmein

import (
	"bytes"
	"compress/gzip"
	"encoding/gob"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"sort"
	"strings"
	"time"

	"github.com/GoogleCloudPlatform/go-endpoints/endpoints"

	"appengine"
	"appengine/datastore"
	"appengine/user"
)

var config = struct {
	OAuthClientID string
}{}

type SyncService struct{}

func init() {
	// load config
	fp, err := os.Open("config.json")
	if err != nil {
		log.Fatal(err)
	}
	defer fp.Close()
	if err = json.NewDecoder(fp).Decode(&config); err != nil {
		log.Fatalf("JSON error decoding config: %v", err)
	}

	// register debugging API (no authorization, simple JSON request)
	http.HandleFunc("/api/v1noauth/sync", syncNoAuthHandler)

	// register the endpoints service handler
	syncService := &SyncService{}
	api, err := endpoints.RegisterService(syncService, "letmein", "v1", "Letmein Sync Service", true)
	if err != nil {
		log.Fatalf("Register service: %v", err)
	}

	// configure the method
	method := api.MethodByName("Sync")
	info := method.Info()
	info.Name = "sync"
	info.HTTPMethod = "POST"
	info.Path = "letmein"
	info.Desc = "Sync profiles"

	endpoints.HandleHTTP()
}

const (
	maxClockDrift        = 7 * 24 * time.Hour
	maxVerifyLen         = 32
	deletedRecordTimeout = time.Hour * 24 * 365
)

type SyncRecord struct {
	Email        string    `datastore:"email,noindex"`
	Verify       string    `datastore:"verify,noindex"`
	CreatedAt    time.Time `datastore:"created_at,noindex"`
	ModifiedAt   time.Time `datastore:"modified_at,noindex"`
	ModifyCount  int       `datastore:"modify_count,noindex"`
	AccessedAt   time.Time `datastore:"accessed_at,noindex"`
	AccessCount  int       `datastore:"access_count,noindex"`
	ProfileCount int       `datastore:"profile_count,noindex"`
	DeletedCount int       `datastore:"deleted_count,noindex"`
}

type SyncRequest struct {
	Name     string     `json:"name,omitempty" datastore:"-"`
	Verify   string     `json:"verify,omitempty" datastore:"-"`
	Profiles []*Profile `json:"profiles,omitempty" datastore:"profiles,noindex"`

	SyncedAt       *time.Time `json:"synced_at,omitempty" datastore:"-"`
	PreviousSyncAt *time.Time `json:"previous_sync_at,omitempty" datastore:"-"`
}

func (elt *SyncRequest) Load(c <-chan datastore.Property) error {
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

func (elt *SyncRequest) Save(c chan<- datastore.Property) error {
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

func syncNoAuthHandler(w http.ResponseWriter, r *http.Request) {
	defer r.Body.Close()
	c := appengine.NewContext(r)

	if r.Method != "POST" {
		http.Error(w, "non-POST request not found", http.StatusNotFound)
		return
	}

	// parse request
	if !strings.Contains(r.Header.Get("Content-Type"), "application/json") {
		c.Errorf("request Content-Type must be application/json")
		http.Error(w, "request Content-Type must be application/json", http.StatusBadRequest)
		return
	}
	if !strings.Contains(r.Header.Get("Accept"), "application/json") {
		c.Errorf("Accept header must include application/json")
		http.Error(w, "Accept header must include application/json", http.StatusBadRequest)
		return
	}
	client := new(SyncRequest)

	// grap the entire request body in case we need to log it later
	body := new(bytes.Buffer)
	if _, err := io.Copy(body, r.Body); err != nil {
		c.Errorf("error reading body of request: %v", err)
		http.Error(w, "error reading body of request: "+err.Error(), http.StatusInternalServerError)
		return
	}
	reqBody := body.String()

	decoder := json.NewDecoder(body)
	if err := decoder.Decode(client); err != nil {
		c.Errorf("decoding request: %v", err)
		c.Debugf("request body:\n%s", reqBody)
		http.Error(w, "error decoding request: "+err.Error(), http.StatusBadRequest)
		return
	}

	response, err := syncCommon(c, client, "", "noauth")

	if err != nil {
		c.Debugf("request body:\n%s", reqBody)
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	// write out the JSON response
	w.Header().Add("Content-Type", "application/json")
	encoded, err := json.MarshalIndent(response, "", "    ")
	if err != nil {
		c.Errorf("Error encoding JSON response: %v", err)
		http.Error(w, "Error encoding JSON response: "+err.Error(), http.StatusInternalServerError)
		return
	}
	encoded = append(encoded, '\n')
	if _, err = w.Write(encoded); err != nil {
		c.Errorf("Error writing response: %v", err)
		http.Error(w, "Error writing response: "+err.Error(), http.StatusInternalServerError)
		return
	}
	c.Debugf("request body:\n%s", reqBody)
}

func (s *SyncService) Sync(c endpoints.Context, client *SyncRequest) (*SyncRequest, error) {
	u, err := endpoints.CurrentBearerTokenUser(c,
		[]string{endpoints.EmailScope},
		[]string{config.OAuthClientID, endpoints.APIExplorerClientID})
	if err != nil {
		return nil, err
	}
	response, err := syncCommon(c, client, u.Email, "")
	if err != nil {
		c.Errorf("%v", err)
		return nil, err
	}
	return response, nil
}

func resetHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != "POST" {
		http.Error(w, "non-POST request not found", http.StatusNotFound)
		return
	}

	suffix := ""

	ctx := appengine.NewContext(r)

	// get the user
	u := user.Current(ctx)
	if u == nil {
		url, err := user.LoginURL(ctx, r.URL.String())
		if err != nil {
			ctx.Errorf("login redirect error: %v", err)
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		w.Header().Set("Location", url)
		w.WriteHeader(http.StatusFound)
		return
	}

	clientKey := u.ID
	if clientKey == "" {
		clientKey = u.Email
	}
	ctx.Debugf("client key is %q", clientKey)

	topts := &datastore.TransactionOptions{}
	err := datastore.RunInTransaction(ctx, func(c appengine.Context) error {
		syncKey := datastore.NewKey(c, "SyncRecord_v1"+suffix, clientKey, 0, nil)
		profileKey := datastore.NewKey(c, "Profiles_v1"+suffix, clientKey, 0, syncKey)
		err := datastore.DeleteMulti(c, []*datastore.Key{syncKey, profileKey})
		if err == datastore.ErrNoSuchEntity {
			c.Infof("entity not found for reset request: %v", err)
			return nil
		} else if err != nil {
			return err
		}
		return nil
	}, topts)
	if err != nil {
		ctx.Errorf("Error in reset transaction: %v", err)
		http.Error(w, "error handling reset: "+err.Error(), http.StatusInternalServerError)
	}
}

// syncCommon does most of the work of syncing profiles.
// It is called by the noauth handler as well as the main endpoints API handler.
func syncCommon(ctx appengine.Context, client *SyncRequest, email string, suffix string) (*SyncRequest, error) {
	now := time.Now().Round(time.Millisecond)
	never := time.Date(1970, 1, 1, 0, 0, 0, 0, time.UTC)

	// round all timestamps to the nearest millisecond
	// to avoid confusing Java and JavaScript clients that
	// prefer millisecond precision
	if client.SyncedAt == nil {
		client.SyncedAt = &now
	} else {
		*client.SyncedAt = client.SyncedAt.Round(time.Millisecond)
	}
	if client.PreviousSyncAt != nil {
		*client.PreviousSyncAt = client.PreviousSyncAt.Round(time.Millisecond)
	}

	// check for sane verify field
	if len(client.Verify) == 0 || len(client.Verify) > maxVerifyLen {
		return nil, fmt.Errorf("verify field must be between 1 and %d characters long", maxVerifyLen)
	}

	// we track clients by email address normally, but in the noauth version
	// we accept a tracking key from the client
	clientKey := email

	// get the ID/email from the request for noauth requests
	if clientKey == "" {
		clientKey = client.Name
		email = client.Name
	}
	if clientKey == "" {
		return nil, fmt.Errorf("unable to determine user ID/Email/Name")
	}
	ctx.Debugf("client key is %q", clientKey)

	// adjust all client-supplied timestamps for clock drift
	// but not if it is too far off
	delta := now.Sub(*client.SyncedAt)
	if delta > maxClockDrift || -delta > maxClockDrift {
		delta = 0
	} else {
		ctx.Debugf("time delta %v\n", delta)
	}

	// client.PreviousSyncAt came from us, so do not adjust it
	*client.SyncedAt = client.SyncedAt.Add(delta)

	// clean up profiles
	var modifiedAt *time.Time
	for _, elt := range client.Profiles {
		if elt.ModifiedAt == nil {
			elt.ModifiedAt = &never
		} else {
			*elt.ModifiedAt = elt.ModifiedAt.Add(delta)
		}
		if elt.ModifiedAt.After(now) {
			elt.ModifiedAt = &now
		}
		elt.SyncedAt = now

		if err := elt.Validate(); err != nil {
			return nil, fmt.Errorf("Invalid profile %s: %v", elt, err)
		}

		// find the latest timestamp from all the profiles the client supplied
		if modifiedAt == nil || modifiedAt.Before(*elt.ModifiedAt) {
			modifiedAt = elt.ModifiedAt
		}
	}

	// run the main sync operation in a transaction
	topts := &datastore.TransactionOptions{}
	err := datastore.RunInTransaction(ctx, func(c appengine.Context) error {
		// start with the sync record
		syncKey := datastore.NewKey(c, "SyncRecord_v1"+suffix, clientKey, 0, nil)
		sync := new(SyncRecord)
		if err := datastore.Get(c, syncKey, sync); err != nil {
			if err != datastore.ErrNoSuchEntity {
				return fmt.Errorf("DB error getting client sync record: %v", err)
			}

			// special case: new client
			// if this is not a first sync, reject the request
			if client.PreviousSyncAt != nil {
				return fmt.Errorf("SyncRequest record not found, but your request suggests this is not the first sync (PreviousSyncAt is not nil)")
			}

			c.Infof("new client sync record")
			sync = &SyncRecord{
				Email:        email,
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

		// insist on a verify field match
		if client.Verify != sync.Verify {
			c.Infof("verify mismatch: client says %s, expecting %s", client.Verify, sync.Verify)
			return errors.New("Verify mismatch: check master password")
		}
		if sync.Email != email && email != "" {
			c.Infof("email mismatch: was %s, updating to %s", sync.Email, email)
			sync.Email = email
		}

		sync.AccessedAt = now
		sync.AccessCount++

		// no action required?
		if len(client.Profiles) == 0 && client.PreviousSyncAt != nil && client.PreviousSyncAt.After(sync.ModifiedAt) {
			// write back the sync record and quit
			if _, err := datastore.Put(c, syncKey, sync); err != nil {
				return fmt.Errorf("DB error putting client sync record: %v", err)
			}

			// tell the client not to make any changes
			client.Profiles = []*Profile{}
			client.SyncedAt = nil
			client.PreviousSyncAt = &now

			c.Infof("simple sync was sufficient; skipping check of all profiles")
			return nil
		}

		// fetch the user's profile list
		profileKey := datastore.NewKey(c, "Profiles_v1"+suffix, clientKey, 0, syncKey)
		server := new(SyncRequest)
		if err := datastore.Get(c, profileKey, server); err != nil {
			if err != datastore.ErrNoSuchEntity {
				return fmt.Errorf("DB error getting client record: %v", err)
			}

			// special case: new client
			c.Infof("new client record")
			server = &SyncRequest{
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
			if p.SyncedAt.Before(sync.CreatedAt) && p.ModifiedAt != nil {
				p.SyncedAt = *p.ModifiedAt
			}
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
				if client.PreviousSyncAt == nil || merged.SyncedAt.After(*client.PreviousSyncAt) {
					// exception: skip delete records for new clients
					if !merged.IsDeleted() || client.PreviousSyncAt != nil {
						c.Infof("client getting new profile: %s", merged)
						clientResult = append(clientResult, merged)
					}
				}

			case ps == nil:
				// server missing this profile
				merged = pc
				modified = true
				c.Infof("server getting new profile: %s", merged)

			default:
				// both have the profile
				if ps.Length > 0 && pc.ModifiedAt.After(*ps.ModifiedAt) {
					merged = pc
					modified = true
					c.Infof("server getting updated profile: %s", merged)
				} else {
					merged = ps
					clientResult = append(clientResult, merged)
					c.Infof("client getting updated profile: %s", merged)
				}
			}

			// everything gets saved on the server
			serverResult = append(serverResult, merged)
			if merged.IsDeleted() {
				deletedCount++
			} else {
				count++
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
			return fmt.Errorf("DB error putting client sync record: %v", err)
		}
		if modified {
			if _, err := datastore.Put(c, profileKey, server); err != nil {
				return fmt.Errorf("DB error putting profiles: %v", err)
			}
		} else {
			c.Infof("no changes to profile list; skipping profile write")
		}

		// prepare client result
		client.SyncedAt = nil
		client.PreviousSyncAt = &now
		client.Profiles = clientResult
		for _, elt := range client.Profiles {
			elt.ModifiedAt = &now
		}

		return nil
	}, topts)
	if err != nil {
		return nil, fmt.Errorf("error in sync transaction: %v", err)
	}

	return client, nil
}
