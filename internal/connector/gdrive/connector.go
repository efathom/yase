// Package gdrive implements a YASE connector for Google Drive / Google Docs.
// Uses service account authentication (JWT bearer) for server-to-server access.
// Incremental sync via Drive API changes.list with page tokens.
package gdrive

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/efathom/yase/pkg/connector"
	"golang.org/x/oauth2/google"
	"google.golang.org/api/drive/v3"
	"google.golang.org/api/option"
)

const connectorType = "gdrive"

func init() {
	connector.DefaultRegistry.Register(connectorType, NewConnector)
}

// Config holds Google Drive-specific configuration.
type Config struct {
	ServiceAccountJSON string   `json:"service_account_json"` // path to service account key file or JSON string
	FolderIDs          []string `json:"folder_ids"`           // specific folders to sync (empty = all)
	SharedDriveIDs     []string `json:"shared_drive_ids"`     // shared drives to include
	MIMETypeFilter     []string `json:"mime_type_filter"`     // e.g., ["application/vnd.google-apps.document", "application/pdf"]
	PageSize           int64    `json:"page_size"`            // default 100
	MaxObjectSize      int64    `json:"max_object_size"`      // max bytes per file (0 = unlimited)
}

// Connector implements the YASE Connector interface for Google Drive.
type Connector struct {
	id      string
	config  Config
	service *drive.Service
}

// NewConnector creates a Google Drive connector from configuration.
func NewConnector(cfg connector.ConnectorConfig) (connector.Connector, error) {
	var config Config
	configBytes, _ := json.Marshal(cfg.Config)
	if err := json.Unmarshal(configBytes, &config); err != nil {
		return nil, fmt.Errorf("parse gdrive config: %w", err)
	}

	if config.ServiceAccountJSON == "" {
		return nil, fmt.Errorf("gdrive: service_account_json is required")
	}
	if config.PageSize <= 0 {
		config.PageSize = 100
	}

	ctx := context.Background()

	// Parse service account credentials
	creds, err := google.CredentialsFromJSON(ctx, []byte(config.ServiceAccountJSON),
		drive.DriveReadonlyScope)
	if err != nil {
		return nil, fmt.Errorf("gdrive credentials: %w", err)
	}

	svc, err := drive.NewService(ctx, option.WithCredentials(creds))
	if err != nil {
		return nil, fmt.Errorf("gdrive service: %w", err)
	}

	return &Connector{
		id:      cfg.Type,
		config:  config,
		service: svc,
	}, nil
}

func (c *Connector) ID() string          { return c.id }
func (c *Connector) DisplayName() string { return "Google Drive" }

func (c *Connector) Spec() *connector.ConnectorSpec {
	return &connector.ConnectorSpec{
		AuthMethods: []connector.AuthMethod{connector.AuthOAuth2},
		SyncModes:   []connector.SyncMode{connector.FullRefresh, connector.Incremental},
	}
}

func (c *Connector) Validate(ctx context.Context) error {
	_, err := c.service.About.Get().Fields("user").Context(ctx).Do()
	return err
}

func (c *Connector) Discover(ctx context.Context) (*connector.Catalog, error) {
	return &connector.Catalog{
		Streams: []connector.Stream{
			{Name: "files", SupportedSyncModes: []connector.SyncMode{connector.FullRefresh, connector.Incremental}, DefaultCursorField: "modifiedTime"},
		},
	}, nil
}

func (c *Connector) Read(ctx context.Context, streams []connector.ConfiguredStream, state *connector.SyncState) (<-chan connector.Record, <-chan error) {
	records := make(chan connector.Record, 100)
	errs := make(chan error, 10)

	go func() {
		defer close(records)
		defer close(errs)

		for _, stream := range streams {
			if stream.Name == "files" {
				if stream.SyncMode == connector.Incremental {
					c.readChanges(ctx, state, records, errs)
				} else {
					c.readAllFiles(ctx, records, errs)
				}
			}
		}
	}()

	return records, errs
}

func (c *Connector) Close() error { return nil }

func (c *Connector) readAllFiles(ctx context.Context, records chan<- connector.Record, errs chan<- error) {
	query := "trashed = false"
	if len(c.config.FolderIDs) > 0 {
		var parts []string
		for _, fid := range c.config.FolderIDs {
			parts = append(parts, fmt.Sprintf("'%s' in parents", fid))
		}
		query += " and (" + strings.Join(parts, " or ") + ")"
	}
	if len(c.config.MIMETypeFilter) > 0 {
		var parts []string
		for _, mime := range c.config.MIMETypeFilter {
			parts = append(parts, fmt.Sprintf("mimeType = '%s'", mime))
		}
		query += " and (" + strings.Join(parts, " or ") + ")"
	}

	pageToken := ""
	for {
		call := c.service.Files.List().
			Q(query).
			Fields("nextPageToken, files(id, name, mimeType, modifiedTime, webViewLink, owners, size)").
			PageSize(c.config.PageSize).
			Context(ctx)
		if pageToken != "" {
			call = call.PageToken(pageToken)
		}
		if len(c.config.SharedDriveIDs) > 0 {
			call = call.SupportsAllDrives(true).IncludeItemsFromAllDrives(true)
		}

		resp, err := call.Do()
		if err != nil {
			errs <- fmt.Errorf("list files: %w", err)
			return
		}

		for _, file := range resp.Files {
			record, err := c.fileToRecord(ctx, file)
			if err != nil {
				errs <- fmt.Errorf("process file %s: %w", file.Id, err)
				continue
			}
			if record != nil {
				connector.SendRecord(ctx, records, *record)
			}
		}

		pageToken = resp.NextPageToken
		if pageToken == "" {
			break
		}
	}
}

func (c *Connector) readChanges(ctx context.Context, state *connector.SyncState, records chan<- connector.Record, errs chan<- error) {
	// Get start page token
	var startToken string
	if raw := state.GetStreamState("files"); raw != nil {
		var cursorState struct {
			PageToken string `json:"page_token"`
		}
		json.Unmarshal(raw, &cursorState)
		startToken = cursorState.PageToken
	}

	if startToken == "" {
		// First run: get initial token and do full read
		resp, err := c.service.Changes.GetStartPageToken().Context(ctx).Do()
		if err != nil {
			errs <- fmt.Errorf("get start page token: %w", err)
			return
		}
		c.readAllFiles(ctx, records, errs)
		cursorJSON, _ := json.Marshal(map[string]string{"page_token": resp.StartPageToken})
		state.SetStreamState("files", cursorJSON)
		return
	}

	// Incremental: fetch changes since last token
	pageToken := startToken
	var latestToken string
	for {
		call := c.service.Changes.List(pageToken).
			Fields("nextPageToken, newStartPageToken, changes(fileId, removed, file(id, name, mimeType, modifiedTime, webViewLink, owners, size))").
			PageSize(c.config.PageSize).
			Context(ctx)

		resp, err := call.Do()
		if err != nil {
			errs <- fmt.Errorf("list changes: %w", err)
			return
		}

		for _, change := range resp.Changes {
			if change.Removed || change.File == nil {
				if change.FileId != "" {
					connector.SendRecord(ctx, records, connector.Record{
						StreamName: "files",
						ID:         change.FileId,
						Action:     connector.Delete,
						EmittedAt:  time.Now(),
					})
				}
				continue
			}

			record, err := c.fileToRecord(ctx, change.File)
			if err != nil {
				errs <- fmt.Errorf("process change %s: %w", change.FileId, err)
				continue
			}
			if record != nil {
				connector.SendRecord(ctx, records, *record)
			}
		}

		if resp.NewStartPageToken != "" {
			latestToken = resp.NewStartPageToken
		}
		pageToken = resp.NextPageToken
		if pageToken == "" {
			break
		}
	}

	// Persist the cursor from the final page only so a multi-page sync window
	// never skips changes beyond the first page.
	if latestToken != "" {
		cursorJSON, _ := json.Marshal(map[string]string{"page_token": latestToken})
		state.SetStreamState("files", cursorJSON)
	}
}

func (c *Connector) fileToRecord(ctx context.Context, file *drive.File) (*connector.Record, error) {
	var content []byte
	var mimeType string

	if file.MimeType == "application/vnd.google-apps.document" {
		// Export Google Docs as plain text
		resp, err := c.service.Files.Export(file.Id, "text/plain").Context(ctx).Download()
		if err != nil {
			return nil, fmt.Errorf("export doc: %w", err)
		}
		defer resp.Body.Close()
		content, err = connector.ReadLimited(resp.Body, c.config.MaxObjectSize)
		if err != nil {
			return nil, fmt.Errorf("export doc %s: %w", file.Id, err)
		}
		mimeType = "text/plain"
	} else if file.MimeType == "application/vnd.google-apps.spreadsheet" ||
		file.MimeType == "application/vnd.google-apps.presentation" {
		// Export as plain text
		resp, err := c.service.Files.Export(file.Id, "text/plain").Context(ctx).Download()
		if err != nil {
			return nil, fmt.Errorf("export: %w", err)
		}
		defer resp.Body.Close()
		content, err = connector.ReadLimited(resp.Body, c.config.MaxObjectSize)
		if err != nil {
			return nil, fmt.Errorf("export %s: %w", file.Id, err)
		}
		mimeType = "text/plain"
	} else if isDownloadable(file.MimeType) {
		// Download binary files (PDF, DOCX, etc.)
		resp, err := c.service.Files.Get(file.Id).Context(ctx).Download()
		if err != nil {
			return nil, fmt.Errorf("download: %w", err)
		}
		defer resp.Body.Close()
		content, err = connector.ReadLimited(resp.Body, c.config.MaxObjectSize)
		if err != nil {
			return nil, fmt.Errorf("download %s: %w", file.Id, err)
		}
		mimeType = file.MimeType
	} else {
		return nil, nil // skip unsupported types
	}

	metadata := map[string]string{
		"title":         file.Name,
		"mime_type":     file.MimeType,
		"modified_time": file.ModifiedTime,
		"source":        "gdrive",
	}
	if len(file.Owners) > 0 {
		metadata["owner"] = file.Owners[0].DisplayName
	}

	url := file.WebViewLink
	if url == "" {
		url = "https://drive.google.com/file/d/" + file.Id
	}

	return &connector.Record{
		StreamName: "files",
		ID:         file.Id,
		Content:    content,
		MimeType:   mimeType,
		URL:        url,
		Metadata:   metadata,
		Action:     connector.Upsert,
		EmittedAt:  time.Now(),
	}, nil
}

func isDownloadable(mimeType string) bool {
	downloadable := []string{
		"application/pdf",
		"text/plain", "text/html", "text/markdown", "text/csv",
		"application/vnd.openxmlformats-officedocument.wordprocessingml.document",
		"application/vnd.openxmlformats-officedocument.presentationml.presentation",
		"application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
	}
	for _, d := range downloadable {
		if mimeType == d {
			return true
		}
	}
	return false
}
