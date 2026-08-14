// Package sharepoint implements a YASE connector for SharePoint Online
// via the Microsoft Graph API. Supports delta queries for incremental sync.
package sharepoint

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/efathom/yase/pkg/connector"
)

const connectorType = "sharepoint"

func init() {
	connector.DefaultRegistry.Register(connectorType, NewConnector)
}

// Config holds SharePoint-specific configuration.
type Config struct {
	TenantID      string   `json:"tenant_id"`       // Azure AD tenant ID
	SiteIDs       []string `json:"site_ids"`        // SharePoint site IDs (empty = root site)
	DriveIDs      []string `json:"drive_ids"`       // specific drives (empty = default document library)
	FolderPath    string   `json:"folder_path"`     // filter to specific folder path
	PageSize      int      `json:"page_size"`       // default 200
	MaxObjectSize int64    `json:"max_object_size"` // max bytes per file (0 = unlimited)
}

// Connector implements the YASE Connector interface for SharePoint.
type Connector struct {
	id     string
	config Config
	http   *connector.HTTPConnectorBase
}

// NewConnector creates a SharePoint connector from configuration.
func NewConnector(cfg connector.ConnectorConfig) (connector.Connector, error) {
	var config Config
	configBytes, _ := json.Marshal(cfg.Config)
	if err := json.Unmarshal(configBytes, &config); err != nil {
		return nil, fmt.Errorf("parse sharepoint config: %w", err)
	}

	if config.PageSize <= 0 {
		config.PageSize = 200
	}

	auth, err := connector.NewAuthenticator(cfg.Auth)
	if err != nil {
		return nil, fmt.Errorf("sharepoint auth: %w", err)
	}

	// Microsoft Graph API base URL
	httpBase := connector.NewHTTPConnectorBase("https://graph.microsoft.com/v1.0", auth, 5)

	return &Connector{
		id:     cfg.Type,
		config: config,
		http:   httpBase,
	}, nil
}

func (c *Connector) ID() string          { return c.id }
func (c *Connector) DisplayName() string { return "SharePoint" }

func (c *Connector) Spec() *connector.ConnectorSpec {
	return &connector.ConnectorSpec{
		AuthMethods: []connector.AuthMethod{connector.AuthOAuth2, connector.AuthBearer},
		SyncModes:   []connector.SyncMode{connector.FullRefresh, connector.Incremental},
	}
}

func (c *Connector) Validate(ctx context.Context) error {
	_, _, err := c.http.DoRequest(ctx, "GET", "/me", nil)
	if err != nil {
		// App-only auth may not have /me — try sites
		_, _, err = c.http.DoRequest(ctx, "GET", "/sites/root", nil)
	}
	return err
}

func (c *Connector) Discover(ctx context.Context) (*connector.Catalog, error) {
	return &connector.Catalog{
		Streams: []connector.Stream{
			{Name: "files", SupportedSyncModes: []connector.SyncMode{connector.FullRefresh, connector.Incremental}, DefaultCursorField: "lastModifiedDateTime"},
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
					c.readDelta(ctx, state, records, errs)
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
	driveIDs := c.config.DriveIDs
	if len(driveIDs) == 0 {
		// Get default drive for each site
		for _, siteID := range c.config.SiteIDs {
			id, err := c.getDefaultDriveID(ctx, siteID)
			if err != nil {
				errs <- err
				continue
			}
			driveIDs = append(driveIDs, id)
		}
		if len(driveIDs) == 0 {
			// Root site default drive
			id, err := c.getDefaultDriveID(ctx, "root")
			if err != nil {
				errs <- err
				return
			}
			driveIDs = append(driveIDs, id)
		}
	}

	for _, driveID := range driveIDs {
		c.listDriveItems(ctx, driveID, records, errs)
	}
}

func (c *Connector) getDefaultDriveID(ctx context.Context, siteID string) (string, error) {
	path := fmt.Sprintf("/sites/%s/drive", siteID)
	_, body, err := c.http.DoRequest(ctx, "GET", path, nil)
	if err != nil {
		return "", fmt.Errorf("get drive for site %s: %w", siteID, err)
	}
	var resp struct {
		ID string `json:"id"`
	}
	_ = json.Unmarshal(body, &resp)
	return resp.ID, nil
}

func (c *Connector) listDriveItems(ctx context.Context, driveID string, records chan<- connector.Record, errs chan<- error) {
	path := fmt.Sprintf("/drives/%s/root/children?$top=%d", driveID, c.config.PageSize)
	if c.config.FolderPath != "" {
		path = fmt.Sprintf("/drives/%s/root:/%s:/children?$top=%d", driveID, c.config.FolderPath, c.config.PageSize)
	}

	for path != "" {
		_, body, err := c.http.DoRequest(ctx, "GET", path, nil)
		if err != nil {
			errs <- fmt.Errorf("list drive items: %w", err)
			return
		}

		var resp graphListResponse
		if err := json.Unmarshal(body, &resp); err != nil {
			errs <- fmt.Errorf("parse drive items: %w", err)
			return
		}

		for _, item := range resp.Value {
			if item.File == nil {
				continue // skip folders
			}

			record, err := c.itemToRecord(ctx, driveID, &item)
			if err != nil {
				errs <- err
				continue
			}
			if record != nil {
				records <- *record
			}
		}

		path = resp.NextLink
		if path != "" {
			// NextLink is a full URL — strip the base
			path = strings.TrimPrefix(path, "https://graph.microsoft.com/v1.0")
		}
	}
}

func (c *Connector) readDelta(ctx context.Context, state *connector.SyncState, records chan<- connector.Record, errs chan<- error) {
	driveIDs := c.config.DriveIDs
	if len(driveIDs) == 0 {
		for _, siteID := range c.config.SiteIDs {
			id, err := c.getDefaultDriveID(ctx, siteID)
			if err != nil {
				errs <- err
				continue
			}
			driveIDs = append(driveIDs, id)
		}
		if len(driveIDs) == 0 {
			id, err := c.getDefaultDriveID(ctx, "root")
			if err != nil {
				errs <- err
				return
			}
			driveIDs = append(driveIDs, id)
		}
	}

	for _, driveID := range driveIDs {
		// Delta links are drive-specific; key the cursor per drive so one
		// drive's link is never reused for another drive.
		stateKey := "files:" + driveID
		var deltaLink string
		if raw := state.GetStreamState(stateKey); raw != nil {
			var cursorState struct {
				DeltaLink string `json:"delta_link"`
			}
			_ = json.Unmarshal(raw, &cursorState)
			deltaLink = cursorState.DeltaLink
		}

		path := deltaLink
		if path == "" {
			path = fmt.Sprintf("/drives/%s/root/delta?$top=%d", driveID, c.config.PageSize)
		} else {
			path = strings.TrimPrefix(path, "https://graph.microsoft.com/v1.0")
		}

		for path != "" {
			_, body, err := c.http.DoRequest(ctx, "GET", path, nil)
			if err != nil {
				errs <- fmt.Errorf("delta query: %w", err)
				return
			}

			var resp graphDeltaResponse
			_ = json.Unmarshal(body, &resp)

			for _, item := range resp.Value {
				if item.Deleted != nil {
					connector.SendRecord(ctx, records, connector.Record{
						StreamName: "files",
						ID:         item.ID,
						Action:     connector.Delete,
						EmittedAt:  time.Now(),
					})
					continue
				}
				if item.File == nil {
					continue
				}

				record, err := c.itemToRecord(ctx, driveID, &item)
				if err != nil {
					errs <- err
					continue
				}
				if record != nil {
					connector.SendRecord(ctx, records, *record)
				}
			}

			if resp.DeltaLink != "" {
				cursorJSON, _ := json.Marshal(map[string]string{"delta_link": resp.DeltaLink})
				state.SetStreamState(stateKey, cursorJSON)
				path = ""
			} else {
				path = resp.NextLink
				if path != "" {
					path = strings.TrimPrefix(path, "https://graph.microsoft.com/v1.0")
				}
			}
		}
	}
}

func (c *Connector) itemToRecord(ctx context.Context, driveID string, item *graphDriveItem) (*connector.Record, error) {
	// Download file content
	downloadPath := fmt.Sprintf("/drives/%s/items/%s/content", driveID, item.ID)
	resp, body, err := c.http.DoRequest(ctx, "GET", downloadPath, nil)
	if err != nil {
		return nil, fmt.Errorf("download %s: %w", item.Name, err)
	}
	if c.config.MaxObjectSize > 0 && int64(len(body)) > c.config.MaxObjectSize {
		return nil, fmt.Errorf("content exceeds maximum size of %d bytes", c.config.MaxObjectSize)
	}

	mimeType := item.File.MimeType
	if mimeType == "" && resp != nil {
		mimeType = resp.Header.Get("Content-Type")
	}

	metadata := map[string]string{
		"title":         item.Name,
		"size":          fmt.Sprintf("%d", item.Size),
		"modified_time": item.LastModified,
		"source":        "sharepoint",
	}
	if item.CreatedBy != nil {
		metadata["author"] = item.CreatedBy.User.DisplayName
	}

	url := item.WebURL
	if url == "" {
		url = fmt.Sprintf("https://graph.microsoft.com/v1.0/drives/%s/items/%s", driveID, item.ID)
	}

	return &connector.Record{
		StreamName: "files",
		ID:         item.ID,
		Content:    body,
		MimeType:   mimeType,
		URL:        url,
		Metadata:   metadata,
		Action:     connector.Upsert,
		EmittedAt:  time.Now(),
	}, nil
}

// --- Microsoft Graph API response types ---

type graphListResponse struct {
	Value    []graphDriveItem `json:"value"`
	NextLink string           `json:"@odata.nextLink"`
}

type graphDeltaResponse struct {
	Value     []graphDriveItem `json:"value"`
	NextLink  string           `json:"@odata.nextLink"`
	DeltaLink string           `json:"@odata.deltaLink"`
}

type graphDriveItem struct {
	ID           string         `json:"id"`
	Name         string         `json:"name"`
	Size         int64          `json:"size"`
	WebURL       string         `json:"webUrl"`
	LastModified string         `json:"lastModifiedDateTime"`
	File         *graphFile     `json:"file"`
	Deleted      *interface{}   `json:"deleted"`
	CreatedBy    *graphIdentity `json:"createdBy"`
}

type graphFile struct {
	MimeType string `json:"mimeType"`
}

type graphIdentity struct {
	User struct {
		DisplayName string `json:"displayName"`
	} `json:"user"`
}
