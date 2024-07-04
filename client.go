// Copyright © 2023 Meroxa, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package schemaregistry

import (
	"context"
	"fmt"
	"log/slog"

	"github.com/conduitio/conduit-schema-registry/internal"
	"github.com/twmb/franz-go/pkg/sr"
)

// Client is a schema registry client that caches schemas. It is safe for
// concurrent use.
type Client struct {
	logger *slog.Logger
	client sr.Client

	cache internal.SchemaCache
}

// NewClient creates a new client using the provided logger and schema registry
// client options.
func NewClient(logger *slog.Logger, opts ...sr.ClientOpt) (*Client, error) {
	defaultOpts := []sr.ClientOpt{
		sr.UserAgent("conduit"),
		sr.URLs(), // disable default URL
	}

	client, err := sr.NewClient(append(defaultOpts, opts...)...)
	if err != nil {
		return nil, fmt.Errorf("failed to create schema registry client: %w", err)
	}

	return &Client{
		logger: logger.With("component", "schemaregistry.Client"),
		client: *client,
	}, nil
}

// CreateSchema checks if the schema is already registered in the cache and
// returns the associated sr.SubjectSchema if it is found. Otherwise, the schema
// is sent to the schema registry and stored in the cache, if the registration
// was successful.
func (c *Client) CreateSchema(ctx context.Context, subject string, schema sr.Schema) (sr.SubjectSchema, error) {
	hit := true
	ss, err := c.cache.GetBySubjectText(subject, schema.Schema, func() (sr.SubjectSchema, error) {
		hit = false // disable output for hit

		// Check if the subject exists. Ignore the error as this is not critical
		// for creating a schema, we assume the subject doesn't exist in case of an error.
		versions, _ := c.client.SubjectVersions(sr.WithParams(ctx, sr.ShowDeleted), subject)
		subjectExists := len(versions) > 0

		ss, err := c.client.CreateSchema(ctx, subject, schema)
		if err != nil {
			return ss, fmt.Errorf("failed to create schema with subject %q: %w", subject, err)
		}

		if !subjectExists {
			// TODO this is here because we need that in Conduit, we can't expect
			//  all records to have compatible schemas. That said, we should
			//  probably remove this and handle it in Conduit, as this repository
			//  should be generic.
			// if we created the schema we need to disable compatibility checks
			result := c.client.SetCompatibility(ctx, sr.SetCompatibility{
				Level: sr.CompatNone,
			}, subject)
			for _, res := range result {
				if res.Err != nil {
					// only log error, don't return it
					c.logger.ErrorContext(ctx, "failed to set compatibility to none, might create issues if an incompatible change happens in the future", "subject", subject, "error", res.Err)
				}
			}
		}
		return ss, nil
	})
	if err != nil {
		return sr.SubjectSchema{}, fmt.Errorf("failed to create schema with subject %q: %w", subject, err)
	}
	c.logger.DebugContext(ctx, "CreateSchema", "subject", subject, "cache-hit", hit)
	return ss, nil
}

// SchemaByID checks if the schema is already registered in the cache and
// returns the associated sr.Schema if it is found. Otherwise, the schema is
// retrieved from the schema registry and stored in the cache.
// Note that the returned schema does not contain a subject and version, so the
// cache will not have an effect on methods that return a sr.SubjectSchema.
func (c *Client) SchemaByID(ctx context.Context, id int) (sr.Schema, error) {
	hit := true
	s, err := c.cache.GetByID(id, func() (sr.Schema, error) {
		hit = false
		return c.client.SchemaByID(ctx, id)
	})
	if err != nil {
		return sr.Schema{}, fmt.Errorf("failed to get schema with ID %q: %w", id, err)
	}
	c.logger.DebugContext(ctx, "SchemaByID", "id", id, "cache-hit", hit)
	return s, nil
}

// SchemaBySubjectVersion checks if the schema is already registered in the
// cache and returns the associated sr.SubjectSchema if it is found. Otherwise,
// the schema is retrieved from the schema registry and stored in the cache.
func (c *Client) SchemaBySubjectVersion(ctx context.Context, subject string, version int) (sr.SubjectSchema, error) {
	// TODO handle latest version separately, let caller define timeout after
	//  which the latest cached version should be downloaded again from upstream
	hit := true
	ss, err := c.cache.GetBySubjectVersion(subject, version, func() (sr.SubjectSchema, error) {
		hit = false
		return c.client.SchemaByVersion(ctx, subject, version)
	})
	if err != nil {
		return sr.SubjectSchema{}, fmt.Errorf("failed to get schema with subject %q and version %q: %w", subject, version, err)
	}
	c.logger.DebugContext(ctx, "SchemaBySubjectVersion", "subject", subject, "version", version, "cache-hit", hit)
	return ss, nil
}
