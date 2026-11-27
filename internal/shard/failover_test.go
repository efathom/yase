package shard

import (
	"context"
	"net"
	"testing"
	"time"

	ingestionv1 "github.com/efathom/yase/proto/v1"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
)

func TestShardSearchFailsOverToHealthyReplica(t *testing.T) {
	engine := createTestEngine(t, 20, 16)
	srv := NewSearchServer()
	srv.RegisterEngine(0, engine)
	g := grpc.NewServer()
	ingestionv1.RegisterShardSearchServiceServer(g, srv)
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	go g.Serve(lis)
	defer g.Stop()

	// A replica address nobody listens on.
	dead, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	deadAddr := dead.Addr().String()
	dead.Close()

	tests := []struct {
		name     string
		replicas []string
		wantErr  bool
	}{
		{"dead replica first", []string{deadAddr, lis.Addr().String()}, false},
		{"dead replica last", []string{lis.Addr().String(), deadAddr}, false},
		{"all replicas dead", []string{deadAddr}, true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := NewRemoteShardSearcher()
			defer s.Close()
			s.UpdateTopology(map[uint32][]string{0: tt.replicas})
			for i := 0; i < 4; i++ { // exercise every round-robin start
				ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
				res, err := s.SearchShard(ctx, 0, "document", make([]float32, 16), nil, 5)
				cancel()
				if tt.wantErr {
					assert.Error(t, err)
					continue
				}
				require.NoError(t, err)
				assert.NotEmpty(t, res)
			}
		})
	}
}
