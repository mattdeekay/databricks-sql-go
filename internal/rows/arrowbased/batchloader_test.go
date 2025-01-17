package arrowbased

import (
	"bytes"
	"context"
	"fmt"
	"github.com/apache/arrow/go/v12/arrow"
	"github.com/apache/arrow/go/v12/arrow/array"
	"github.com/apache/arrow/go/v12/arrow/ipc"
	"github.com/apache/arrow/go/v12/arrow/memory"
	dbsqlerr "github.com/databricks/databricks-sql-go/errors"
	"github.com/databricks/databricks-sql-go/internal/cli_service"
	"github.com/databricks/databricks-sql-go/internal/config"
	dbsqlerrint "github.com/databricks/databricks-sql-go/internal/errors"
	"github.com/pkg/errors"
	"github.com/stretchr/testify/assert"
	"net/http"
	"net/http/httptest"
	"reflect"
	"testing"
	"time"
)

func generateMockArrowBytes() []byte {
	mem := memory.NewCheckedAllocator(memory.NewGoAllocator())
	defer mem.AssertSize(nil, 0)

	fields := []arrow.Field{
		{Name: "id", Type: arrow.PrimitiveTypes.Int32},
		{Name: "name", Type: arrow.BinaryTypes.String},
	}
	schema := arrow.NewSchema(fields, nil)

	builder := array.NewRecordBuilder(mem, schema)
	defer builder.Release()

	builder.Field(0).(*array.Int32Builder).AppendValues([]int32{1, 2, 3}, nil)
	builder.Field(1).(*array.StringBuilder).AppendValues([]string{"one", "two", "three"}, nil)

	record := builder.NewRecord()
	defer record.Release()

	var buf bytes.Buffer
	w := ipc.NewWriter(&buf, ipc.WithSchema(record.Schema()))
	if err := w.Write(record); err != nil {
		return nil
	}
	if err := w.Close(); err != nil {
		return nil
	}
	return buf.Bytes()
}

func TestBatchLoader(t *testing.T) {
	var handler func(w http.ResponseWriter, r *http.Request)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		handler(w, r)
	}))
	defer server.Close()
	testTable := []struct {
		name             string
		response         func(w http.ResponseWriter, r *http.Request)
		linkExpired      bool
		expectedResponse []*sparkArrowBatch
		expectedErr      error
	}{
		{
			name: "cloud-fetch-happy-case",
			response: func(w http.ResponseWriter, r *http.Request) {
				w.WriteHeader(http.StatusOK)
				_, err := w.Write(generateMockArrowBytes())
				if err != nil {
					panic(err)
				}
			},
			linkExpired: false,
			expectedResponse: []*sparkArrowBatch{
				{
					arrowRecordBytes: generateMockArrowBytes(),
					hasSchema:        true,
					rowCount:         3,
					startRow:         0,
					endRow:           2,
				},
			},
			expectedErr: nil,
		},
		{
			name: "cloud-fetch-expired_link",
			response: func(w http.ResponseWriter, r *http.Request) {
				w.WriteHeader(http.StatusOK)
				_, err := w.Write(generateMockArrowBytes())
				if err != nil {
					panic(err)
				}
			},
			linkExpired:      true,
			expectedResponse: nil,
			expectedErr:      errors.New(dbsqlerr.ErrLinkExpired),
		},
		{
			name: "cloud-fetch-http-error",
			response: func(w http.ResponseWriter, r *http.Request) {
				w.WriteHeader(http.StatusInternalServerError)
			},
			linkExpired:      false,
			expectedResponse: nil,
			expectedErr:      dbsqlerrint.NewDriverError(context.TODO(), errArrowRowsCloudFetchDownloadFailure, nil),
		},
	}

	for _, tc := range testTable {
		t.Run(tc.name, func(t *testing.T) {
			handler = tc.response

			expiryTime := time.Now()
			// If link expired, subtract 1 sec from current time to get expiration time
			if tc.linkExpired {
				expiryTime = expiryTime.Add(-1 * time.Second)
			} else {
				expiryTime = expiryTime.Add(1 * time.Second)
			}

			cu := &cloudURL{
				TSparkArrowResultLink: &cli_service.TSparkArrowResultLink{
					FileLink:   server.URL,
					ExpiryTime: expiryTime.Unix(),
				},
			}

			ctx := context.Background()
			cfg := config.WithDefaults()

			resp, err := cu.Fetch(ctx, cfg)

			if !reflect.DeepEqual(resp, tc.expectedResponse) {
				t.Errorf("expected (%v), got (%v)", tc.expectedResponse, resp)
			}
			if !errors.Is(err, tc.expectedErr) {
				assert.EqualErrorf(t, err, fmt.Sprintf("%v", tc.expectedErr), "expected (%v), got (%v)", tc.expectedErr, err)
			}
		})
	}
}
