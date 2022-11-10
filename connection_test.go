package dbsql

import (
	"context"
	"database/sql/driver"
	"fmt"
	"github.com/databricks/databricks-sql-go/internal/cli_service"
	"github.com/databricks/databricks-sql-go/internal/client"
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestConn_ExecContext(t *testing.T) {

	t.Run("executeStatement should err when ExecuteStatement fails", func(t *testing.T) {
		var executeStatementCount int
		executeStatement := func(ctx context.Context, req *cli_service.TExecuteStatementReq) (r *cli_service.TExecuteStatementResp, err error) {
			executeStatementCount++
			return nil, fmt.Errorf("TClIServiceClient.ExecuteStatement errror")
		}
		testClient := &rowTestClient{
			fExecuteStatement: executeStatement,
		}
		testConn := &conn{
			client: &client.ThriftServiceClient{
				&testClient,
			},
		}
		_, err := testConn.executeStatement(context.Background(), "select 1", []driver.NamedValue{})
		assert.AnError(err)
	})
}
