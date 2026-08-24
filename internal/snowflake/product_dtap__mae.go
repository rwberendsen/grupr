package snowflake

import (
	"context"
	"database/sql"
	"fmt"
	"iter"
	"log"
	"slices"
	"strings"

	"github.com/rwberendsen/grupr/internal/semantics"
	"github.com/rwberendsen/grupr/internal/util"
)

/*
In product_dtap__mae.go, we have ProductDTAP methods that deal with managing access exclusively
*/

func (pd *ProductDTAP) manageAccessExclusively(ctx context.Context, semCnf *semantics.Config, cnf *Config, conn *sql.DB) error {
	pd.Interface.aggAccountObjects {
	}
}
