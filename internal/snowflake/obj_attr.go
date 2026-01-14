package snowflake

import (
	"context"
	"database/sql"
)

type ObjAttr struct {
	ObjectType 	ObjType
	Owner 		string
	GrantsTo	map[Mode]map[Privilege]struct{} // has to be unique for every interface; hence for every accountobjs; we can't copy it around
}

func (o ObjAttr) setGrantTo(m Mode, p Privilege) {
	if o.GrantsTo == nil { o.GrantsTo = map[Mode]map[Privilege]struct{}{} }
	if _, ok := o.GrantsTo[m]; !ok { o.GrantsTo[m] = map[Privilege]struct{}{} }
	o.GrantsTo[m][p] = struct{}{}
}

func (o ObjAttr) hasGrantTo(m Mode, p Privilege) {
	if v, ok := o.GrantsTo[m] {
		_, ok = v[p]
		return ok
	}
}

func (o ObjAttr) doGrant(ctx context.Context, cnf *Config, conn *sql.DB, db string, schema string, obj string, role string) error {
	if !o.hasGrantTo(ModeRead, PrvSelect) {
		if err := GrantToRole{
				Privilege: PrvSelect,
				GrantedOn: ObjTpObject,
				Database: db,
				Schema: schema,
				Object: obj,
		}.DoGrantToDBRole(ctx, cnf, conn, db, role); err != nil {
			return err
		}
	}
	if !o.hasGrantTo(ModeRead, PrvReferences) {
		if err := GrantToRole{
				Privilege: PrvReferences,
				GrantedOn: ObjTpObject,
				Database: db,
				Schema: schema,
				Object: obj,
		}.DoGrantToDBRole(ctx, cnf, conn, db, role); err != nil {
			return err
		}
	}
	return nil
}
