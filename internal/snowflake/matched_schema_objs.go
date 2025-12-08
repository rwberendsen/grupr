package snowflake

type matchedSchemaObjs struct {
	version int
	objects map[ObjKey]struct
}
