# Snowflake package

This package can match actual objects (tables / views) in a Snowflake account to the object expressions defined in the YAML.

It can store counts of matched objects in a Snowflake table.

It also can generate roles and grant privileges for read access to the matched objects according to the YAML.

## Counts

## Role management

For each data product, per dtap environment, a read-only business role is created. It uses a prefix like `_grupr_` so that grupr can enumerate
all the roles it is managing. 

All objects that belong to a data product are grouped in per-product per-dtap database roles. If the data product has interfaces, for each interface
also one or more database roles are created (one per database). 

Every data product business role is granted all database roles that group its own objects, and is granted all database roles that group objects
of interfaces it consumes.
