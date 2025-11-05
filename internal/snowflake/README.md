# Snowflake package

This package can match actual objects (tables / views) in a Snowflake account
to the object expressions defined in the YAML.

It can generate roles and grant privileges for read and write access to the matched
objects according to the YAML.

It also can store counts of matched objects in a Snowflake table.

## Role management

### Access management model
Grupr, when used for access management in Snowflake, creates and manages
privileges of two business roles per data product, per DTAP environment; a
read-only role, and a write role. The read-only role is intended to be granted
to people. The write role is intended to be granted to service accounts. In
non-production environments, the write role may also be granted to people.
During severe production incidents, even in production the write role may be
granted to people temporarily.

The read-only role will be granted read access to: 

- All objects in the data product, in the DTAP environment
- All objects of interfaces consumed by the data product, in the relevant DTAP
  environments, according to the DTAP mapping on the consume relationship.

The write role will be granted everything the read-only role has been granted,
and also ownership of all object in the data product, in the DTAP environment.

#### Secondary roles
In Snowflake, by default, when users connect, all the roles they have been
granted are activated in the session all at once. One role acts as the primary
role, it is the role assumed by the user. That role is the only one that can
authorize CREATE statements, if it has been granted this privilege on any
object. The other roles are secondary roles, and any other privileges these
roles have been granted, the user can use them in the session as well.

Grupr is designed for settings where different teams work on different
products, where products are designed with privacy in mind (privacy by design).
This means products can combine certain data sources only. If a skilled
employee is a member of several teams, or even if one team works on several
data products, still, in any particular session, only one product role should
be active. Because only a product role can give the context in which the
specific combination of data sources is authorised. 

Also generic roles, such as an orchestrator role for all ETL pipelines, 
should ideally not own all the objects that happened to be created by jobs
on the orchestration platform. These objects, part of different data products,
should be owned by specific data product roles, so that they can be managed
effectively as being part of that data product. While you may require in your
organisation an actual service account per data product, it may suffice also
to still use a simple generic service account, and grant it a separate role
for each data product. This way, you don't have that many users to manage, 
but you still enjoy some separattion of concerns. Similarly, a visualisation
platform should ideally not have access to the union of all the objects used 
in any of the reports built on this platform. These reports may have very
different classifications and sensitivity. Again, perhaps managing a multitude
of users may not be necessary, but at least, separate access to objects in 
distinct roles to assume. This works, as long as no secondary roles are activated
by these service accounts.

For these reasons, it is
recommended to *set an account wide default session policy that would not allow
secondary roles to be activated*. If needed in particular contexts, this can be
overridden on the user level. Or, if you have certain roles in your
organisation that you would like everybody to be able to combine with product
roles, then you can allow the activation of only those roles.

### Read-only priveleges
For each data product, per dtap environment, a read-only business role is
created. It uses a configurable prefix like `_X_` so that grupr can enumerate
all the roles it is managing. 

All objects that belong to a data product are grouped in per-product per-dtap
database roles. If the data product has interfaces, for each interface also one
or more database roles are created (one per database). 

Every data product business role is granted all database roles that group its
own objects, and is granted all database roles that group objects of interfaces
it consumes.

Example database role names:

```
_X_MY_PRODUCT_X_MY_DTAP_X_MY_INTERFACE_X_R
_X_MY_PRODUCT_X_MY_DTAP_X_R
```

Note that `_X_` acts like a (configurable) infix. We have here a role name that
has three parts: a product name, a dtap, an interface name, and a mode (`R`,
for read-only, in this case).  Also note that here we use `_X_` also as a
prefix.  This way we can mark all roles that grupr is managing.

Privileges granted to the database roles: USAGE on DATABASE and SCHEMA; SELECT
and REFERENCES on objects. If all objects in a schema or database are matched
in the YAML object expression, then these privileges are granted on FUTURE
objects as well.

An example business role name:

```
_X_MY_PRODUCT_X_MY_DTAP_X_R
```
Note that this name is exactly the same as database role names, but the latter
are unique only in the scope of a particular database, so there is no name
clash.

Privileges granted to the business roles: USAGE of the database roles of the
product, USAGE of all database roles of consumed interfaces, and USAGE on a
virtual warehouse. Read-only roles are intended to be assumed by people. Note
that the relationship between which read-only business roles use which
warehouse is not maintained in the main Grupr yaml format.  To the extent
possible, the Grupr yaml format is to be generic enough so that it could be
employed on other data-intensive compute platforms. We can provide a Snowflake
specific mini yaml format for specifying this relationship.

Note that Grupr will not revoke any existing privileges on the objects that
have been granted to roles not managed by Grupr. Grupr can be used in an
environment where also other tools are used to manage access, and Grupr can be
used in a progressive fashion, gradually increasing its scope when you find it
simplifies operations. If other tools, however, start revoking privileges to
roles managed by Grupr, just because Grupr is granting privileges on objects
also managed by these other tools, then care must be taken while mixing tools,
such that they operate on a distinct set of objects.

### Write privileges
Like for read-only privileges, grupr will create per product per DTAP
environment one business role:

```
_X_MY_PRODUCT_X_MY_DTAP_X_W
```

with `W` this time, for writing.

Privileges granted to this role:

- USAGE of the read-only database roles of the product
- USAGE of the read-only database roles of the interfaces consumed by the product
- OWNERSHIP of objects: TODO: test this: if CREATE is allowed in a database
  role, and a business role that has been granted the CREATE privilege through
  the database role: will the database role be listed as owner of the object or
  will the business role be listed? In the former case, we could group OWNERSHIP
  grants in database roles, it would be easier to manage.  But it seems more
  likely that Snowflake would list a business role as the owner, in that case, we
  should directly transfer ownership on objects to this business role.
- If all objects in a schema or database are matched in the YAML object
  expression, then OWNERSHIP is granted on future objects as well.
- CREATE SCHEMA on the database level
- CREATE TABLE / VIEW on the schema level (as of now, dynamic, event, and
  iceberg tables are not yet in scope of Grupr)

Transferring ownership is tricky business. For certain object types, specific
requirements apply, such as that pipes must be paused, and tasks are suspended
while ownership is being transferred. So we can imagine that for this type of
scenario, Grupr would have to run in an interactive fashion, where ops would
have time to pause pipes / suspend tasks while grupr does its job. But for
simple TABLES and VIEWS, grupr can do it in an unattended fashion. 

While transferring ownership, Grupr will make sure that in effect, roles which
currently hold ownership do not lose ownership. Grupr ensures this by first
granting current owners USAGE on the product business role. In time, ops
personnel should revoke these grants, finally confiscating objects from the
original owners. But this has to be done offline, after changing automated
processes to assume the new product business roles.

Also, while transferring ownership, Grupr will COPY GRANTS, so that outbound
grants are copied. To authorize these copied grants on behalf of the new owner,
Grupr has to activate as a secondary role the new owner: the product business
role. That's why the grupr service account user has to have a session policy
overriding the recommended account level session policy that allows no
secondary roles; the Grupr service account should be allowed to activate all
roles it has been granted as a secondary role. As an intricate detail, a side
effect of the COPY GRANTS action is that if any of the outbound privileges
had been re-granted by the grantees, then these grants can no longer be revoked
in one go by a REVOKE FROM ROLE statemetent with the CASCADE option, because
they have a different grantor (the previous owner).

Note that, apart from OWNERSHIP, Grupr will leave additional outbound
privileges granted on objects alone, it will even copy them, ensuring they are
kept. Grupr will only manage privileges granted to the (database) roles it has
created and owns. This means in some respects Grupr tries to be non-invasive,
while at the same time, it tries to be decisive, and streamline ownership a
lot.

### Removing objects from the YAML
If we remove objects from the YAML, then Grupr will take action accordingly.
If we remove object matching expressions from a product, then Grupr will revoke
privileges on those objects from the product role. If we move an object
expression from one product to another, then Grupr will first grant the other
product read-only privileges, before revoking them from the the first product,
to prevent downtime of consuming processes. When it comes to transferring
ownership while moving an object expression from product A to B, then yes, this
may have to be coordinated in a tight time period with a change in the
processes creating tables. 

As an example, what happens when we move an object X from product A to product B?
Grupr will:

- grant read access on X to product B (by granting the database role)
- revoke read access on X from product A (by revoking the database role)
- transfer ownership of X from product A to product B, copying outbound grants

Between these steps, odd situations can exist, where product B has read access,
but no ownership yet. Eventually though, even if Grupr for some reason crashed
in between, it will converge on the desired end state.

If we remove an entire product, then Grupr will indeed drop the product
(database) roles. This can have a severe impact. If objects still exist in the
database, object that were previously matched by object expressions of the now
dropped product, but are now no longer matched by any product, then what
happens?  Ownership will now be transferred to the role that DROPs the
previously owning role: the Grupr role will become the new owner. If the
product role had been granted to other roles, those roles now lose all the
privileges they had been granted through it. And if the former product role was
the grantor of any grants to other roles, then these grants will be revoked.
This could impact business continuity. As a best practice, if you caused the
product role Grupr was managing to appear as the grantor or grantee in any
additional grants, on top of what Grupr did, then before you remove this
product from the YAML, you should evaluate what to do about these additional
grants. If you do remove a product from the YAML, then, in a subsequent run,
the previsouly matched objects could be included in a new product, and then
Grupr would transfer ownership of the objects from its own role to the new
product role.

### Discussion

All of the above amount to a scheme in which Grupr can come in in an existing
account and help an operations team to streamline access management, while
minimizing interruption of business continuity. The result will be a situation
in which product business roles will be enabled to do a lot indeed, even create
schemas, own schema's etc. So in that sense, it is quite lenient. One cannot
use Grupr to enforce strict policies such as using only managed access schemas,
or only allowing creation of certain object types. Indeed, if a product role
owns a schema, it can create objects of any type, and become owner of it, and
even grant privileges on these objects to other roles.

We do believe that Snowflake should offer account administrator teams ways to
define policies to enforce that only approved implementation patterns are used
in an organization; but, blocking CREATE SCHEMA would not be the ideal way.
This is because CREATE SCHEMA is an essential privilege to have for many best
practices. For example, atomic deployments, where we first clone a production
schema to a staging schema (copying grants), run a transformation job
consisting of numerous queries in batch fashion, and then swap the production
schema with the staging schema using an ALTER SCHEMA ... SWAP WITH statement.

What operations teams can expect from adopting grupr is a way of working in
which they can report to enterprise governance teams on the data they are 
managing in their Snowflake accounts on topics like:

- Which entities own what data?
- What data products do we have?
- What interfaces or data sets do we have?
- What is the classification of data in specific products and interfaces?
- What is the lineage on a high level: which products consume what interfaces?
  Note that many tools exist that provide table level lineage or even column
  level lineage: but the information is so dense that it can be hard to
  reason about it or grasp it, or present it in digestible ways.
- Who has access to this data and why?

Also, account administrator teams can expect to establish streamlined access
management processes. Rather than judging requests by specific employees to
access specific tables on a case by case basis, it would be part of data
product design to state what data sets a data product is allowed to consume.
This would only change periodically, during product development, and is
reflected in the Grupr YAML. After that, it would be decided which teams are
working on which data products. This, too, would not change that frequently,
and it would be the result of deliberation at the management level. It, too,
can be maintained in a simple YAML format. Day to day access requests to
operations teams would then be reduced to only this trivial question: is person
X a member of team Y? A simple approval of the manager of team Y would suffice;
or even a quick glance at an org chart, if this is an officially registered
team. Being registered in a YAML as a team member would then result in your
user being granted the read-only business roles of all the products that team
is working on. No more special employees with special access, all access would
be based on team membership.
