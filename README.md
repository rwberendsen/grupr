# grupr

A tool to group database objects and manage them more easily.

Grupr is a CLI (command line interface) tool that works with a YAML format for
grouping database objects into data products and interfaces.  It targets
typical ANSI SQL environments with databases, schema's, and objects (tables and
views). You can annotate high level lineage in grupr YAML, where data products
consume interfaces. 

YAML is friendly to human eyes, so the collection of YAML files give you a
understanding of your data platform at a high level. Since YAML is also machine
readable, there are use cases for automation, once you have defined
your data products. Currently, grupr can manage
access to data objects in Snowflake.

Because grupr uses a text based data format, you can take
your metadata with you wherever you go.

Let's have a bit closer look at data products, interfaces, and the relations
between them.

## Data products
An example product.yaml file may look like this:

```
product:
  id: crm
  classification: l3
  dtaps:
    prod: p
    non_prod: [d, t, a]
  user_groups:
    - fr
    - de
  user_group_column: user_group
  objects:
    - '{{ .DTAP }}_gold.crm.*'
```

There are a few things to note here.  First you assign all `objects` in schema
`p_gold.crm` (for the production environment) to a product with id `crm`. In
grupr YAML, any object that may exist in your data platform can only be part of
a single data product. This object grouping is already enough information to
set up some basic access management in an automated fashion, and grupr does
indeed provide an implementation of this; currently only for Snowflake, a well
known SaaS database and compute platform.

`classification` is a mandatory field at the moment. It is the classification
of the most sensitive data contained in this data product. While you can define
labels like `l3` here, you have to assign (in a separate YAML file) each label
an integer value, and a higher value is interpreted as more sensitive. 

`dtaps` is optional, and can be used in cases where you have both production
and non-production data available in one and the same collection of databases
that you are managing with this YAML. Note how you can use go template syntax
for rendering the dtap name as part of a database, schema, or object identifier.
You have to do this if you specify multiple DTAP environments; an object can
only be part of a single DTAP.

`user_groups` is a term borrowed from a LeanIX meta model. In enterprise
settings, you may use it to refer to high level entities in your organisation,
to keep track of which objects have their data in them. Like with DTAPs, you can use 
use go templating to render the user group name as part of the database, schema,
or object name. Unlike with DTAPs, you do not have to do so. If you don't, the
object is considered to have data of all user groups.

`user_group_column` is the name of a column that can be used by the data team
that build the data product to denote to which user group each row in a table
or view belongs, for tables or views that contain data that belongs to
different user groups. This promotes a simple way of keeping track of data
the user groups in your organisation accross your data platform.

Together, the above fields form a kind of definition of what a data product is,
when you use grupr:

A *data product* is a collection of objects that have a single way of referring
to DTAP environments and user groups.

This was designed as a bare minimum of information that a data platform team
may require and enforce from any data team that wants to store objects on the
platform.

In many cases, DBAs or data platform engineers will be the ones to get 
questions like:

- What is the classification of the data in your platform?
- Where is all the data of divested entity X, Y, or Z?
- Please decommission project A, but make sure nothing else breaks

Even the question "Which data products do you have" can be non-trivial if
you had enough data engineers and analysts churring along.

Using this same YAML metadata for access management is not only convenient, but
also ensures that the grouping will be correct. Your data teams will make sure
they have access to all the data they need, and as a result, you will have a
clean record of that.


## Interfaces

An example interface may look like this:

```
interface:
  id: customer
  product_id: crm_fr
  objects:
    - '{{.DTAP}}_gold.crm_fr.customer*'
```

Again, there are a few things to note. Like a product, an interface is a
collection of objects. Note how you can use a `*` as a suffix; in fact, in
grupr YAML, you are only allowed to use it as a suffix. That means you
basically cut out namespaces for your data products and interfaces. Any objects
that happen to exist in your data platform and are matched by this expression
are considered part of the interface. The same object can be part of multiple
interfaces in grupr YAML.

The `customer` interface belongs to a product with id `crm_fr`.
An interface may override some metadata with regard to the product it belongs
to. For example, it may define a lower classification, or a subset of
user groups.

## Consumption relationships

Since products consume interfaces, let us add a few consumption relationships
to our product example.

```
product:
  id: crm
  classification: l3
  dtaps:
    prod: p
    non_prod: [d, t, a]
  user_groups:
    - fr
    - de
  objects:
    - '{{ .DTAP }}_gold.crm.*'
  consumes:
    id: customer
    product_id: crm_fr
    id: customer
    product_id: crm_de
```

It looks like our `crm` data product is consuming `customer` interfaces from CRM
data products of two differernt user groups. With this information in hand,
we can define a simple access management model.

## Other features

There are more features than listed above, in particular features that
introduce a bit more flexibility and configurability regarding classifications,
user groups, dtaps, and consumption relationships. Different teams do things
differently sometimes, and grupr is designed to capture some of those
differences. For example, different data products may use different names for
user groups, and grupr offers a way for you to map them on a single list of
user group names. Different teams may have different DTAPs, too, and in a
consumption relationship you can therefore specify a DTAP mapping.  Regarding
both usergroups and DTAPs, how you name them in the YAML may not always reflect
how they appear in physical object names, and for that reason you can define
multiple mappings regarding how they should be rendered.

grupr enforces a few policies, too. For example, production environments are not
allowed to consume non-production environments. Products are not allowed to
consume interfaces with a higher classification than the product classification
itself. Such policies make sure the YAML in internally consistent, coherent.

## Access management

At the moment, we have a single access management model on top of Snowflake, 
which we will discuss here.

For each combination of data product and dtap, grupr creates a product read role.
Employees working on this data product can assume this role. 

The product role gets read access to all objects in the product, as well as
read access to all objects in interfaces consumed by the product.

Rather than directly getting privileges on the objects,for efficiency reasons
(fewer relations to manage) privileges to objects are granted to database
roles. Each product and each interface has a set of associated database roles,
usually just one database role; but there can be more if objects of a sinlge
product or interface reside in different databases.

Concretely then, the product role is granted the database roles of the product
objects, and the database roles of all interface it consumes.

If you change the YAML, it can be that privileges that have been granted in the
database, based on an earlier YAML version, need to be revoked, and grupr will
indeed revoke such privileges. But if you as a DBA granted additional privileges
to a (database) role, perhaps privileges that are not currently in scope for
grupr, grupr will leave those privileges intact.

It is important to note that grupr will also clean up after itself. Any
(database) roles that it itself may have created in the past (i.e., that start
with a configurable prefix) but that are not found in the YAML will be removed.
However, if such roles have privileges that are outside of grupr its scope, 
and therefore must have been granted outside of grupr, grupr logs a message but
keeps the role and those additional privileges intact.

It is interesting to compare the way grupr manages access with popular infra as
code approaches like Terraform or OpenTofu. Such approaches tend to stay close
to the kind of objects they create. You define each resource in code, and the
tool manages its lifecycle, create, read, update, delete (CRUD). In contrast,
in grupr YAML, you define more high level concepts like data products, interfaces,
and consumption relationships. While those concepts may not have a one to one
physical representation in the database, based on just a few concepts, potentially
many resources such as roles, database roles, and privileges can be managed
automatically.

## Roadmap

Next steps include:

- The addition of a write role for each product, which will obtain OWNERSHIP
  of all objects in the product. This role is intended for service accounts
  to assume.
- The ability to describe which service accounts deploy which data products.
- The ability to describe which teams work on which data products.
- Ways to query the YAML metadata and / or the physical objects: for example: 
  - Give me a list of all products that consume interface X or Y.
  - Give me a list of all products and interfaces that have data of usergroup A or B.
  - Give me a list of all physical objects that have data of usergroup A or B.
