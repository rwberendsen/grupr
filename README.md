# grupr

A tool to group database objects and manage them more easily.

Grupr is a CLI (command line interface) tool that works with a YAML format for
grouping database objects into data products and interfaces.  It targets
typical ANSI SQL environments with databases, schema's, and objects (tables and
views). You can annotate high level lineage in grupr YAML, where data products
consume interfaces. 

YAML is friendly to human eyes, so the collection of YAML files give you a
understanding of your data platform at a high level. YAML it is also machine
readable, which means there are use cases for automation, once you describe
your data products in grupr its YAML format.  Currently, grupr can manage
access to data objects in Snowflake.

Because grupr uses a simple text based data format (YAML), you can take
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
  objects:
    - '{{ .DTAP }}_gold.crm.*'
```

There are a few things to note here.  First you assign all `objects` in schema
`gold.crm` to a product with id `crm`. In grupr YAML, any object that may exist
in your data platform can only be part of a single data product. This object
grouping is already enough information to set up some basic access management
in an automated fashion, and grupr does indeed provide an implementation of
this; currently only for Snowflake, a well known SaaS database and compute
platform.

`classification` is a mandatory field at the moment. It is the classification
of the most sensitive data contained in this data product. While you can define
labels like `l3` here, you have to assign each label an integer value, and a
higher value is interpreted as more sensitive. 

`dtaps` is optional, and can be used in cases where you have both production
and non-production data available in one and the same collection of databases
you are managing with this YAML. Note how you can use go template syntax to
rendering the dtap name as part of a database, schema, or object identifier.

`user_groups` is a term borrowed from a LeanIX meta model. In enterprise
settings, you may use it to refer to high level entities in your organisation,
to keep track of which objects have their data in them.

Together, the above fields form a kind of definition of what a data product is,
when you use grupr.

| :memo:   | A data product is a collection of objects that have            |
|          | a single way of referring to DTAP environments and user groups |
|----------|:---------------------------------------------------------------|

This was designed as a bare minimum of information that a data platform team
may require from any data team that wants to store objects on the platform.

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


