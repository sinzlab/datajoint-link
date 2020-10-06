# DataJoint-Link
![Test](https://github.com/cblessing24/link/workflows/Test/badge.svg)
![Black](https://github.com/cblessing24/link/workflows/Black/badge.svg)
![Mypy](https://github.com/cblessing24/link/workflows/Mypy/badge.svg)
[![codecov](https://codecov.io/gh/cblessing24/link/branch/master/graph/badge.svg?token=94RHFeL75V)](https://codecov.io/gh/cblessing24/link)
![PyPI](https://github.com/cblessing24/datajoint-link/workflows/PyPI/badge.svg)
[![PyPI version](https://badge.fury.io/py/datajoint-link.svg)](https://badge.fury.io/py/datajoint-link)

 DataJoint-Link is a tool for linking two [DataJoint](https://datajoint.io/) tables located on different database
  servers together and exchanging entities between them. It preserves [referential integrity](https://docs.datajoint.io/python/concepts/04-Integrity.html#referential-integrity) across the linked tables and supports useful DataJoint features such as part tables and externally stored files. 

# How It Works
A link involves three tables. A table from which entities are pulled, called the source table, a table into which the
 pulled entities are inserted, called the local table, and a third table used to enforce referential integrity between
  the two. This third table is called the outbound table.

## Source Table
The source table is the table from which entities will be pulled and it should be located on a different database server
 than the local table to justify the usage of this tool. For example, the source table could be located on a database
  server belonging to a lab that wants to share its data with a collaborator or the public.
  
## Local Table
The local table stores entities that were pulled from the source table. It is created when the link is first established
 and has primary and non-primary attributes that are identical to the ones of the source table.
 
## Outbound Table
The outbound table is located on the same database server as the source table. Its job is to enforce referential
 integrity and it does so by depending on the source table and containing all entities that were pulled into the
  local table. As a result these entities can not be deleted from the source table without also being deleted from
   the outbound table. This fact in combination with the procedure of deleting entities from the local table before
    deleting them from the outbound and source tables enforces referential integrity.
    
## Pulling
A pull consists of three consecutive steps. First the entities are fetched from the source table. Next their primary
 keys are inserted into the outbound table and, lastly, the complete entities are inserted into the local table. If
  the source table has part tables, their entities will also be fetched and inserted into the corresponding part
   tables of the local table.
  
## Deleting
A deletion request can be sent from the source to the local side by enabling a flag on the entities that should be
 deleted in the outbound table. The local side can view these requests and delete the corresponding entities. This
  will trigger the enablement of another flag in the outbound table letting the source side know the entities have
   been locally deleted. Now the entities can be safely deleted from the outbound and source tables.
   
The local side is always free to delete any entities from the local table that had their deletion not requested by
 the source side. Doing so will simply delete the locally deleted entities from the outbound table.
  
# Install
```pip3 install datajoint-link```

Older versions of DataJoint-Link that were installed via `pip` can be upgraded with

```pip3 install --upgrade datajoint-link```

# Setup
A small one-time setup is necessary before two tables can be linked.

DataJoint-Link requires access to a user on the source database server to create and manage the outbound table and to
 fetch entities from the local table. This user will be called link-user here and its username and password must be
  set via the environment variables `LINK_USER` and `LINK_PASS`, respectively.

Creating a new user with the minimally required set of privileges on the source database to be the link-user is
 recommended to limit the access of the tool to the database. These privileges include `SELECT` and `REFERENCE` on
  the schema containing the source table to be able to create the outbound table and to fetch entities from the
   source table.
 
 Furthermore the name of the schema containing the outbound table must be specified via the environment variable
  `LINK_OUTBOUND` and the link-user must be granted full privileges on the outbound schema. It is recommended to
   create a new schema to function as the outbound schema.
   
Regular database users should have no privileges on the outbound schema to prevent them from accidentally deleting
 entities that are still present in the local table from the source table, thus breaking referential integrity.
     
 Environment variables example:
 ```
LINK_USER=link_user
LINK_PASS=password
LINK_OUTBOUND=outbound_schema
```

# Usage
The definition of the local table looks like this:
```python
from dj_link import LazySchema, Link

local_schema = LazySchema('local_schema')
source_schema = LazySchema('source_schema', host='source_database')

@Link(local_schema, source_schema)
class Table:
    """The local table."""
```
`LazySchema` is a modified version of DataJoint's `Schema` class. It establishes the connection to the database when
 needed and, unlike `Schema`, not when the instance is created. The `host` keyword argument must be set to the host
  name of the database server that contains the source schema when creating the corresponding `LazySchema` instance.
   
Two instances of the `LazySchema` class must be passed when creating an instance of the `Link` class. The first one must
 correspond to the schema which will contain the local table and the second one to the schema on the source database
  server containing the source table. Note that the name of the class that gets decorated by the `Link` instance must be
   the same as the name of the source table.

A mapping of store names can be passed as a keyword argument to the constructor of the `Link` class:
```python
Link(local_schema, source_schema, stores={'local_store': 'source_store'})
```
This is necessary if the source table contains references to externally stored files. Note that the local side needs
 to be able to access the files stored in the source store. Currently it is not possible to pull entities without
  creating a copy of the external files.

## Pulling
The contents of the source table can be viewed using the `source` attribute:
```python
Table().source()
```

All entities that are not present in the local table and whose deletion has not been requested can be pulled using the
 `pull` method:
```python
Table().pull()
```

DataJoint restrictions can be passed to the `pull` method if only specific entities are to be pulled:
```python
Table().pull(*restrictions)
```

## Deleting

The primary keys of entities that should be deleted from the local table can be inserted into the `DeletionRequested
` part table of the outbound table. The local side can check which entities had their deletion requested by viewing
 their `DeletionRequested` part table:
 ```python
Table().DeletionRequested()
```

Deleting the aforementioned entities from the local table can be accomplished by appropriately restricting the local
 table and calling the `delete` method:
```python
(Table() & restriction).delete()
```

The source side can then see that entities have been deleted from the local table by checking if the corresponding
 primary keys are present in the `DeletionApproved` part table of the outbound table.

## Refreshing
The `refresh` method can be used to update the `DeletionRequested` part table of the local table with new entries from
 the outbound table:
```python
Table().refresh()
```

# Running Tests
Clone this repository and run the following command from within the cloned repository to run all tests:
```
docker-compose run test tests
```
