# :link: datajoint-link

<p align="center">
<img src="datajoint_link.png" alt="datajoint-link logo" style="display: block; margin-left: auto; margin-right: auto; width: 20%">
<br>
<a href="https://github.com/sinzlab/datajoint-link/actions/workflows/ci.yml"><img alt="Actions Status" src="https://github.com/sinzlab/datajoint-link/actions/workflows/ci.yml/badge.svg"></a>
<a href="https://codecov.io/gh/sinzlab/datajoint-link/"><img alt="Coverage" src="https://img.shields.io/codecov/c/github/sinzlab/datajoint-link"></a>
<a href="https://pypi.org/project/datajoint-link/"><img alt="PyPI" src="https://img.shields.io/pypi/v/datajoint-link"></a>
</p>


<p align="center"> A tool for convenient and integrity-preserving data sharing between database servers. </p>

## :floppy_disk: Installation 

Only users interacting with the destination of the data need to install the datajoint-link package:

```bash
pip install datajoint-link
```

## :wrench: Setup

### Source

Datajoint-link requires access to the database server from which data will be pulled. It is recommended to create a new user for this purpose:

```sql
CREATE USER 'djlink'@'%' IDENTIFIED BY 'secret-password';
```

The user needs to have certain privileges on the table from which data will be pulled:

```sql
GRANT SELECT, REFERENCES ON `source\_schema`.`source\_table` TO 'djlink'@'%';
```

Each table from which data will be pulled also needs an additional helper table:

```sql
GRANT ALL PRIVILEGES ON `helper\_schema`.`helper\_table` TO 'djlink'@'%';
```

In order to preserve data integrity across the link regular users must not have any privileges on this helper table. 

### Destination

Datajoint-link needs to be configured with the username and password of the user created in the previous section. This is accomplished via environment variables:

```bash
LINK_USER=djlink
LINK_PASS=secret-password
```

## :computer: Usage

The destination table is created by passing information about where to find the source table to the `link` decorator:

```python
from link import link

@link(
    "databaseserver.com", 
    "source_schema", 
    "helper_schema", 
    "helper_table", 
    "destination_schema"
)
class Table:
    """Some table present in the source schema on the source database server."""
```

Note that the name of the declared class must match the name of the table from which the data will be pulled.

The class returned by the decorator behaves like a regular table with some added functionality. For one it allows the browsing of rows that can be pulled from the source:

```python
Table().source
```

All the rows can be pulled like so:

```python
Table().source.pull()  # Hint: Pass display_progress=True to get a progress bar
```

That said usually we only want to pull rows that match a certain criteria:

```python
(Table().source & "foo = 1").pull()
```

The deletion of already pulled rows works the same as for any other table:

```python
(Table() & "foo = 1").delete()
```

The deletion of certain rows from the destination can also be requested by flagging them in the corresponding helper table:

```python
row = (Helper() & "foo = 1").fetch1()
(Helper() & row).delete()
row["is_flagged"] = "TRUE"
Helper().insert1(row)
```

The `flagged` attribute makes the deletion of flagged rows from the destination table convenient:

```python
(Table() & Table().source.flagged).delete()
```

Deleting a flagged row automatically updates its corresponding row in the helper table:

```python
assert (Helper() & "foo = 1").fetch1("is_deprecated") == "TRUE" # No error!
```

Now it is save to delete the row from the source table as well!

## :package: External Storage

Data stored in a source table that refers to one (or more) external stores can be stored in different store(s) after pulling:

```python
@link(
    ...,
    stores={"source_store": "destination_store"}
)
class Table:
    ...
```

Note that all stores mentioned in the dictionary need to be configured via `dj.config`.

## :white_check_mark: Tests

Clone this repository and run the following command from within the cloned repository to run all tests:

```bash
docker compose run functional_tests tests
```
