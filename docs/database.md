# Database Setup and Migrations

Absurd stores all durable workflow state in Postgres.  Before you can run a
worker, you need to install the `absurd` schema.  Later, when upgrading
Absurd, you need to migrate that schema forward.

There are two supported ways to do this:

1. **Use `absurdctl` directly against the database** for local development or
   simple deployments.
2. **Generate SQL and apply it with your own migration system**.  This is
   **strongly recommended** for production so schema changes stay in the same
   deployment flow as the rest of your database changes.

If you need to install `absurdctl` first, see **[absurdctl](./tools/absurdctl.md)**.
You can either run it with
[`uvx`](https://docs.astral.sh/uv/guides/tools/) `absurdctl ...` or install it
with `uv tool install absurdctl`.

## Point `absurdctl` at Your Database

All examples below assume either `PG*` environment variables or `-d` / `-h` /
`-p` / `-U` flags.

```bash
export PGDATABASE="postgresql://user:pass@localhost:5432/mydb"
```

## Initialize a Fresh Database

This immediately moves you to the latest release version of absurd.

### Directly with `absurdctl`

For a brand-new database, `absurdctl init` applies the full `absurd.sql`
schema:

```bash
absurdctl init
```

You can then verify what version was installed:

```bash
absurdctl schema-version
```

If you want to initialize from a specific release instead of `main`, pin the
schema with `--ref`:

```bash
absurdctl init --ref 0.2.0
absurdctl schema-version
```

### With Your Own Migration System (Recommended)

`absurdctl init` is convenient, but for production systems it is usually better
to check the initial Absurd schema into your own migration history.

The initial schema lives in `sql/absurd.sql`, so a typical workflow looks like
this:

```bash
cp sql/absurd.sql db/migrations/202604010001_absurd_init.sql
```

Then apply it with your existing migration tool.  If you just want to test the
SQL manually, you can also apply that file with `psql`:

```bash
psql "$PGDATABASE" -v ON_ERROR_STOP=1 -1 -f db/migrations/202604010001_absurd_init.sql
```

If you need to pin the initial schema to a specific Absurd release, copy the
`sql/absurd.sql` file from that tagged release into your migration repository.

## Upgrade an Existing Database

This is for moving upwards from an earlier version of absurd.

### Apply Migrations Directly with `absurdctl`

To upgrade an existing installation to the latest available schema:

```bash
absurdctl migrate
```

To upgrade to a specific target version:

```bash
absurdctl migrate --to 0.2.0
```

To inspect the current version before migrating:

```bash
absurdctl schema-version
```

To preview the migration plan without applying it:

```bash
absurdctl migrate --dry-run
```

### Generate Migration SQL for Your Own Migration System (Recommended)

`absurdctl` can also generate one combined SQL script for a version range.
This is the recommended way to bring Absurd upgrades into your own migration
system.

First inspect the version currently installed in the database:

```bash
absurdctl schema-version
```

Then generate a migration bundle from that version to your target version and
store it in your application's migration directory:

```bash
absurdctl migrate --from 0.1.1 --to 0.2.0 --dump-sql \
  > db/migrations/202604010002_absurd_0.1.1_to_0.2.0.sql
```

Or generate a bundle all the way to the latest schema on `main`:

```bash
CURRENT_VERSION=$(absurdctl schema-version)
absurdctl migrate --from "$CURRENT_VERSION" --to main --dump-sql \
  > db/migrations/202604010002_absurd_upgrade.sql
```

Once that SQL file is checked into your migration system, apply it the same way
as the rest of your database migrations.  For example, to test it directly with
`psql`:

```bash
psql "$PGDATABASE" -v ON_ERROR_STOP=1 -1 -f db/migrations/202604010002_absurd_upgrade.sql
```

### Upgrading from 0.5.0: queue names are limited to 53 bytes

Queue names of 54 to 57 bytes were accepted by 0.5.0 but are not supported any
longer: weekly partition names carry a year-unique tag that makes them three
bytes longer, and PostgreSQL would truncate them to 63 bytes so that different
weeks collide.  The `0.5.0-main` migration therefore checks first, before it
changes anything, and fails with an error listing the affected queues if any
exists.  To upgrade, drain each listed queue on the 0.5.0 database, drop it
with `absurd.drop_queue()`, recreate it under a name of at most 53 bytes (and
point your producers and workers at the new name), and run the migration again.
The check covers unpartitioned queues too, because the functions that validate
the queue name reject them.

## Notes

- `absurdctl init` is for **fresh installs**.
- `absurdctl migrate` is for **upgrading an existing install**.
- `absurdctl migrate --dump-sql` requires `--from` and **does not connect to
  Postgres**.  It only renders the SQL bundle.
- After the schema is installed, create at least one queue before running
  workers:

```bash
absurdctl create-queue default
```
