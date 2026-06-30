#!/usr/bin/env python3
"""
sgas-wlcg-tool

Usage examples
--------------

Show everything (default):
    sgas-wlcg-tool show

Show only a specific site:
    sgas-wlcg-tool show --site "FOO"

Show only a specific machine:
    sgas-wlcg-tool show --machine "kurvi.tasch.bx"

Show only a specific country:
    sgas-wlcg-tool show --country "Borduria"

Show only a specific tier (optionally combined with a VO):
    sgas-wlcg-tool show --tier "ndgf-t1"

    or

    sgas-wlcg-tool show --tier "ndgf-t1:alice"


Add a new machine to a country and site (adding the site and country if needed), with tiers.
Or add tiers to an existsing machine:

    sgas-wlcg-tool add \\
        --machine "kurvi.tasch.bx" \\
        --country "Borduria" \\
        --site "FOO" \\
        --tier "ndgf-t1:atlas" \\
        --tier "borduria-t2:cms"

Multiple ``--tier`` flags can be used to insert several (tier_name:vo_name) rows.
'*' can be used for vo_name, meaning "all VOs"


Remove a specific tier (or tier:VO combination) from a machine:

    sgas-wlcg-tool del --machine "kurvi.tasch.bx" --tier "ndgf-t1:atlas"

    or multiple tiers at once:

    sgas-wlcg-tool del --machine "kurvi.tasch.bx" --tier "ndgf-t1:atlas" --tier "borduria-t2:cms"

Disassociate a machine from a site:

    sgas-wlcg-tool del --machine "kurvi.tasch.bx" --site "FOO"

Fully remove a machine (only possible if it has no usage records in usagedata).
Also removes its tiers, site associations and scale factor entries:

    sgas-wlcg-tool del --machine "kurvi.tasch.bx"

Remove a site (only possible if no machines are associated with it and it has no pledges):

    sgas-wlcg-tool del --site "FOO"

Remove a country (only possible if it has no sites and no pledges):

    sgas-wlcg-tool del --country "Borduria"
"""

import argparse
import sys

import psycopg2
from psycopg2 import sql
from psycopg2.extensions import connection as db_connection, cursor as db_cursor
from dataclasses import dataclass

from sgas.server import config

DEFAULT_POSTGRESQL_PORT = 5432
DEFAULT_CONFIG_FILE = "/etc/sgas.conf"


def parse_dbstring(dbstring: str) -> dict:
    fields = dbstring.split(':')
    host = fields[0] or None
    port =  int(fields[1]) if fields[1] else DEFAULT_POSTGRESQL_PORT
    database = fields[2]
    user = fields[3]
    password = fields[4]
    return {"host": host, "port": port, "database": database, "user": user, "password": password}


def get_connection(dbstring: str) -> db_connection:
    """Create and return a new DB connection."""
    try:
        conn = psycopg2.connect(**parse_dbstring(dbstring))
        conn.autocommit = False
        return conn
    except Exception as e:
        sys.exit(f"ERROR: Could not connect to the database: {e}")


@dataclass
class TierVo:
    tier: str
    vo: str

def parse_tier_arg(arg: str) -> TierVo:
    """
    Parse a ``TIER_NAME:VO_NAME`` string into a TierVo.
    Raises ``argparse.ArgumentTypeError`` if the format is wrong.
    """
    parts = arg.split(':')

    if len(parts) == 1:
        tier = parts[0]
        vo = '*'
    elif len(parts) == 2:
        tier, vo = parts
    else:
        raise argparse.ArgumentTypeError(f'Invalid tier specification "{arg}". Expected format "tier_name:vo_name".')

    if not vo or not tier:
        raise argparse.ArgumentTypeError(f'Invalid tier specification "{arg}". Both parts must be non‑empty.')

    return TierVo(tier.strip(), vo.strip())


def expect_one(cursor: db_cursor, query: str|sql.SQL, params: tuple|None=None) -> tuple:
    "Fetchone, raise eception if nothing found"

    cursor.execute(query, params or ())
    x = cursor.fetchone()

    if x is None:
        raise Exception(f"Expected result from query {query} with parameters {params}, but got nothing")

    return x


#
# Mode 1 – SHOW
#
def show_data(args: argparse.Namespace, db_conn: db_connection) -> None:
    """
    Print rows from the schema according to the supplied filters.
    """
    cur = db_conn.cursor()

    base_query = sql.SQL(
        """
        SELECT
            c.country_name,
            s.site_name,
            m.machine_name,
            array_to_string(array_agg(t.tier_name || ':' || t.vo_name),', ') as tier_vo
        FROM machinename AS m
        JOIN wlcg.machinename_site_junction AS j
            ON j.machine_name_id = m.id
        JOIN wlcg.sites AS s
            ON s.site_id = j.site_id
        JOIN wlcg.countries AS c
            ON c.country_id = s.country_id
        LEFT JOIN wlcg.tiers AS t
            ON t.machine_name_id = m.id
        """
    )

    conditions = []
    params = []

    if args.machine:
        conditions.append(sql.SQL("m.machine_name = %s"))
        params.append(args.machine)

    if args.country:
        conditions.append(sql.SQL("c.country_name = %s"))
        params.append(args.country)

    if args.site:
        conditions.append(sql.SQL("s.site_name = %s"))
        params.append(args.site)

    if args.tier:
        # Allow filtering by tier name alone or by both VO and tier.
        if ":" in args.tier:
            tier, vo = args.tier.split(":", 1)
            conditions.append(sql.SQL("(t.vo_name = %s AND t.tier_name = %s)"))
            params.extend([vo, tier])
        else:
            conditions.append(sql.SQL("t.tier_name = %s"))
            params.append(args.tier)

    epilog = sql.SQL(" GROUP BY m.machine_name, c.country_name, s.site_name ORDER BY c.country_name, s.site_name, m.machine_name")

    if conditions:
        where_clause = sql.SQL(" WHERE ") + sql.SQL(" AND ").join(conditions)
        final_query = base_query + where_clause + epilog
    else:
        final_query = base_query + epilog


    cur.execute(final_query, params)

    rows = cur.fetchall()
    if not rows:
        print("No matching records found.")
        db_conn.close()
        return

    # Pretty‑print a table
    header = ["Country", "Site", "Machine", "Tier:VO"]
    col_widths = [max(len(str(cell)) for cell in col) for col in zip(*([header] + rows))]
    fmt = " | ".join(f"{{:<{w}}}" for w in col_widths)

    print(fmt.format(*header))
    print("-" * (sum(col_widths) + 3 * (len(header) - 1)))
    for row in rows:
        print(fmt.format(*[cell if cell is not None else "" for cell in row]))

    db_conn.close()


#
# Mode 2 – ADD
#
def add_data(args: argparse.Namespace, db_conn: db_connection) -> None:
    """
    Insert a new machine (and related country/site/tier rows) into the schema.
    All inserts are performed inside a single transaction – any error rolls
    back the whole operation.
    """
    cur = db_conn.cursor()

    try:
        if args.country:
            # Ensure the country exists (or create it)
            cur.execute("SELECT country_id FROM wlcg.countries WHERE country_name = %s", (args.country,))
            country_row = cur.fetchone()
            if country_row:
                country_id = country_row[0]
            else:
                country_id = expect_one(cur, "INSERT INTO wlcg.countries (country_name) VALUES (%s) RETURNING country_id", (args.country,))[0]
                print(f"Created country '{args.country}' (id={country_id})")
        else:
            country_id = None

        if args.site:
            # Ensure the site exists (or create it)
            cur.execute("SELECT site_id FROM wlcg.sites WHERE site_name = %s", (args.site,))
            site_row = cur.fetchone()
            if site_row:
                site_id = site_row[0]
            else:
                if country_id is None:
                    sys.exit(f"Site {args.site} does not exists, and to create it, we need to know the country")

                site_id = expect_one(cur, "INSERT INTO wlcg.sites (country_id, site_name) VALUES (%s, %s) RETURNING site_id", (country_id, args.site))[0]
                print(f"Created site '{args.site}' (id={site_id})")

        #  Ensure the machine exists (or create it)
        cur.execute("SELECT id FROM machinename WHERE machine_name = %s", (args.machine,))
        mach_row = cur.fetchone()
        if mach_row:
            machine_id = mach_row[0]
        else:
            machine_id = expect_one(cur, "INSERT INTO machinename (machine_name) VALUES (%s) RETURNING id", (args.machine,))[0]
            print(f"Created machine '{args.machine}' (id={machine_id})")

        # Link machine ↔ site (junction table)
        # The junction column is declared UNIQUE, so we either INSERT or UPDATE.
        cur.execute(
            """
            INSERT INTO wlcg.machinename_site_junction (machine_name_id, site_id)
            VALUES (%s, %s)
            ON CONFLICT (machine_name_id) DO UPDATE SET site_id = EXCLUDED.site_id
            """,
            (machine_id, site_id),
        )
        print(f"Linked machine '{args.machine}' to site '{args.site}'")

        # Insert tier / VO rows (may be many)
        for tiervo in args.tiers:
            cur.execute(
                """
                INSERT INTO wlcg.tiers (machine_name_id, vo_name, tier_name)
                VALUES (%s, %s, %s)
                ON CONFLICT (machine_name_id, vo_name) DO UPDATE
                    SET tier_name = EXCLUDED.tier_name
                """,
                (machine_id, tiervo.vo, tiervo.tier),
            )
            print(f"Set tier '{tiervo.tier}' for VO '{tiervo.vo}' on machine '{args.machine}'")

        # Commit everything
        db_conn.commit()
        print("\nAll data successfully stored.")
    except Exception as exc:
        db_conn.rollback()
        sys.exit(f"\nError while inserting data – transaction rolled back.\nDetails: {exc}")
    finally:
        db_conn.close()


#
# Mode 3 – DEL
#
def del_data(args: argparse.Namespace, db_conn: db_connection) -> None:
    """
    Remove tier rows from wlcg.tiers and/or disassociate a machine from a site
    in wlcg.machinename_site_junction.
    """
    if not args.machine and not args.site and not args.country:
        sys.exit("Error: at least one of --machine, --site or --country must be given.")
    if args.tiers and not args.machine:
        sys.exit("Error: --tier requires --machine.")
    if args.country and (args.machine or args.site or args.tiers):
        sys.exit("Error: --country cannot be combined with other options.")

    cur = db_conn.cursor()

    try:
        if args.country:
            # Country-only removal
            cur.execute("SELECT country_id FROM wlcg.countries WHERE country_name = %s", (args.country,))
            country_row = cur.fetchone()
            if not country_row:
                sys.exit(f"Country '{args.country}' not found.")
            country_id = country_row[0]

            cur.execute("SELECT COUNT(*) FROM wlcg.sites WHERE country_id = %s", (country_id,))
            if cur.fetchone()[0] > 0:
                sys.exit(f"Cannot remove country '{args.country}': it still has sites in wlcg.sites.")

            cur.execute("SELECT COUNT(*) FROM wlcg.pledges WHERE country_id = %s", (country_id,))
            if cur.fetchone()[0] > 0:
                sys.exit(f"Cannot remove country '{args.country}': it has pledges in wlcg.pledges.")

            cur.execute("DELETE FROM wlcg.countries WHERE country_id = %s", (country_id,))
            print(f"Removed country '{args.country}'.")

            db_conn.commit()
            print("\nDone.")
            return

        if args.site and not args.machine:
            # Site-only removal
            cur.execute("SELECT site_id FROM wlcg.sites WHERE site_name = %s", (args.site,))
            site_row = cur.fetchone()
            if not site_row:
                sys.exit(f"Site '{args.site}' not found.")
            site_id = site_row[0]

            cur.execute("SELECT COUNT(*) FROM wlcg.machinename_site_junction WHERE site_id = %s", (site_id,))
            if cur.fetchone()[0] > 0:
                sys.exit(f"Cannot remove site '{args.site}': it still has machines associated with it.")

            cur.execute("SELECT COUNT(*) FROM wlcg.pledges WHERE site_id = %s", (site_id,))
            if cur.fetchone()[0] > 0:
                sys.exit(f"Cannot remove site '{args.site}': it has pledges in wlcg.pledges.")

            cur.execute("DELETE FROM wlcg.sites WHERE site_id = %s", (site_id,))
            print(f"Removed site '{args.site}'.")

            db_conn.commit()
            print("\nDone.")
            return

        cur.execute("SELECT id FROM machinename WHERE machine_name = %s", (args.machine,))
        mach_row = cur.fetchone()
        if not mach_row:
            sys.exit(f"Machine '{args.machine}' not found.")
        machine_id = mach_row[0]

        if not args.tiers and not args.site:
            # Full machine removal
            cur.execute("SELECT COUNT(*) FROM usagedata WHERE machine_name_id = %s", (machine_id,))
            n = cur.fetchone()[0]
            if n > 0:
                sys.exit(f"Cannot remove machine '{args.machine}': it has {n} usage record(s) in usagedata.")

            cur.execute("DELETE FROM wlcg.tiers WHERE machine_name_id = %s", (machine_id,))
            print(f"Removed {cur.rowcount} tier row(s) for machine '{args.machine}'.")

            cur.execute("DELETE FROM wlcg.machinename_site_junction WHERE machine_name_id = %s", (machine_id,))
            print(f"Disassociated machine '{args.machine}' from {cur.rowcount} site(s).")

            cur.execute("DELETE FROM hostscalefactors_data WHERE machine_name_id = %s", (machine_id,))
            print(f"Removed {cur.rowcount} scale factor row(s) for machine '{args.machine}'.")

            cur.execute("DELETE FROM machinename WHERE id = %s", (machine_id,))
            print(f"Removed machine '{args.machine}' from machinename.")

        else:
            for tiervo in (args.tiers or []):
                cur.execute(
                    "DELETE FROM wlcg.tiers WHERE machine_name_id = %s AND tier_name = %s AND vo_name = %s",
                    (machine_id, tiervo.tier, tiervo.vo),
                )
                if cur.rowcount == 0:
                    print(f"No tier '{tiervo.tier}' for VO '{tiervo.vo}' on machine '{args.machine}' found.")
                else:
                    print(f"Removed tier '{tiervo.tier}' for VO '{tiervo.vo}' from machine '{args.machine}'.")

            if args.site:
                cur.execute("SELECT site_id FROM wlcg.sites WHERE site_name = %s", (args.site,))
                site_row = cur.fetchone()
                if not site_row:
                    sys.exit(f"Site '{args.site}' not found.")
                site_id = site_row[0]

                cur.execute(
                    "DELETE FROM wlcg.machinename_site_junction WHERE machine_name_id = %s AND site_id = %s",
                    (machine_id, site_id),
                )
                if cur.rowcount == 0:
                    print(f"Machine '{args.machine}' was not associated with site '{args.site}'.")
                else:
                    print(f"Disassociated machine '{args.machine}' from site '{args.site}'.")

        db_conn.commit()
        print("\nDone.")
    except Exception as exc:
        db_conn.rollback()
        sys.exit(f"\nError while deleting data – transaction rolled back.\nDetails: {exc}")
    finally:
        db_conn.close()


def main() -> None:
    parser = argparse.ArgumentParser(description="Utility for the SGAS WLCG schema (show / add data).")
    parser.add_argument('-c', '--conf', help="SGAS config file (default: /etc/sgas.conf)", default=DEFAULT_CONFIG_FILE)
    subparsers = parser.add_subparsers(dest="command", required=True)

    # SHOW
    show_parser = subparsers.add_parser("show", help="List sites, machines and tiers with optional filters.")
    show_parser.add_argument("--machine", help="Filter by machine_name")
    show_parser.add_argument("--country", help="Filter by country_name")
    show_parser.add_argument("--site", help="Filter by site_name")
    show_parser.add_argument("--tier", help='Filter by tier_name or "tier_name:vo_name".')
    show_parser.set_defaults(func=show_data)

    # ADD
    add_parser = subparsers.add_parser("add", help="Insert a new machine together with its country, site and tiers")
    add_parser.add_argument("--machine", required=True, help="Name of the machine to insert")
    add_parser.add_argument("--country", required=False, help="Country where the site resides")
    add_parser.add_argument("--site", required=True, help="Site name to associate with the machine")
    add_parser.add_argument("--tier", dest="tiers", action="append", type=parse_tier_arg, required=True, metavar="TIER:VO", help='One or more tier specifications, e.g. "--tier ndgf-t1:atlas". Can be repeated')
    add_parser.set_defaults(func=add_data)

    # DEL
    del_parser = subparsers.add_parser("del", help="Remove tier(s)/machine/site/country from the WLCG schema")
    del_parser.add_argument("--machine", required=False, help="Name of the machine")
    del_parser.add_argument("--site", help="Site to disassociate the machine from, or remove (alone)")
    del_parser.add_argument("--country", help="Country to remove (alone, no other options)")
    del_parser.add_argument("--tier", dest="tiers", action="append", type=parse_tier_arg, required=False, metavar="TIER:VO", help='One or more tier specifications to remove, e.g. "--tier ndgf-t1:atlas". Can be repeated')
    del_parser.set_defaults(func=del_data)

    args = parser.parse_args()
    cfg = config.readConfig(args.conf)
    conn = get_connection(cfg.get(config.SERVER_BLOCK,config.DB))
    args.func(args, conn)


if __name__ == "__main__":
    main()
