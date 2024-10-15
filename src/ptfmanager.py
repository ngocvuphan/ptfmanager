import glob
import os
import sqlite3
import time
from argparse import ArgumentParser
from sqlite3 import Connection
from printcolors import colors

DB_KEY = "_KEY_"


def update_table_columns(dbconnection: Connection, table: str, header: list, pri_hdr_sz: int):
    try:
        cursor = dbconnection.cursor()
        cursor.execute(f"PRAGMA table_info(`{table}`)")
        tbheader = cursor.fetchall()
        if not tbheader:
            # Create new table
            sql_columns = ",".join([f"`{x}` TEXT" for x in header])
            sql_pri_keys = ",".join([f"`{x}`" for x in header[:pri_hdr_sz]])
            sql = f"CREATE TABLE `{table}` (`{DB_KEY}`,{sql_columns}, PRIMARY KEY({sql_pri_keys}), UNIQUE(`{DB_KEY}`))"
            cursor.execute(sql)
            dbconnection.commit()
            return True
        else:
            # Table exists. Check if header need to update
            tbheader_updated = False
            tbheader_old = tbheader.copy()
            for i, hdr in enumerate(header):
                tbhdr = [h for h in tbheader if h[1] == hdr]
                if not tbhdr:
                    tbheader_updated = True
                    tbheader.insert(i, (i, hdr, "TEXT", 0, None, i if i < pri_hdr_sz else 0))
            if tbheader_updated:
                # Rename old table
                sql = f"ALTER TABLE `{table}` RENAME TO `{table}__OLD`"
                cursor.execute(sql)
                # Create new table
                sql_columns = ",".join([f"`{x[1]}` TEXT" for x in tbheader])
                sql_pri_keys = ",".join([f"`{x[1]}`" for x in tbheader if x[5]])
                sql = f"CREATE TABLE `{table}` ({sql_columns}, PRIMARY KEY({sql_pri_keys}), UNIQUE(`{DB_KEY}`))"
                cursor.execute(sql)
                # Copy data from old table to new table
                sql_columns = ",".join([f"`{x[1]}`" for x in tbheader_old])
                sql = f"INSERT INTO `{table}` ({sql_columns}) SELECT {sql_columns} FROM `{table}__OLD`"
                cursor.execute(sql)
                # Drop old table
                sql = f"DROP TABLE `{table}__OLD`"
                cursor.execute(sql)
                # Commit changes
                dbconnection.commit()
            return True
    except sqlite3.Error as error:
        print(f"{colors.fg.red}Error occurred - {error}{colors.reset}")
        return False
    finally:
        if cursor:
            cursor.close()


def update_database(dbconnection: Connection, table: str, header: list, pri_hdr_sz: int, data: dict):
    try:
        if not update_table_columns(dbconnection, table, header, pri_hdr_sz):
            print(colors.fg.red, "Failed to create/update table", table, colors.reset)
            return False
        cursor = dbconnection.cursor()
        sql_columns = ",".join([f"`{DB_KEY}`"] + [f"`{x}`" for x in header])
        sql_values = ",".join(["?"] * (len(header) + 1))
        sql_tuples = [tuple([",".join([d[k] for k in header[:pri_hdr_sz]])] + [d[k] for k in header]) for d in data]
        sql = f"INSERT OR IGNORE INTO `{table}` ({sql_columns}) VALUES ({sql_values})"
        cursor.executemany(sql, sql_tuples)
        dbconnection.commit()
        return True
    except sqlite3.Error as error:
        print(f"{colors.fg.red}Error occurred - {error}{colors.reset}")
        return False
    finally:
        if cursor:
            cursor.close()


def pft_to_db(ptf_path, dbconnection: Connection):
    with open(ptf_path, mode="r") as ptf_file:
        FILE_TYPE = ""
        PART = ""
        PRI_HEADER_CNT = -1
        HEADER = []
        part_list = []
        is_comment = False
        for line in ptf_file:
            line = line.strip()
            if not line:
                continue
            if line.startswith("{"):
                is_comment = True
                continue
            if line.endswith("}"):
                is_comment = False
                continue
            if is_comment:
                continue
            if not FILE_TYPE and line.startswith("FILE_TYPE"):
                FILE_TYPE = line.split("=")[1].strip().strip(";")
                if FILE_TYPE != "MULTI_PHYS_TABLE":
                    print(f"{colors.fg.red}Invalid file format!!!{colors.reset}")
                    break
            elif line.startswith("PART"):
                lst = line.split("'")
                PART = lst[1].strip()
            elif line.startswith(":"):
                lst = line.lstrip(":").split("=")
                HEADER = [x.replace("(OPT)", "").strip() for x in lst[0].split("|")]
                PRI_HEADER_CNT = len(HEADER)
                HEADER.extend([x.replace("(OPT)", "").strip() for x in lst[1].rstrip(";").split("|")])
            elif line.startswith("END_PART"):
                update_database(dbconnection, PART, list(dict.fromkeys(HEADER)), PRI_HEADER_CNT, part_list)
                PART = ""
                part_list = []
            elif line.startswith("END."):
                FILE_TYPE = ""
                break
            else:
                params = []
                prev_val = ""
                idx = 0
                for x in line.split("|"):
                    if prev_val.endswith("\\"):
                        prev_val = prev_val + "|" + x
                    else:
                        prev_val = x
                    if prev_val.endswith("\\"):
                        continue
                    else:
                        idx += 1
                        # Last primary param
                        if idx == PRI_HEADER_CNT:
                            prev_val2 = ""
                            for y in prev_val.strip().split("="):
                                if prev_val2 and not (prev_val2.strip().endswith("'") or prev_val2.strip().endswith(")")):
                                    prev_val2 = prev_val2 + "=" + y
                                else:
                                    prev_val2 = y
                                prev_val2_t = prev_val2.strip()
                                if prev_val2_t.endswith("'"):
                                    params.append(prev_val2_t)
                                    prev_val2 = ""
                                elif prev_val2_t.endswith(")"):
                                    params.append(prev_val2_t.split("(")[0].strip())
                                    prev_val2 = ""
                        else:
                            params.append(prev_val.strip())
                # Last param
                prev_val2 = ""
                for y in params.pop().strip().split(":"):
                    if prev_val2 and not prev_val2.strip().endswith("'"):
                        prev_val2 = prev_val2 + ":" + y
                    else:
                        prev_val2 = y
                    prev_val2_t = prev_val2.strip()
                    if prev_val2_t.endswith("'"):
                        params.append(prev_val2_t)
                        break
                part_list.append(dict(zip(HEADER, params)))


def db_to_ptf(ptf_path, dbconnection: Connection):
    try:
        with open(ptf_path, mode="w") as ptf_file:
            ptf_file.write("FILE_TYPE=MULTI_PHYS_TABLE;\n")
            cursor = dbconnection.cursor()
            cursor.execute("SELECT `name` FROM sqlite_master WHERE type='table' ORDER BY `name` ASC")
            for row in cursor.fetchall():
                tb_name = row[0]
                # Write PART define
                ptf_file.write(f"\nPART '{tb_name}'\n")
                # Get and write HEADER define
                cursor.execute(f"PRAGMA table_info(`{tb_name}`)")
                columns = cursor.fetchall()
                pri_columns = [x[1] for x in filter(lambda y: y[1] != DB_KEY and y[5] > 0, columns)]
                sec_columns = [x[1] for x in filter(lambda y: y[1] != DB_KEY and y[5] == 0, columns)]
                ptf_file.write(":{}={};\n".format("|".join(pri_columns), "|".join(sec_columns)))
                # Get and write rows
                pri_cnt = len(pri_columns)
                sql_columns = ",".join([f"`{x}`" for x in pri_columns + sec_columns])
                cursor.execute(f"SELECT {sql_columns} FROM `{tb_name}` ORDER BY `{pri_columns[0]}` ASC")
                while row := cursor.fetchone():
                    pri_vals = [x if x else "''" for x in row[:pri_cnt]]
                    sec_vals = [x if x else "''" for x in row[pri_cnt:]]
                    ptf_file.write("{}={}\n".format("|".join(pri_vals), "|".join(sec_vals)))
                # Write END_PART
                ptf_file.write("END_PART\n")
            ptf_file.write("\nEND.\n")
    except sqlite3.Error as error:
        print(f"{colors.fg.red}Error occurred - {error}{colors.reset}")
        return False
    finally:
        if cursor:
            cursor.close()


if __name__ == "__main__":
    parser = ArgumentParser(prog="PTFManager", description="Manage PTF files")
    subparsers = parser.add_subparsers(dest="command", title="Commands", required=True)

    imp_parser = subparsers.add_parser("import")
    imp_parser.add_argument("-f", "--db-file", default="part_table.db")
    imp_parser.add_argument("-d", "--ptf-dir", default=".")
    imp_parser.add_argument("-r", "--recursive", action="store_true")

    exp_parser = subparsers.add_parser("export")
    exp_parser.add_argument("-f", "--db-file", default="part_table.db")
    exp_parser.add_argument("-p", "--ptf-file", default="part_table.ptf")

    args = parser.parse_args()
    # print(args)

    try:
        dbconnection = None
        match args.command:
            case "import":
                dbconnection = sqlite3.connect(args.db_file)
                ptf_dir = os.path.join(args.ptf_dir, "**" if args.recursive else "", "*.ptf")
                for ptf in glob.iglob(ptf_dir, recursive=args.recursive):
                    print("Importing PTF file '{}'".format(ptf), end="", flush=True)
                    start_time = time.time()
                    pft_to_db(ptf, dbconnection)
                    print("\rImported PTF file '{}' ({:.2f}s)".format(ptf, (time.time()-start_time)))
            case "export":
                if not os.path.isfile(args.db_file):
                    print(f"{colors.fg.red}The database file '{args.db_file}' does not exist{colors.reset}")
                else:
                    dbconnection = sqlite3.connect(args.db_file)
                    print("Exporting database file {} to {}".format(args.db_file, args.ptf_file), end="", flush=True)
                    start_time = time.time()
                    db_to_ptf(args.ptf_file, dbconnection)
                    print("\rExported database file {} to {} ({:.2f}s)".format(args.db_file, args.ptf_file, (time.time()-start_time)))
    except sqlite3.Error as error:
        print(f"{colors.fg.red}Error occurred - {error}{colors.reset}")
    finally:
        if dbconnection:
            dbconnection.close()
