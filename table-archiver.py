import argparse
import logging
import sys
import time
from datetime import datetime, timedelta
from enum import Flag
from functools import wraps

from sqlalchemy import create_engine, text, inspect


def run_time(func):
    @wraps(func)
    def wrapper(*args, **kwargs):
        start_time = datetime.now()
        res = func(*args, **kwargs)
        end_time = datetime.now()
        logger.info(f"开始 {start_time}")
        logger.info(f"完成 {end_time} ,耗时: {(end_time - start_time).total_seconds()}s")
        return res

    return wrapper


# ========== 日志配置 ==========
def setup_logger(logfile="archive.log", debug=False):
    log = logging.getLogger("archiver")
    log.setLevel(logging.DEBUG if debug else logging.INFO)
    if not log.handlers:
        fh = logging.FileHandler(logfile, encoding="utf-8")
        fmt = logging.Formatter("[%(asctime)s] %(levelname)s: %(message)s")
        fh.setFormatter(fmt)
        log.addHandler(fh)
        sh = logging.StreamHandler(sys.stdout)
        sh.setFormatter(fmt)
        log.addHandler(sh)
    return log


# ========== 归档逻辑 ==========
def get_table_fields(engine, table_name):
    """
    检查表结构，返回id字段类型（int/varchar），以及所有虚拟json字段名和定义
    同时返回所有物理字段名（不含虚拟字段）
    """
    with engine.connect() as conn:
        id_type = None
        virtual_json_fields = []
        physical_fields = []
        sql = f"SHOW FULL COLUMNS FROM {table_name}"
        rows = conn.execute(text(sql)).fetchall()
        if not rows:
            raise Exception(f"表 {table_name} 不存在")

        for row in rows:
            field = row[0]
            col_type = row[1].lower()
            extra = row[6].lower() if row[6] else ''
            # comment = row[-1]
            if field == 'id':
                if 'int' in col_type:
                    id_type = 'int'
                elif 'char' in col_type:
                    id_type = 'varchar'
            # 检查虚拟json字段
            if 'virtual' in extra or 'stored' in extra:
                # if 'json_extract' in row[5].lower() or 'json_unquote' in row[5].lower():
                virtual_json_fields.append({
                    'field': field,
                    # 'type': col_type,
                    # 'default': row[5],
                    # 'comment': comment
                })
            else:
                physical_fields.append(field)
        return id_type, virtual_json_fields, physical_fields


@run_time
def archive_table(engine, source_table, dest_table, where_clause, batch_size, do_delete=False, check_schema=True):
    id_type, virtual_json_fields, physical_fields = get_table_fields(engine, source_table)
    logger.info(f'归档表 {source_table} -> {dest_table}')
    logger.info(f"""条件 {where_clause}""")
    logger.info(f"表 {source_table} id 字段类型: {id_type}")
    logger.debug(f"表 {source_table} 物理字段: {physical_fields}")
    if virtual_json_fields:
        logger.info(f"表 {source_table} 虚拟json字段:")
        for f in virtual_json_fields:
            logger.info(f"  {f['field']}")
            # logger.info(f"  {f['field']} {f['type']} {f['default']} 备注:{f['comment']}")
    else:
        logger.info(f"表 {source_table} 没有虚拟json字段")

    # 归档前结构检查：否缺少物理字段
    if check_schema:
        _, _, dest_physical_fields = get_table_fields(engine, dest_table)
        src_set = set(physical_fields)
        dst_set = set(dest_physical_fields)
        all_missing = src_set ^ dst_set
        src_2_dst = src_set - dst_set
        dst_2_src = dst_set - src_set
        flag = False
        if all_missing:
            logger.error(f"2表差异字段为: {all_missing}")
            flag = True
        if src_2_dst:
            logger.error(f"目标 {dest_table} 对比源表缺失: {src_2_dst}")
        if dst_2_src:
            logger.error(f"源表 {source_table} 对比目标缺失: {dst_2_src}")
        if flag:
            raise Exception("2表字段差异")

    total_archived = 0
    total_deleted = 0
    total_queried = 0
    last_id = None

    while True:
        t0 = time.time()
        status = "SUCCESS"
        deleted_count = 0

        try:
            with engine.begin() as conn:
                # 1. 查一批主键
                if last_id is not None:
                    if id_type == 'int':
                        id_cond = f" AND id > {last_id}"
                    else:
                        id_cond = f" AND id > '{last_id}'"
                else:
                    id_cond = ""

                select_ids_sql = f"""
                    SELECT id FROM {source_table} FORCE INDEX(`PRIMARY`)
                    WHERE {where_clause} {id_cond}
                    ORDER BY id
                    LIMIT {batch_size}
                """
                res = conn.execute(text(select_ids_sql))
                id_list = [row[0] for row in res.fetchall()]
                if not id_list:
                    break
                total_queried += res.rowcount

                # 构造 IN 参数
                id_params = {f"id_{i}": id_list[i] for i in range(res.rowcount)}
                in_clause = ','.join([f":{k}" for k in id_params.keys()])

                # 2. 查归档表已存在的 id
                select_archived_sql = f"""
                    SELECT id FROM {dest_table} FORCE INDEX(`PRIMARY`) WHERE id IN ({in_clause})
                """
                archived_rows = conn.execute(text(select_archived_sql), id_params).fetchall()
                archived_ids = set(row[0] for row in archived_rows)

                #
                to_delete_ids = []
                to_insert_ids = []
                for i in id_list:
                    if i in archived_ids:
                        to_delete_ids.append(i)
                    else:
                        to_insert_ids.append(i)

                if to_insert_ids:
                    insert_params = {f"id_{i}": v for i, v in enumerate(to_insert_ids)}
                    insert_in_clause = ','.join([f":{k}" for k in insert_params.keys()])
                    if virtual_json_fields:
                        fields_str = ','.join(physical_fields)
                        insert_sql = f"""
                            INSERT INTO {dest_table} ({fields_str}) SELECT {fields_str} FROM {source_table} FORCE INDEX(`PRIMARY`) WHERE id IN ({insert_in_clause})
                        """
                    else:
                        insert_sql = f"""
                            INSERT INTO {dest_table} SELECT * FROM {source_table} FORCE INDEX(`PRIMARY`) WHERE id IN ({insert_in_clause})
                        """

                    conn.execute(text(insert_sql), insert_params)
                    total_archived += len(to_insert_ids)
                # 确认归档时删除
                if do_delete:
                    # 删除所有已归档
                    ids_to_remove = to_delete_ids + to_insert_ids
                    if ids_to_remove:
                        del_params = {f"id_{i}": v for i, v in enumerate(ids_to_remove)}
                        del_in_clause = ','.join([f":{k}" for k in del_params.keys()])
                        delete_sql = f"""
                            DELETE FROM {source_table} WHERE id IN ({del_in_clause})
                        """

                        conn.execute(text(delete_sql), del_params)
                        deleted_count = len(ids_to_remove)
                        total_deleted += deleted_count

                # 6. 游标推进
                last_id = id_list[-1]
        except Exception as e:
            status = "FAILED"
            logger.error(f"归档出错: {e}, 当前游标id: {last_id}")
            raise e

        elapsed_ms = int((time.time() - t0) * 1000)
        logger.info(
            f"当前游标id: {last_id}, 待归档: {len(id_list)} 行, 实际归档 {len(to_insert_ids) if 'to_insert_ids' in locals() else 0}, 删除: {deleted_count} 行, 总归档 {total_archived}, 总删除 {total_deleted}, 耗时 {elapsed_ms} ms, 状态 {status}")
    logger.info(
        f"✅ {source_table} 归档完成, 总归档 {total_archived} 行, 总删除 {total_deleted} 行, 总查询 {total_queried} 行")


# ========== 主入口 ==========
def main():
    parser = argparse.ArgumentParser(description="MySQL 数据归档工具 (类似 pt-archiver)")
    parser.add_argument("-u", required=True, help="user")
    parser.add_argument("-p", required=False, help="password")
    parser.add_argument("-ip", required=False, help="host")
    parser.add_argument("-P", required=False, help="port")
    parser.add_argument("-d", required=True, help="数据库")
    parser.add_argument("-t", required=True, help="归档的表，多个表用','分隔，例如 t1,t2")
    parser.add_argument("-dst-suffix", required=False,
                        help="归档到目标表后缀,自动拼接,默认 _history 后缀，多个表用','分隔，例如 t1_history,t2_history")
    parser.add_argument("--where", default="",
                        help="""归档条件 (SQL 片段，不带 WHERE)，例如: "create_time < '2023-01-01' AND status=1" """)
    parser.add_argument("--batch", type=int, default=100, help="每批行数 (默认 1000)")
    parser.add_argument("--delete", action="store_true", help="归档后是否删除源表数据")
    parser.add_argument("--debug", action="store_true", help="开启debug模式，输出详细日志和SQL")
    parser.add_argument("--skip-schema-check", action="store_true", help="跳过源表与目标表字段一致性检查")
    args = parser.parse_args()

    # 设置日志
    global logger
    logger = setup_logger("archive.log", args.debug)
    logger.debug(f"启动归档，debug模式: {args.debug}")

    db = f'mysql+pymysql://{args.u}:{args.p}@{args.ip}:{args.P if args.P else 3306}/{args.d}?charset=utf8mb4&use_unicode=True'
    engine = create_engine(db, pool_recycle=3600, echo=args.debug)

    tables = [t.strip() for t in args.t.split(",")]
    table_suffix = args.dst_suffix or "_history"
    for table in tables:
        dest_table = f"{table}{table_suffix}"
        archive_table(engine, table, dest_table, args.where, batch_size=args.batch, do_delete=args.delete,
                      check_schema=not args.skip_schema_check)


if __name__ == "__main__":
    logger = None
    main()
