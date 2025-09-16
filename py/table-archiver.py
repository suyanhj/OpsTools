import argparse
import logging
import sys
import time
from datetime import datetime, timedelta
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


class ArchiveManager:
    """数据归档管理器"""

    def __init__(self, engine, debug=False, slow_ms=0, check_mode='in', idxs='',
                 batch_size=1000, do_delete=False,
                 check_schema=True, count_all=False, dry_run=False, analyze=False):
        self.engine = engine
        self.debug = debug
        self.slow_ms = slow_ms
        self.check_mode = check_mode
        self.logger = logging.getLogger("archiver")

        self.check_schema = check_schema
        self.count_all = count_all
        self.dry_run = dry_run
        self.analyze = analyze
        # 默认值（可被方法参数覆盖）
        self.batch_size = batch_size
        self.do_delete = do_delete

        if idxs:
            self.idxs = {
                idx[0].strip(): f"FORCE INDEX({idx[1]})" if idx else ""
                for idx in [_.strip().split('=') for _ in idxs.split(',')]
             }
        else:
            self.idxs = {}
        # self.idxs = f"FORCE INDEX({idx})" if idx else ""

    def run_query_sql(self, sql, params=None, fetch="all", mappings=False, scalar=False):
        """
        仅执行查询 SQL，并在连接关闭前完成结果提取，避免游标在连接关闭后被读取。
        - params: 可选参数字典
        - fetch: all | one | none
        - mappings: 是否返回字典风格行（依赖 .mappings()）
        - scalar: 是否返回标量（通常与 fetch=one 配合）
        """
        with self.engine.connect() as conn:
            result = conn.execute(text(sql), params or {})
            if scalar:
                return result.scalar_one_or_none()
            if mappings:
                return result.mappings().all() if fetch == "all" else (
                    result.mappings().one_or_none() if fetch == "one" else None)
            return result.fetchall() if fetch == "all" else (result.fetchone() if fetch == "one" else None)

    def get_table_fields(self, table_name):
        """检查表结构，返回虚拟字段、物理字段和id类型"""
        virtual_json_fields = []
        physical_fields = []
        id_sql_type = None
        sql = f"SHOW FULL COLUMNS FROM {table_name}"
        rows = self.run_query_sql(sql, fetch="all")
        if not rows:
            raise Exception(f"表 {table_name} 不存在")

        for row in rows:
            field = row[0]
            col_type = row[1]  # 完整类型，如 'bigint(20)' 或 'varchar(64)'
            extra = row[6].lower() if row[6] else ''

            if field == 'id':
                id_sql_type = col_type

            # 检查虚拟json字段
            if 'virtual' in extra or 'stored' in extra:
                virtual_json_fields.append({'field': field})
            else:
                physical_fields.append(field)
        return virtual_json_fields, physical_fields, id_sql_type

    def check_schema_compatibility(self, source_table, dest_table, physical_fields):
        """检查源表和目标表字段兼容性"""
        _, dest_physical_fields, _ = self.get_table_fields(dest_table)
        src_set = set(physical_fields)
        dst_set = set(dest_physical_fields)
        all_missing = src_set ^ dst_set
        src_2_dst = src_set - dst_set
        dst_2_src = dst_set - src_set
        flag = False
        if all_missing:
            self.logger.error(f"2表差异字段为: {all_missing}")
            flag = True
        if src_2_dst:
            self.logger.error(f"目标 {dest_table} 对比源表缺失: {src_2_dst}")
        if dst_2_src:
            self.logger.error(f"源表 {source_table} 对比目标缺失: {dst_2_src}")
        if flag:
            raise Exception("2表字段差异")

    def analyze_query_plan(self, source_table, where_clause):
        """分析查询计划，检查是否全表扫描"""

        explain_sql = f"""
            EXPLAIN SELECT id 
            FROM {source_table} {self.idxs.get(source_table)}
            WHERE {where_clause}
        """
        res = self.run_query_sql(explain_sql, fetch="all", mappings=True) or []
        if any([True for _ in res if _ and (_.get("type") == "ALL" or not _.get('key'))]):
            self.logger.warning(f"当前条件存在全表扫描")

    def count_total_records(self, source_table, where_clause):
        """统计待归档总数"""
        count_all_sql = f"""
            SELECT count(id) FROM {source_table} {self.idxs.get(source_table)}
            WHERE {where_clause}
        """
        total_count = self.run_query_sql(count_all_sql, fetch="one", scalar=True) or 0
        self.logger.info(f"待归档总数: {total_count}")
        return total_count

    def compute_boundary_ids(self, source_table, where_clause):
        """计算符合条件的最小/最大ID"""
        sql = f"""
            SELECT MIN(id) AS min_id, MAX(id) AS max_id
            FROM {source_table} {self.idxs.get(source_table)}
            WHERE {where_clause}
        """
        row = self.run_query_sql(sql, fetch="one")
        if not row or (row[0] is None and row[1] is None):
            return None, None
        return row[0], row[1]

    def query_batch_ids(self, source_table, where_clause, last_id, end_id, batch_size):
        """查询一批主键ID（增加上限 end_id）"""
        conds = [where_clause]
        params = {}

        if last_id is not None:
            conds.append("id > :last_id")
            params["last_id"] = last_id
        if end_id is not None:
            conds.append("id <= :end_id")
            params["end_id"] = end_id
        where_sql = " AND ".join([c for c in conds if c and c.strip()])
        select_ids_sql = f"""
            SELECT id FROM {source_table} {self.idxs.get(source_table)}
            WHERE {where_sql}
            ORDER BY id
            LIMIT {batch_size}
        """
        return select_ids_sql, params

    def process_with_join_mode(self, conn, source_table, dest_table, id_list, virtual_json_fields,
                               physical_fields, id_sql_type, do_delete):
        """使用临时表JOIN模式处理归档"""
        n_chk, n_ins, n_del, t_chk, t_ins, t_del = 0, 0, 0, 0, 0, 0
        temp_id_type = id_sql_type or 'varchar(128)'
        temp_table_name = f"temp_ids_{dest_table}"

        # 创建临时表
        conn.execute(text(
            f"CREATE TEMPORARY TABLE IF NOT EXISTS {temp_table_name} (id {temp_id_type} PRIMARY KEY) ENGINE=Memory DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_general_ci"))
        conn.execute(text(f"TRUNCATE TABLE {temp_table_name}"))

        # 批量插入临时表
        insert_params = {f"id_{i}": v for i, v in enumerate(id_list)}
        insert_values = ','.join([f"(:{k})" for k in insert_params.keys()])
        _tc = time.time()
        conn.execute(text(f"INSERT INTO {temp_table_name} (id) VALUES {insert_values}"), insert_params)

        # 校验已归档
        archived_sql = f"SELECT t.id FROM {temp_table_name} t JOIN {dest_table} d ON d.id=t.id"
        result = conn.execute(text(archived_sql))
        t_chk = int((time.time() - _tc) * 1000)
        n_chk = result.rowcount
        if self.slow_ms > 0:
            self.logger.info(f"阶段-校验已归档: {t_chk} ms, 已存在 {n_chk}")

        # 插入未归档数据
        if virtual_json_fields:
            fields_str = ','.join(physical_fields)
            s_fields_str = ','.join([f"s.{field}" for field in physical_fields])
            join_insert_sql = f"""
                INSERT INTO {dest_table} ({fields_str})
                SELECT {s_fields_str}
                FROM {source_table} s
                JOIN {temp_table_name} t ON s.id=t.id
                LEFT JOIN {dest_table} d ON s.id=d.id
                WHERE d.id IS NULL
            """
        else:
            join_insert_sql = f"""
                INSERT INTO {dest_table}
                SELECT s.*
                FROM {source_table} s
                JOIN {temp_table_name} t ON s.id=t.id
                LEFT JOIN {dest_table} d ON s.id=d.id
                WHERE d.id IS NULL
            """
        _ti = time.time()
        result = conn.execute(text(join_insert_sql))
        t_ins = int((time.time() - _ti) * 1000)
        n_ins = result.rowcount
        if n_ins and self.slow_ms > 0:
            self.logger.info(f"阶段-插入归档: {t_ins} ms, 插入 {n_ins}")

        # 删除源表
        if do_delete:
            del_sql = f"""
                DELETE s FROM {source_table} s
                JOIN {dest_table} d ON s.id=d.id
                JOIN {temp_table_name} t ON s.id=t.id
            """
            _td = time.time()
            result = conn.execute(text(del_sql))
            t_del = int((time.time() - _td) * 1000)
            n_del = result.rowcount
            if n_del and self.slow_ms > 0:
                self.logger.info(f"阶段-删除源表: {t_del} ms, 删除 {n_del}")

        return n_chk, n_ins, n_del, t_chk, t_ins, t_del

    def process_with_in_mode(self, conn, source_table, dest_table, id_list, virtual_json_fields,
                             physical_fields, do_delete):
        """使用IN模式处理归档"""
        n_chk, n_ins, n_del, t_chk, t_ins, t_del = 0, 0, 0, 0, 0, 0

        # 构造 IN 参数
        id_params = {f"id_{i}": v for i, v in enumerate(id_list)}
        in_clause = ','.join([f":{k}" for k in id_params.keys()])

        # 查归档表已存在的 id
        select_archived_sql = f"""
            SELECT id FROM {dest_table} WHERE id IN ({in_clause})
        """
        _tc = time.time()
        res = conn.execute(text(select_archived_sql), id_params)
        t_chk = int((time.time() - _tc) * 1000)
        archived_ids = set(row[0] for row in res.fetchall())
        n_chk = res.rowcount
        if self.slow_ms > 0:
            self.logger.info(f"阶段-校验已归档: {t_chk} ms, 已存在 {n_chk}")

        to_delete_ids = []
        to_insert_ids = []
        for i in id_list:
            if i in archived_ids:
                to_delete_ids.append(i)
            else:
                to_insert_ids.append(i)
                n_ins += 1
            n_del += 1

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
            _ti = time.time()
            conn.execute(text(insert_sql), insert_params)
            t_ins = int((time.time() - _ti) * 1000)
            if n_ins and self.slow_ms > 0:
                self.logger.info(f"阶段-插入归档: {t_ins} ms, 插入 {n_ins}")

        if do_delete:
            ids_to_remove = to_delete_ids + to_insert_ids
            if ids_to_remove:
                del_params = {f"id_{i}": v for i, v in enumerate(ids_to_remove)}
                del_in_clause = ','.join([f":{k}" for k in del_params.keys()])
                delete_sql = f"""
                    DELETE FROM {source_table} WHERE id IN ({del_in_clause})
                """
                _td = time.time()
                conn.execute(text(delete_sql), del_params)
                t_del = int((time.time() - _td) * 1000)
                if n_del and self.slow_ms > 0:
                    self.logger.info(f"阶段-删除源表: {t_del} ms, 删除 {n_del}")

        return n_chk, n_ins, n_del, t_chk, t_ins, t_del

    @run_time
    def archive_table(self, source_table, dest_table, where_clause, batch_size=None,
                      do_delete=None):
        """归档表的主方法"""
        # 覆盖默认参数（方法级优先级更高）
        if batch_size is None:
            batch_size = self.batch_size
        if do_delete is None:
            do_delete = self.do_delete

        virtual_json_fields, physical_fields, id_sql_type = self.get_table_fields(source_table)

        self.logger.info(f'归档表 {source_table} -> {dest_table}')
        self.logger.info(f"""条件 {where_clause}""")
        self.logger.debug(f"表 {source_table} 物理字段: {physical_fields}")

        if virtual_json_fields:
            self.logger.info(f"表 {source_table} 虚拟json字段:")
            for f in virtual_json_fields:
                self.logger.info(f"  {f['field']}")
        else:
            self.logger.info(f"表 {source_table} 没有虚拟json字段")

        # 归档前结构检查
        if self.check_schema:
            self.check_schema_compatibility(source_table, dest_table, physical_fields)

        if self.analyze:
            self.analyze_query_plan(source_table, where_clause)

        # 如果是试运行模式，只做检查不执行归档
        if self.dry_run:
            self.logger.info("试运行模式：仅检查字段和统计")
            return

        start_id, end_id = self.compute_boundary_ids(source_table, where_clause)
        if end_id is None:
            self.logger.info("没有符合条件的数据，直接结束")
            return
        self.logger.info(f"首尾ID: start_id={start_id}, end_id={end_id}")

        if self.count_all:
            self.count_total_records(source_table, where_clause)

        total_archived = 0
        total_deleted = 0
        total_queried = 0
        last_id = None

        while True:
            t0 = time.time()

            try:
                with self.engine.begin() as conn:
                    # 1. 查一批主键
                    select_ids_sql, params = self.query_batch_ids(source_table, where_clause, last_id, end_id,
                                                                  batch_size)
                    _ts = time.time()
                    res = conn.execute(text(select_ids_sql), params)
                    id_list = [row[0] for row in res.fetchall()]
                    t_sel = int((time.time() - _ts) * 1000)
                    n_sel = res.rowcount
                    if not id_list:
                        break
                    total_queried += n_sel
                    if self.slow_ms > 0:
                        self.logger.info(f"阶段-查询: {t_sel} ms, 行数 {n_sel}")

                    # 2. 根据模式处理归档
                    if self.check_mode == 'join':
                        n_chk, n_ins, n_del, t_chk, t_ins, t_del = self.process_with_join_mode(
                            conn, source_table, dest_table, id_list, virtual_json_fields,
                            physical_fields, id_sql_type, do_delete)
                    else:
                        n_chk, n_ins, n_del, t_chk, t_ins, t_del = self.process_with_in_mode(
                            conn, source_table, dest_table, id_list, virtual_json_fields,
                            physical_fields, do_delete)

                    total_archived += n_ins
                    total_deleted += n_del

                    # 3. 游标推进
                    if not last_id:
                        self.logger.info(f"首次游标id: {id_list[0]}")
                    last_id = id_list[-1]
            except Exception as e:
                self.logger.error(f"归档出错: {e}, 当前游标id: {last_id}")
                raise e

            elapsed_ms = int((time.time() - t0) * 1000)
            self.logger.info(
                f"当前游标id: {last_id}, 待归档: {n_sel} 行, 实际归档 {n_ins}, 删除: {n_del} 行, 总归档 {total_archived}, 总删除 {total_deleted}, 耗时 {elapsed_ms} ms")

            # 慢批次详情
            if 0 < self.slow_ms <= elapsed_ms:
                self.logger.info(
                    f"阶段耗时: select_ids={t_sel}ms/{n_sel} 行, check_archived={t_chk}ms/{n_chk} 行, insert={t_ins}ms/{n_ins} 行, delete={t_del}ms/{n_del} 行")

        self.logger.info(
            f"✅  {source_table} 归档完成, 总查询 {total_queried} 行, 总归档 {total_archived} 行, 总删除 {total_deleted} 行")


# ========== 主入口 ==========
def main():
    parser = argparse.ArgumentParser(description="MySQL 数据归档工具 (类似 pt-archiver)")
    parser.add_argument("-u", required=True, help="user")
    parser.add_argument("-p", required=False, help="password")
    parser.add_argument("-ip", required=False, help="host")
    parser.add_argument("-P", required=False, help="port")
    parser.add_argument("-d", required=True, help="数据库")
    parser.add_argument("-t", required=True, help="归档的表，多个表用','分隔，如 t1,t2")
    parser.add_argument("-dst-suffix", required=False,
                        help="归档到目标表后缀,自动拼接,默认 _history 后缀，多个表用','分隔，例如 t1_history,t2_history")
    parser.add_argument("--where", default="",
                        help="""归档条件 (SQL 片段，不带 WHERE)，例如: "create_time < '2023-01-01'" """)
    parser.add_argument("-idxs", help="指定索引,格式: 表名=索引,... ")
    parser.add_argument("--batch", type=int, default=1000, help="每批行数 (默认 1000)")
    parser.add_argument("--delete", action="store_true", help="归档后是否删除源表数据")
    parser.add_argument("--debug", action="store_true", help="开启debug模式，输出详细日志和SQL")
    parser.add_argument("--skip-schema-check", action="store_true", help="跳过源表与目标表字段一致性检查")
    parser.add_argument("-c", action="store_true", help="启动时统计所有即将归档的")
    parser.add_argument("--dry-run", action="store_true", help="试运行模式，仅检查字段、统计数据，不执行归档")
    parser.add_argument("--slow-ms", type=int, default=0, help="慢批次阈值(ms)，>0时输出阶段耗时与慢批详情")
    parser.add_argument("--analyze", action="store_true", help="分析归档条件是否扫全表")
    parser.add_argument("-m", choices=["in", "join"], default="in", help="校验模式：in(默认) 或 join(临时表)")
    args = parser.parse_args()

    # 设置日志
    global logger
    logger = setup_logger("archive.log", args.debug)
    logger.debug(f"启动归档，debug模式: {args.debug}")

    db = f'mysql+pymysql://{args.u}:{args.p}@{args.ip}:{args.P if args.P else 3306}/{args.d}?charset=utf8mb4&use_unicode=True'
    engine = create_engine(db, pool_recycle=3600, echo=args.debug)

    # 创建归档管理器
    archive_manager = ArchiveManager(engine, args.debug, args.slow_ms, args.m, idxs=args.idxs, batch_size=args.batch,
                                     do_delete=args.delete,
                                     check_schema=not args.skip_schema_check, count_all=args.c,
                                     dry_run=args.dry_run, analyze=args.analyze)

    tables = [t.strip() for t in args.t.split(",")]
    table_suffix = args.dst_suffix or "_history"
    for table in tables:
        logger.info(f"{'-' * 30} 开始归档表: {table} {'-' * 30}")
        dest_table = f"{table}{table_suffix}"
        archive_manager.archive_table(table, dest_table, args.where)


if __name__ == "__main__":
    logger = None
    try:
        main()
    except KeyboardInterrupt:
        logger.info("命令行中断")
        sys.exit(1)

    ## 测试
    #python3 tar2.py -u root -ip 192.168.10.10 -p MYSQL-SIT --where "create_time < '2025-08-10 00:00:00'" --debug -d paynow-deposit -t o_deposit_order,o_exception_order -idxs o_deposit_order=create_time,o_exception_order=page_index --slow-ms 10 -m join --analyze -c