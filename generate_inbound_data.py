#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
亚马逊 FBA 入库货件数据生成脚本
================================
自动生成 mws_fi_data_inbound_shipment 和 mws_fi_data_inbound_shipment_item 两张表的测试数据。

功能：
  - 生成货件主表数据（mws_fi_data_inbound_shipment）
  - 生成货件明细数据（mws_fi_data_inbound_shipment_item），自动关联主表
  - 支持 CSV 文件导出 或 直接写入 MySQL 数据库
  - 命令行参数灵活配置

用法：
  # 生成 50 个货件，每个货件 1~5 个 SKU，导出为 CSV
  python generate_inbound_data.py --count 50 --output csv

  # 生成 100 个货件，每个货件 3~8 个 SKU，直接写入 MySQL
  python generate_inbound_data.py --count 100 --output db \
      --host 127.0.0.1 --port 3306 --user root --password 123456 --database test_db

  # 只生成 SQL 语句（不执行），保存到文件
  python generate_inbound_data.py --count 30 --output sql --sql-file insert_data.sql

依赖安装：
  pip install faker pymysql
"""

import argparse
import csv
import os
import random
import sys
from datetime import datetime, timedelta

try:
    from faker import Faker
except ImportError:
    print("❌ 缺少 faker 库，请执行: pip install faker")
    sys.exit(1)

# ============================================================
# 常量 & 配置
# ============================================================

# 亚马逊常见 MarketPlace ID
MARKETPLACE_IDS = [
    "ATVPDKIKX0DER",  # 美国
    "A2EUQ1WTGCTBG2",  # 加拿大
    "A1AM78C64UM0Y8",  # 墨西哥
    "A1PA6795UKMFR9",  # 德国
    "A1RKKUPIHCS9HS",  # 西班牙
    "A13V1IB3VIYZZH",  # 法国
    "APJ6JRA9NG5V4",   # 意大利
    "A1F83G8C2ARO7P",  # 英国
    "A21TJRUUN4KGV",   # 印度
    "A39IBJ37TRP1C6",  # 澳大利亚
    "A1VC38T7YXB528",  # 日本
]

# 货件状态枚举
SHIPMENT_STATUS_MAP = {
    1: "处理中",
    2: "已发货",
    3: "在途",
    4: "已交付",
    5: "检查中",
    6: "接收中",
    7: "已完成",
    8: "已取消",
    9: "已删除",
    10: "错误",
}

# 货件状态权重（模拟真实分布：大部分是已完成/已发货）
SHIPMENT_STATUS_WEIGHTS = [5, 15, 10, 10, 3, 5, 30, 10, 8, 4]

# 标签类型
LABEL_PREP_TYPES = ["NO_LABEL", "SELLER_LABEL", "AMAZON_LABEL"]

# 箱内物品来源
BOX_CONTENTS_SOURCES = ["NONE", "FBA_PACKAGING", "2D_BARCODE"]

# 货币代码
CURRENCY_CODES = ["USD", "CAD", "EUR", "GBP", "JPY", "INR", "AUD", "MXN"]

# 国家代码
COUNTRY_CODES = ["US", "CA", "MX", "DE", "ES", "FR", "IT", "GB", "IN", "AU", "JP"]


# ============================================================
# 数据生成器
# ============================================================

class InboundShipmentGenerator:
    """亚马逊入库货件数据生成器"""

    def __init__(self, locale='zh_CN', seed=None):
        """
        初始化生成器

        Args:
            locale: Faker 地区设置
            seed: 随机种子，设置后可复现数据
        """
        self.fake = Faker(locale)
        self.fake_en = Faker('en_US')
        if seed is not None:
            Faker.seed(seed)
            random.seed(seed)

        # 用于去重的集合
        self._used_shipment_ids = set()
        self._used_seller_ids = set()

        # 预生成一批 Seller ID，模拟多个卖家
        self._seller_pool = [self._gen_seller_id() for _ in range(20)]

    def _gen_seller_id(self):
        """生成卖家 ID，格式 A + 13位字母数字"""
        sid = 'A' + ''.join(random.choices('ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789', k=13))
        return sid

    def _gen_shipment_id(self):
        """生成货件 ID，格式 FBA + 15位字母数字"""
        while True:
            prefix = random.choice(['FBA15', 'FBA18', 'FBA19', 'FBA20', 'FBA21', 'FBA22', 'FBA23', 'FBA24'])
            suffix = ''.join(random.choices('ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789', k=random.randint(6, 10)))
            sid = prefix + suffix
            if sid not in self._used_shipment_ids:
                self._used_shipment_ids.add(sid)
                return sid

    def _gen_seller_sku(self):
        """生成卖家 SKU"""
        patterns = [
            lambda: f"SKU-{''.join(random.choices('ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789', k=6))}",
            lambda: f"{''.join(random.choices('ABCDEFGHIJKLMNOPQRSTUVWXYZ', k=3))}-{''.join(random.choices('0123456789', k=5))}",
            lambda: f"PROD-{random.randint(10000, 99999)}",
            lambda: f"AS-{self.fake_en.bothify('??##??##')}",
        ]
        return random.choice(patterns)()

    def _gen_fnsku(self):
        """生成 FNSKU，格式 X00 + 7位字母数字"""
        return 'X00' + ''.join(random.choices('ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789', k=7))

    def _gen_fulfillment_center_id(self):
        """生成亚马逊配送中心 ID"""
        prefixes = ['PHX', 'LAX', 'SEA', 'OAK', 'SDF', 'IND', 'CMH', 'AVP', 'ABE', 'BWI',
                    'CLT', 'RDU', 'JAX', 'MCO', 'TPA', 'DFW', 'IAH', 'SAT', 'DEN', 'SLC',
                    'MKE', 'MSP', 'ORD', 'DTW', 'YYZ', 'YYZ3', 'YUL', 'LCY', 'BHX', 'MAN',
                    'CGN', 'MUC', 'TXL', 'CDG', 'ORY', 'MAD', 'BCN', 'MXP', 'FCO']
        suffix = ''.join(random.choices('0123456789', k=random.randint(1, 2)))
        return random.choice(prefixes) + suffix

    def _random_date_in_range(self, days_back=90):
        """生成最近 N 天内的随机日期"""
        end_date = datetime.now()
        start_date = end_date - timedelta(days=days_back)
        random_seconds = random.randint(0, int((end_date - start_date).total_seconds()))
        return start_date + timedelta(seconds=random_seconds)

    def generate_shipment(self, marketplace_id=None, seller_id=None):
        """
        生成一条货件主表数据

        Args:
            marketplace_id: 指定市场 ID，不指定则随机
            seller_id: 指定卖家 ID，不指定则随机

        Returns:
            dict: 货件主表数据
        """
        if marketplace_id is None:
            marketplace_id = random.choice(MARKETPLACE_IDS)
        if seller_id is None:
            seller_id = random.choice(self._seller_pool)

        shipment_id = self._gen_shipment_id()
        shipment_name = f"Shipment-{self.fake_en.bothify('##??####')}-{random.randint(1, 999)}"

        # 发货地址
        ship_from_name = self.fake.company()
        address_line1 = self.fake.street_address()
        address_line2 = self.fake_en.secondary_address() if random.random() > 0.4 else None
        district = self.fake.city_suffix() if random.random() > 0.5 else None
        city = self.fake.city()
        state = self.fake_en.state_abbr() if random.random() > 0.3 else self.fake_en.state()
        postal_code = self.fake_en.zipcode()
        country_code = random.choice(COUNTRY_CODES)

        # 货件属性
        destination_fc = self._gen_fulfillment_center_id()
        label_prep_type = random.choice(LABEL_PREP_TYPES)
        shipment_status = random.choices(
            list(SHIPMENT_STATUS_MAP.keys()),
            weights=SHIPMENT_STATUS_WEIGHTS,
            k=1
        )[0]
        are_cases_required = random.choice([True, False])

        # 日期
        create_time = self._random_date_in_range(days_back=180)
        update_time = None
        if shipment_status >= 2:
            update_time = create_time + timedelta(hours=random.randint(1, 720))
        confirmed_need_by_date = None
        if shipment_status in [2, 3, 4, 5, 6, 7]:
            confirmed_need_by_date = (create_time + timedelta(days=random.randint(7, 30))).strftime('%Y-%m-%d')

        # 费用 & 数量
        total_units = random.randint(10, 5000)
        currency_code = random.choice(CURRENCY_CODES)
        fee_per_unit = round(random.uniform(0.1, 5.0), 2)
        total_fee = round(fee_per_unit * total_units, 2)

        box_contents_source = random.choice(BOX_CONTENTS_SOURCES)

        # 预约号
        amazon_reference_id = None
        if shipment_status >= 2:
            amazon_reference_id = ''.join(random.choices('0123456789', k=10))

        return {
            'MARKETPLACE_ID': marketplace_id,
            'SELLER_ID': seller_id,
            'SHIPMENT_ID': shipment_id,
            'SHIPMENT_NAME': shipment_name,
            'SHIP_FROM_NAME': ship_from_name,
            'SHIP_FROM_ADDRESS_LINE1': address_line1,
            'SHIP_FROM_ADDRESS_LINE2': address_line2,
            'SHIP_FROM_DISTRICT_OR_COUNTY': district,
            'SHIP_FROM_CITY': city,
            'SHIP_FROM_STATE_OR_PROVINCE_CODE': state,
            'SHIP_FROM_POSTAL_CODE': postal_code,
            'SHIP_FROM_COUNTRY_CODE': country_code,
            'DESTINATION_FULFILLMENT_CENTER_ID': destination_fc,
            'LABEL_PREP_TYPE': label_prep_type,
            'SHIPMENT_STATUS': shipment_status,
            'ARE_CASES_REQUIRED': are_cases_required,
            'CONFIRMED_NEED_BY_DATE': confirmed_need_by_date,
            'BOX_CONTENTS_SOURCE': box_contents_source,
            'TOTAL_UNITS': total_units,
            'CURRENCY_CODE': currency_code,
            'FEE_PER_UNIT': fee_per_unit,
            'TOTAL_FEE': total_fee,
            'AMAZON_REFERENCE_ID': amazon_reference_id,
            'CREATE_TIME': create_time.strftime('%Y-%m-%d %H:%M:%S'),
            'UPDATE_TIME': update_time.strftime('%Y-%m-%d %H:%M:%S') if update_time else None,
        }

    def generate_shipment_items(self, shipment, min_items=1, max_items=5):
        """
        为指定货件生成明细数据

        Args:
            shipment: 货件主表数据（dict）
            min_items: 最少 SKU 数量
            max_items: 最多 SKU 数量

        Returns:
            list[dict]: 明细数据列表
        """
        num_items = random.randint(min_items, max_items)
        items = []
        used_skus = set()

        # 货件状态影响明细的接收情况
        shipment_status = shipment['SHIPMENT_STATUS']

        for _ in range(num_items):
            # SKU 去重
            while True:
                seller_sku = self._gen_seller_sku()
                if seller_sku not in used_skus:
                    used_skus.add(seller_sku)
                    break

            fnsku = self._gen_fnsku()

            # 发货数量
            quantity_shipped = random.randint(1, 500)

            # 接收数量：根据货件状态决定
            if shipment_status in [1, 2, 3]:
                # 未到仓，接收为0或部分
                quantity_received = 0
            elif shipment_status in [4, 5, 6]:
                # 部分接收
                quantity_received = random.randint(int(quantity_shipped * 0.5), quantity_shipped)
            elif shipment_status == 7:
                # 已完成，大部分接收
                quantity_received = random.randint(int(quantity_shipped * 0.9), quantity_shipped)
            elif shipment_status == 8:
                # 已取消
                quantity_received = 0
                quantity_shipped = 0
            else:
                quantity_received = random.randint(0, quantity_shipped)

            # 原厂包装数量
            quantity_in_case = random.randint(1, 24) if random.random() > 0.3 else None

            # 时间
            create_time = shipment['CREATE_TIME']
            update_time = shipment['UPDATE_TIME']

            # 全部签收时间
            all_sign_time = None
            if shipment_status == 7 and quantity_received >= quantity_shipped * 0.95:
                base_time = datetime.strptime(create_time, '%Y-%m-%d %H:%M:%S')
                all_sign_time = (base_time + timedelta(days=random.randint(3, 30))).strftime('%Y-%m-%d %H:%M:%S')

            items.append({
                'MARKETPLACE_ID': shipment['MARKETPLACE_ID'],
                'SELLER_ID': shipment['SELLER_ID'],
                'SHIPMENT_ID': shipment['SHIPMENT_ID'],
                'SELLER_SKU': seller_sku,
                'FNSKU': fnsku,
                'QUANTITY_SHIPPED': quantity_shipped,
                'QUANTITY_RECEIVED': quantity_received,
                'QUANTITY_IN_CASE': quantity_in_case,
                'CREATE_TIME': create_time,
                'UPDATE_TIME': update_time,
                'ALL_SIGN_TIME': all_sign_time,
            })

        return items

    def generate_batch(self, count=50, min_items=1, max_items=5):
        """
        批量生成货件及明细数据

        Args:
            count: 货件数量
            min_items: 每个货件最少 SKU 数
            max_items: 每个货件最多 SKU 数

        Returns:
            tuple: (shipments_list, items_list)
        """
        shipments = []
        items = []

        for i in range(count):
            shipment = self.generate_shipment()
            shipment_items = self.generate_shipment_items(shipment, min_items, max_items)

            shipments.append(shipment)
            items.extend(shipment_items)

            if (i + 1) % 100 == 0:
                print(f"  已生成 {i + 1}/{count} 个货件...")

        return shipments, items


# ============================================================
# 输出方式
# ============================================================

def export_to_csv(shipments, items, output_dir='.'):
    """导出为 CSV 文件"""
    os.makedirs(output_dir, exist_ok=True)

    # 货件主表 CSV
    shipment_file = os.path.join(output_dir, 'mws_fi_data_inbound_shipment.csv')
    if shipments:
        keys = shipments[0].keys()
        with open(shipment_file, 'w', newline='', encoding='utf-8-sig') as f:
            writer = csv.DictWriter(f, fieldnames=keys)
            writer.writeheader()
            writer.writerows(shipments)
        print(f"✅ 货件主表已导出: {shipment_file} ({len(shipments)} 条)")

    # 明细表 CSV
    item_file = os.path.join(output_dir, 'mws_fi_data_inbound_shipment_item.csv')
    if items:
        keys = items[0].keys()
        with open(item_file, 'w', newline='', encoding='utf-8-sig') as f:
            writer = csv.DictWriter(f, fieldnames=keys)
            writer.writeheader()
            writer.writerows(items)
        print(f"✅ 货件明细已导出: {item_file} ({len(items)} 条)")


def export_to_sql(shipments, items, sql_file='insert_data.sql'):
    """生成 INSERT SQL 语句并保存到文件"""
    os.makedirs(os.path.dirname(sql_file) if os.path.dirname(sql_file) else '.', exist_ok=True)

    with open(sql_file, 'w', encoding='utf-8') as f:
        f.write("-- ============================================\n")
        f.write("-- 亚马逊 FBA 入库货件测试数据\n")
        f.write(f"-- 生成时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
        f.write(f"-- 货件数量: {len(shipments)}, 明细数量: {len(items)}\n")
        f.write("-- ============================================\n\n")

        # 插入货件主表
        if shipments:
            f.write("-- 货件主表数据\n")
            for s in shipments:
                cols = ', '.join(s.keys())
                vals = ', '.join([_sql_value(v) for v in s.values()])
                f.write(f"INSERT INTO `mws_fi_data_inbound_shipment` ({cols}) VALUES ({vals});\n")
            f.write(f"\n-- 共 {len(shipments)} 条货件数据\n\n")

        # 插入明细表
        if items:
            f.write("-- 货件明细数据\n")
            for item in items:
                cols = ', '.join(item.keys())
                vals = ', '.join([_sql_value(v) for v in item.values()])
                f.write(f"INSERT INTO `mws_fi_data_inbound_shipment_item` ({cols}) VALUES ({vals});\n")
            f.write(f"\n-- 共 {len(items)} 条明细数据\n")

    print(f"✅ SQL 文件已导出: {sql_file}")


def _sql_value(value):
    """将 Python 值转为 SQL 值"""
    if value is None:
        return 'NULL'
    elif isinstance(value, bool):
        return '1' if value else '0'
    elif isinstance(value, (int, float)):
        return str(value)
    else:
        escaped = str(value).replace("'", "\\'").replace("\\", "\\\\")
        return f"'{escaped}'"


def insert_to_mysql(shipments, items, host, port, user, password, database, batch_size=500):
    """直接写入 MySQL 数据库"""
    try:
        import pymysql
    except ImportError:
        print("❌ 缺少 pymysql 库，请执行: pip install pymysql")
        sys.exit(1)

    conn = pymysql.connect(
        host=host,
        port=port,
        user=user,
        password=password,
        database=database,
        charset='utf8mb4',
    )

    try:
        cursor = conn.cursor()

        # 插入货件主表
        print(f"\n📦 正在插入货件主表数据...")
        shipment_sql = """
            INSERT INTO `mws_fi_data_inbound_shipment`
            (MARKETPLACE_ID, SELLER_ID, SHIPMENT_ID, SHIPMENT_NAME,
             SHIP_FROM_NAME, SHIP_FROM_ADDRESS_LINE1, SHIP_FROM_ADDRESS_LINE2,
             SHIP_FROM_DISTRICT_OR_COUNTY, SHIP_FROM_CITY,
             SHIP_FROM_STATE_OR_PROVINCE_CODE, SHIP_FROM_POSTAL_CODE,
             SHIP_FROM_COUNTRY_CODE, DESTINATION_FULFILLMENT_CENTER_ID,
             LABEL_PREP_TYPE, SHIPMENT_STATUS, ARE_CASES_REQUIRED,
             CONFIRMED_NEED_BY_DATE, BOX_CONTENTS_SOURCE, TOTAL_UNITS,
             CURRENCY_CODE, FEE_PER_UNIT, TOTAL_FEE, AMAZON_REFERENCE_ID,
             CREATE_TIME, UPDATE_TIME)
            VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
        """
        shipment_values = [
            (
                s['MARKETPLACE_ID'], s['SELLER_ID'], s['SHIPMENT_ID'], s['SHIPMENT_NAME'],
                s['SHIP_FROM_NAME'], s['SHIP_FROM_ADDRESS_LINE1'], s['SHIP_FROM_ADDRESS_LINE2'],
                s['SHIP_FROM_DISTRICT_OR_COUNTY'], s['SHIP_FROM_CITY'],
                s['SHIP_FROM_STATE_OR_PROVINCE_CODE'], s['SHIP_FROM_POSTAL_CODE'],
                s['SHIP_FROM_COUNTRY_CODE'], s['DESTINATION_FULFILLMENT_CENTER_ID'],
                s['LABEL_PREP_TYPE'], s['SHIPMENT_STATUS'], s['ARE_CASES_REQUIRED'],
                s['CONFIRMED_NEED_BY_DATE'], s['BOX_CONTENTS_SOURCE'], s['TOTAL_UNITS'],
                s['CURRENCY_CODE'], s['FEE_PER_UNIT'], s['TOTAL_FEE'], s['AMAZON_REFERENCE_ID'],
                s['CREATE_TIME'], s['UPDATE_TIME'],
            )
            for s in shipments
        ]

        for i in range(0, len(shipment_values), batch_size):
            batch = shipment_values[i:i + batch_size]
            cursor.executemany(shipment_sql, batch)
            conn.commit()
            print(f"  已插入 {min(i + batch_size, len(shipment_values))}/{len(shipment_values)} 条")

        # 插入明细表
        print(f"\n📋 正在插入货件明细数据...")
        item_sql = """
            INSERT INTO `mws_fi_data_inbound_shipment_item`
            (MARKETPLACE_ID, SELLER_ID, SHIPMENT_ID, SELLER_SKU, FNSKU,
             QUANTITY_SHIPPED, QUANTITY_RECEIVED, QUANTITY_IN_CASE,
             CREATE_TIME, UPDATE_TIME, ALL_SIGN_TIME)
            VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
        """
        item_values = [
            (
                item['MARKETPLACE_ID'], item['SELLER_ID'], item['SHIPMENT_ID'],
                item['SELLER_SKU'], item['FNSKU'],
                item['QUANTITY_SHIPPED'], item['QUANTITY_RECEIVED'], item['QUANTITY_IN_CASE'],
                item['CREATE_TIME'], item['UPDATE_TIME'], item['ALL_SIGN_TIME'],
            )
            for item in items
        ]

        for i in range(0, len(item_values), batch_size):
            batch = item_values[i:i + batch_size]
            cursor.executemany(item_sql, batch)
            conn.commit()
            print(f"  已插入 {min(i + batch_size, len(item_values))}/{len(item_values)} 条")

        print(f"\n✅ 数据写入完成！货件 {len(shipments)} 条，明细 {len(items)} 条")

    except Exception as e:
        conn.rollback()
        print(f"❌ 数据库写入失败: {e}")
        raise
    finally:
        cursor.close()
        conn.close()


# ============================================================
# 主入口
# ============================================================

def main():
    parser = argparse.ArgumentParser(
        description='亚马逊 FBA 入库货件数据生成脚本',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
示例:
  # 生成 50 个货件，导出 CSV
  python generate_inbound_data.py --count 50 --output csv

  # 生成 100 个货件，写入 MySQL
  python generate_inbound_data.py --count 100 --output db --host 127.0.0.1 --user root --password 123456 --database test

  # 生成 30 个货件，导出 SQL 文件
  python generate_inbound_data.py --count 30 --output sql --sql-file data.sql

  # 指定每个货件 SKU 数量范围
  python generate_inbound_data.py --count 50 --output csv --min-items 2 --max-items 10
        """
    )

    # 基本参数
    parser.add_argument('--count', type=int, default=50, help='生成货件数量（默认 50）')
    parser.add_argument('--min-items', type=int, default=1, help='每个货件最少 SKU 数（默认 1）')
    parser.add_argument('--max-items', type=int, default=5, help='每个货件最多 SKU 数（默认 5）')
    parser.add_argument('--seed', type=int, default=None, help='随机种子，设置后可复现数据')

    # 输出方式
    parser.add_argument('--output', choices=['csv', 'db', 'sql'], default='csv',
                        help='输出方式: csv=导出CSV文件, db=写入MySQL, sql=生成SQL文件（默认 csv）')
    parser.add_argument('--output-dir', type=str, default='./output', help='CSV 输出目录（默认 ./output）')
    parser.add_argument('--sql-file', type=str, default='./insert_data.sql', help='SQL 文件路径（默认 ./insert_data.sql）')

    # 数据库参数
    parser.add_argument('--host', type=str, default='127.0.0.1', help='MySQL 主机（默认 127.0.0.1）')
    parser.add_argument('--port', type=int, default=3306, help='MySQL 端口（默认 3306）')
    parser.add_argument('--user', type=str, default='root', help='MySQL 用户名（默认 root）')
    parser.add_argument('--password', type=str, default='', help='MySQL 密码')
    parser.add_argument('--database', type=str, default='test', help='MySQL 数据库名（默认 test）')

    args = parser.parse_args()

    # 参数校验
    if args.min_items > args.max_items:
        print("❌ --min-items 不能大于 --max-items")
        sys.exit(1)

    if args.output == 'db' and not args.password:
        print("⚠️  写入数据库需要提供 --password 参数")

    # 开始生成
    print("=" * 60)
    print("🚀 亚马逊 FBA 入库货件数据生成器")
    print("=" * 60)
    print(f"  货件数量: {args.count}")
    print(f"  每个货件 SKU 数: {args.min_items} ~ {args.max_items}")
    print(f"  预计明细数量: ~{args.count * (args.min_items + args.max_items) // 2}")
    print(f"  输出方式: {args.output.upper()}")
    if args.seed is not None:
        print(f"  随机种子: {args.seed}")
    print("=" * 60)

    generator = InboundShipmentGenerator(seed=args.seed)
    shipments, items = generator.generate_batch(
        count=args.count,
        min_items=args.min_items,
        max_items=args.max_items,
    )

    # 打印状态分布
    print(f"\n📊 货件状态分布:")
    status_count = {}
    for s in shipments:
        status = s['SHIPMENT_STATUS']
        status_name = SHIPMENT_STATUS_MAP.get(status, '未知')
        key = f"{status}-{status_name}"
        status_count[key] = status_count.get(key, 0) + 1
    for k, v in sorted(status_count.items(), key=lambda x: int(x[0].split('-')[0])):
        bar = '█' * (v * 30 // args.count)
        print(f"  {k:12s}: {v:4d}  {bar}")

    print(f"\n📊 数据统计:")
    print(f"  货件总数: {len(shipments)}")
    print(f"  明细总数: {len(items)}")
    print(f"  平均每货件 SKU: {len(items) / len(shipments):.1f}")

    # 输出
    if args.output == 'csv':
        export_to_csv(shipments, items, args.output_dir)
    elif args.output == 'sql':
        export_to_sql(shipments, items, args.sql_file)
    elif args.output == 'db':
        insert_to_mysql(
            shipments, items,
            host=args.host, port=args.port,
            user=args.user, password=args.password,
            database=args.database,
        )

    print("\n🎉 完成！")


if __name__ == '__main__':
    main()
