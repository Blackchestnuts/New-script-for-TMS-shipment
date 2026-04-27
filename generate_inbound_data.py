#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
亚马逊 FBA 入库货件 —— 交互式数据生成脚本
============================================
支持自定义选择字段、自定义字段值、控制数据条数，交互式造数据。

功能：
  - 交互式选择要生成的字段
  - 每个字段可选：自动生成 / 固定值 / 自定义范围
  - 支持保存/加载配置模板，下次直接复用
  - 三种输出方式：CSV / SQL / 直连MySQL

用法：
  # 交互式模式（推荐）
  python generate_inbound_data.py

  # 使用已保存的模板快速生成
  python generate_inbound_data.py --template default

  # 命令行模式（非交互式）
  python generate_inbound_data.py --count 50 --output csv

依赖安装：
  pip install faker pymysql
"""

import argparse
import csv
import json
import os
import random
import sys
from datetime import datetime, timedelta

try:
    from faker import Faker
except ImportError:
    print("缺少 faker 库，请执行: pip install faker")
    sys.exit(1)

# ============================================================
# 字段定义 —— 每个字段的元信息
# ============================================================

SHIPMENT_FIELDS = {
    "MARKETPLACE_ID": {
        "label": "市场ID",
        "type": "choice",
        "choices": [
            "ATVPDKIKX0DER", "A2EUQ1WTGCTBG2", "A1AM78C64UM0Y8",
            "A1PA6795UKMFR9", "A1RKKUPIHCS9HS", "A13V1IB3VIYZZH",
            "APJ6JRA9NG5V4", "A1F83G8C2ARO7P", "A21TJRUUN4KGV",
            "A39IBJ37TRP1C6", "A1VC38T7YXB528",
        ],
        "required": True,
        "default_gen": "random_choice",
    },
    "SELLER_ID": {
        "label": "卖家ID",
        "type": "string",
        "pattern": "A + 13位字母数字",
        "required": True,
        "default_gen": "seller_id",
    },
    "SHIPMENT_ID": {
        "label": "货件ID",
        "type": "string",
        "pattern": "FBA + 6~10位字母数字",
        "required": True,
        "default_gen": "shipment_id",
    },
    "SHIPMENT_NAME": {
        "label": "货件名称",
        "type": "string",
        "default_gen": "shipment_name",
    },
    "SHIP_FROM_NAME": {
        "label": "发货方名称",
        "type": "string",
        "default_gen": "company",
    },
    "SHIP_FROM_ADDRESS_LINE1": {
        "label": "地址1",
        "type": "string",
        "default_gen": "street_address",
    },
    "SHIP_FROM_ADDRESS_LINE2": {
        "label": "地址2",
        "type": "string",
        "default_gen": "secondary_address",
        "nullable": True,
    },
    "SHIP_FROM_DISTRICT_OR_COUNTY": {
        "label": "区/县",
        "type": "string",
        "default_gen": "district",
        "nullable": True,
    },
    "SHIP_FROM_CITY": {
        "label": "城市",
        "type": "string",
        "default_gen": "city",
    },
    "SHIP_FROM_STATE_OR_PROVINCE_CODE": {
        "label": "州/省",
        "type": "string",
        "default_gen": "state",
    },
    "SHIP_FROM_POSTAL_CODE": {
        "label": "邮政编码",
        "type": "string",
        "default_gen": "zipcode",
    },
    "SHIP_FROM_COUNTRY_CODE": {
        "label": "国家代码",
        "type": "choice",
        "choices": ["US", "CA", "MX", "DE", "ES", "FR", "IT", "GB", "IN", "AU", "JP"],
        "default_gen": "random_choice",
    },
    "DESTINATION_FULFILLMENT_CENTER_ID": {
        "label": "亚马逊配送中心ID",
        "type": "string",
        "default_gen": "fc_id",
    },
    "LABEL_PREP_TYPE": {
        "label": "标签类型",
        "type": "choice",
        "choices": ["NO_LABEL", "SELLER_LABEL", "AMAZON_LABEL"],
        "default_gen": "random_choice",
    },
    "SHIPMENT_STATUS": {
        "label": "货件状态",
        "type": "choice",
        "choices": {
            "1": "处理中", "2": "已发货", "3": "在途", "4": "已交付",
            "5": "检查中", "6": "接收中", "7": "已完成", "8": "已取消",
            "9": "已删除", "10": "错误",
        },
        "default_gen": "weighted_status",
    },
    "ARE_CASES_REQUIRED": {
        "label": "是否原厂包装",
        "type": "choice",
        "choices": ["True", "False"],
        "default_gen": "random_bool",
    },
    "CONFIRMED_NEED_BY_DATE": {
        "label": "确认需求日期",
        "type": "string",
        "default_gen": "need_by_date",
        "nullable": True,
        "depends_on": "SHIPMENT_STATUS",
    },
    "BOX_CONTENTS_SOURCE": {
        "label": "箱内容来源",
        "type": "choice",
        "choices": ["NONE", "FBA_PACKAGING", "2D_BARCODE"],
        "default_gen": "random_choice",
    },
    "TOTAL_UNITS": {
        "label": "总数量",
        "type": "int",
        "range": [10, 5000],
        "default_gen": "random_int",
    },
    "CURRENCY_CODE": {
        "label": "货币代码",
        "type": "choice",
        "choices": ["USD", "CAD", "EUR", "GBP", "JPY", "INR", "AUD", "MXN"],
        "default_gen": "random_choice",
    },
    "FEE_PER_UNIT": {
        "label": "单价费用",
        "type": "float",
        "range": [0.1, 5.0],
        "default_gen": "random_float",
    },
    "TOTAL_FEE": {
        "label": "总费用",
        "type": "float",
        "default_gen": "computed_total_fee",
        "auto": True,
    },
    "AMAZON_REFERENCE_ID": {
        "label": "预约号",
        "type": "string",
        "default_gen": "reference_id",
        "nullable": True,
    },
    "CREATE_TIME": {
        "label": "创建时间",
        "type": "datetime",
        "default_gen": "create_time",
        "auto": True,
    },
    "UPDATE_TIME": {
        "label": "更新时间",
        "type": "datetime",
        "default_gen": "update_time",
        "auto": True,
        "nullable": True,
    },
}

ITEM_FIELDS = {
    "MARKETPLACE_ID": {
        "label": "市场ID",
        "type": "inherit",
        "inherit_from": "shipment",
        "required": True,
    },
    "SELLER_ID": {
        "label": "卖家ID",
        "type": "inherit",
        "inherit_from": "shipment",
        "required": True,
    },
    "SHIPMENT_ID": {
        "label": "货件ID",
        "type": "inherit",
        "inherit_from": "shipment",
        "required": True,
    },
    "SELLER_SKU": {
        "label": "卖家SKU",
        "type": "string",
        "pattern": "自定义格式",
        "required": True,
        "default_gen": "seller_sku",
    },
    "FNSKU": {
        "label": "FNSKU",
        "type": "string",
        "pattern": "X00 + 7位字母数字",
        "default_gen": "fnsku",
    },
    "QUANTITY_SHIPPED": {
        "label": "发货数量",
        "type": "int",
        "range": [1, 500],
        "default_gen": "quantity_shipped",
    },
    "QUANTITY_RECEIVED": {
        "label": "接收数量",
        "type": "int",
        "default_gen": "quantity_received",
        "auto": True,
    },
    "QUANTITY_IN_CASE": {
        "label": "每箱数量",
        "type": "int",
        "range": [1, 24],
        "default_gen": "random_int_nullable",
        "nullable": True,
    },
    "CREATE_TIME": {
        "label": "创建时间",
        "type": "inherit",
        "inherit_from": "shipment",
        "auto": True,
    },
    "UPDATE_TIME": {
        "label": "更新时间",
        "type": "inherit",
        "inherit_from": "shipment",
        "auto": True,
        "nullable": True,
    },
    "ALL_SIGN_TIME": {
        "label": "全部签收时间",
        "type": "datetime",
        "default_gen": "all_sign_time",
        "auto": True,
        "nullable": True,
    },
}

# 货件状态权重
SHIPMENT_STATUS_WEIGHTS = [5, 15, 10, 10, 3, 5, 30, 10, 8, 4]

# 配置模板保存路径
TEMPLATE_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "templates")


# ============================================================
# 数据生成器
# ============================================================

class InboundDataGenerator:
    """亚马逊入库货件数据生成器"""

    def __init__(self, seed=None):
        self.fake = Faker('zh_CN')
        self.fake_en = Faker('en_US')
        if seed is not None:
            Faker.seed(seed)
            random.seed(seed)

        self._used_shipment_ids = set()
        self._seller_pool = [self._gen_seller_id() for _ in range(20)]

    def _gen_seller_id(self):
        return 'A' + ''.join(random.choices('ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789', k=13))

    def _gen_shipment_id(self):
        while True:
            prefix = random.choice(['FBA15', 'FBA18', 'FBA19', 'FBA20', 'FBA21', 'FBA22', 'FBA23', 'FBA24'])
            suffix = ''.join(random.choices('ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789', k=random.randint(6, 10)))
            sid = prefix + suffix
            if sid not in self._used_shipment_ids:
                self._used_shipment_ids.add(sid)
                return sid

    def _gen_seller_sku(self):
        patterns = [
            lambda: f"SKU-{''.join(random.choices('ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789', k=6))}",
            lambda: f"{''.join(random.choices('ABCDEFGHIJKLMNOPQRSTUVWXYZ', k=3))}-{''.join(random.choices('0123456789', k=5))}",
            lambda: f"PROD-{random.randint(10000, 99999)}",
            lambda: f"AS-{self.fake_en.bothify('??##??##')}",
        ]
        return random.choice(patterns)()

    def _gen_fnsku(self):
        return 'X00' + ''.join(random.choices('ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789', k=7))

    def _gen_fc_id(self):
        prefixes = ['PHX', 'LAX', 'SEA', 'OAK', 'SDF', 'IND', 'CMH', 'AVP', 'ABE', 'BWI',
                    'CLT', 'RDU', 'JAX', 'MCO', 'TPA', 'DFW', 'IAH', 'SAT', 'DEN', 'SLC']
        suffix = ''.join(random.choices('0123456789', k=random.randint(1, 2)))
        return random.choice(prefixes) + suffix

    def _random_date(self, days_back=180):
        end = datetime.now()
        start = end - timedelta(days=days_back)
        delta = int((end - start).total_seconds())
        return start + timedelta(seconds=random.randint(0, delta))

    # ---- 核心方法：根据用户配置生成单条数据 ----

    def generate_field_value(self, field_name, field_config, user_config, context=None):
        """
        根据用户配置生成单个字段的值

        user_config 结构:
            "mode": "auto" | "fixed" | "range" | "null"
            "fixed_value": "xxx"    (mode=fixed 时)
            "range_min": 1          (mode=range 时)
            "range_max": 100        (mode=range 时)
            "choices": ["A","B"]    (mode=range 时，枚举选择)
        """
        mode = user_config.get("mode", "auto")

        # mode=null：不生成，返回 None
        if mode == "null":
            return None

        # mode=fixed：使用固定值
        if mode == "fixed":
            val = user_config.get("fixed_value", "")
            return self._cast_value(val, field_config)

        # mode=range：范围内随机
        if mode == "range":
            ftype = field_config.get("type", "string")
            if ftype == "int":
                lo = user_config.get("range_min", field_config.get("range", [0, 100])[0])
                hi = user_config.get("range_max", field_config.get("range", [0, 100])[1])
                return random.randint(int(lo), int(hi))
            elif ftype == "float":
                lo = user_config.get("range_min", field_config.get("range", [0.0, 1.0])[0])
                hi = user_config.get("range_max", field_config.get("range", [0.0, 1.0])[1])
                return round(random.uniform(float(lo), float(hi)), 2)
            elif ftype == "choice":
                choices = user_config.get("choices", field_config.get("choices", []))
                if isinstance(choices, dict):
                    choices = list(choices.keys())
                return random.choice(choices) if choices else None
            elif ftype == "string":
                choices = user_config.get("choices", [])
                if choices:
                    return random.choice(choices)
                lo = user_config.get("range_min", 5)
                hi = user_config.get("range_max", 10)
                return ''.join(random.choices('ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789', k=random.randint(int(lo), int(hi))))

        # mode=auto：自动生成
        gen_type = field_config.get("default_gen", "")

        if gen_type == "random_choice":
            choices = field_config.get("choices", [])
            if isinstance(choices, dict):
                choices = list(choices.keys())
            return random.choice(choices) if choices else None

        elif gen_type == "seller_id":
            return random.choice(self._seller_pool)

        elif gen_type == "shipment_id":
            return self._gen_shipment_id()

        elif gen_type == "shipment_name":
            return f"Shipment-{self.fake_en.bothify('##??####')}-{random.randint(1, 999)}"

        elif gen_type == "company":
            return self.fake.company()

        elif gen_type == "street_address":
            return self.fake.street_address()

        elif gen_type == "secondary_address":
            return self.fake_en.secondary_address() if random.random() > 0.4 else None

        elif gen_type == "district":
            return self.fake.city_suffix() if random.random() > 0.5 else None

        elif gen_type == "city":
            return self.fake.city()

        elif gen_type == "state":
            return self.fake_en.state_abbr() if random.random() > 0.3 else self.fake_en.state()

        elif gen_type == "zipcode":
            return self.fake_en.zipcode()

        elif gen_type == "fc_id":
            return self._gen_fc_id()

        elif gen_type == "weighted_status":
            return random.choices(
                list(range(1, 11)),
                weights=SHIPMENT_STATUS_WEIGHTS,
                k=1
            )[0]

        elif gen_type == "random_bool":
            return random.choice([True, False])

        elif gen_type == "random_int":
            r = field_config.get("range", [1, 100])
            return random.randint(r[0], r[1])

        elif gen_type == "random_int_nullable":
            r = field_config.get("range", [1, 24])
            return random.randint(r[0], r[1]) if random.random() > 0.3 else None

        elif gen_type == "random_float":
            r = field_config.get("range", [0.1, 5.0])
            return round(random.uniform(r[0], r[1]), 2)

        elif gen_type == "need_by_date":
            status = context.get("SHIPMENT_STATUS", 1) if context else 1
            if status in [2, 3, 4, 5, 6, 7]:
                base = datetime.now()
                return (base + timedelta(days=random.randint(7, 30))).strftime('%Y-%m-%d')
            return None

        elif gen_type == "computed_total_fee":
            total_units = context.get("TOTAL_UNITS", 0) if context else 0
            fee_per_unit = context.get("FEE_PER_UNIT", 0) if context else 0
            if total_units and fee_per_unit:
                return round(fee_per_unit * total_units, 2)
            return round(random.uniform(10, 5000), 2)

        elif gen_type == "reference_id":
            status = context.get("SHIPMENT_STATUS", 1) if context else 1
            if status >= 2:
                return ''.join(random.choices('0123456789', k=10))
            return None

        elif gen_type == "create_time":
            return self._random_date().strftime('%Y-%m-%d %H:%M:%S')

        elif gen_type == "update_time":
            status = context.get("SHIPMENT_STATUS", 1) if context else 1
            create = context.get("CREATE_TIME", "") if context else ""
            if status >= 2 and create:
                ct = datetime.strptime(create, '%Y-%m-%d %H:%M:%S')
                return (ct + timedelta(hours=random.randint(1, 720))).strftime('%Y-%m-%d %H:%M:%S')
            return None

        elif gen_type == "seller_sku":
            return self._gen_seller_sku()

        elif gen_type == "fnsku":
            return self._gen_fnsku()

        elif gen_type == "quantity_shipped":
            status = context.get("SHIPMENT_STATUS", 1) if context else 1
            if status == 8:
                return 0
            return random.randint(1, 500)

        elif gen_type == "quantity_received":
            status = context.get("SHIPMENT_STATUS", 1) if context else 1
            shipped = context.get("QUANTITY_SHIPPED", 0) if context else 0
            if status in [1, 2, 3]:
                return 0
            elif status in [4, 5, 6]:
                return random.randint(int(shipped * 0.5), shipped)
            elif status == 7:
                return random.randint(int(shipped * 0.9), shipped)
            elif status == 8:
                return 0
            return random.randint(0, shipped)

        elif gen_type == "all_sign_time":
            status = context.get("SHIPMENT_STATUS", 1) if context else 1
            shipped = context.get("QUANTITY_SHIPPED", 0) if context else 0
            received = context.get("QUANTITY_RECEIVED", 0) if context else 0
            if status == 7 and received >= shipped * 0.95:
                create = context.get("CREATE_TIME", "") if context else ""
                if create:
                    ct = datetime.strptime(create, '%Y-%m-%d %H:%M:%S')
                    return (ct + timedelta(days=random.randint(3, 30))).strftime('%Y-%m-%d %H:%M:%S')
            return None

        # 兜底
        return None

    def _cast_value(self, val, field_config):
        """将字符串值转换为字段对应类型"""
        ftype = field_config.get("type", "string")
        if ftype == "int":
            return int(val)
        elif ftype == "float":
            return float(val)
        elif ftype == "choice":
            choices = field_config.get("choices", {})
            if isinstance(choices, dict):
                if val in choices:
                    return int(val) if val.isdigit() else val
            return val
        elif ftype == "datetime":
            return val
        return val

    def generate_shipment(self, field_config):
        """
        根据用户配置生成一条货件数据

        Args:
            field_config: dict, {field_name: {mode, fixed_value, range_min, range_max, choices}}

        Returns:
            dict: 一条货件数据
        """
        row = {}
        context = {}

        # 分两轮：第一轮生成普通字段，第二轮生成依赖其他字段的自动字段
        auto_fields = []
        for fname, fdef in SHIPMENT_FIELDS.items():
            if fdef.get("auto") or fdef.get("type") == "inherit":
                auto_fields.append(fname)
                continue
            cfg = field_config.get(fname, {"mode": "auto"})
            val = self.generate_field_value(fname, fdef, cfg, context)
            row[fname] = val
            context[fname] = val

        # 第二轮：生成自动/依赖字段
        for fname in auto_fields:
            fdef = SHIPMENT_FIELDS[fname]
            cfg = field_config.get(fname, {"mode": "auto"})
            val = self.generate_field_value(fname, fdef, cfg, context)
            row[fname] = val
            context[fname] = val

        return row

    def generate_items_for_shipment(self, shipment, item_field_config, min_items=1, max_items=5):
        """为一个货件生成明细数据"""
        num_items = random.randint(min_items, max_items)
        items = []
        used_skus = set()

        for _ in range(num_items):
            row = {}
            context = dict(shipment)

            for fname, fdef in ITEM_FIELDS.items():
                # 继承字段
                if fdef.get("type") == "inherit":
                    row[fname] = shipment.get(fname)
                    context[fname] = shipment.get(fname)
                    continue

                cfg = item_field_config.get(fname, {"mode": "auto"})

                # SKU 需要去重
                if fname == "SELLER_SKU":
                    while True:
                        val = self.generate_field_value(fname, fdef, cfg, context)
                        if val not in used_skus:
                            used_skus.add(val)
                            break
                    row[fname] = val
                    context[fname] = val
                else:
                    val = self.generate_field_value(fname, fdef, cfg, context)
                    row[fname] = val
                    context[fname] = val

            items.append(row)

        return items

    def generate_batch(self, count, shipment_config, item_config, min_items=1, max_items=5):
        """批量生成数据"""
        shipments = []
        items = []

        for i in range(count):
            s = self.generate_shipment(shipment_config)
            s_items = self.generate_items_for_shipment(s, item_config, min_items, max_items)
            shipments.append(s)
            items.extend(s_items)

            if (i + 1) % 100 == 0:
                print(f"  已生成 {i + 1}/{count} 个货件...")

        return shipments, items


# ============================================================
# 交互式配置
# ============================================================

def print_separator(char="=", length=60):
    print(char * length)


def print_title(title):
    print_separator()
    print(f"  {title}")
    print_separator()


def input_with_default(prompt, default=""):
    val = input(f"{prompt} [{default}]: ").strip()
    return val if val else default


def yes_no(prompt, default="n"):
    val = input(f"{prompt} (y/n) [{default}]: ").strip().lower()
    return val in ("y", "yes") if val else default in ("y", "yes")


def configure_field_interactive(field_name, field_def):
    """
    交互式配置单个字段

    Returns:
        dict: {mode, fixed_value, range_min, range_max, choices}
    """
    label = field_def.get("label", field_name)
    ftype = field_def.get("type", "string")
    nullable = field_def.get("nullable", False)
    required = field_def.get("required", False)

    print(f"\n  📌 {field_name} ({label}) — 类型: {ftype}")

    # 显示可选项
    if ftype == "choice":
        choices = field_def.get("choices", {})
        if isinstance(choices, dict):
            print(f"     可选值:")
            for k, v in choices.items():
                print(f"       {k}: {v}")
        elif isinstance(choices, list):
            print(f"     可选值: {', '.join(str(c) for c in choices)}")

    if ftype in ("int", "float"):
        rng = field_def.get("range", [])
        if rng:
            print(f"     默认范围: {rng[0]} ~ {rng[1]}")

    pattern = field_def.get("pattern", "")
    if pattern:
        print(f"     格式: {pattern}")

    # 选择模式
    if nullable and not required:
        print(f"  生成模式:")
        print(f"    1. 自动生成（随机）")
        print(f"    2. 固定值（每条数据都一样）")
        print(f"    3. 自定义范围/选项")
        print(f"    4. 留空（NULL）")
        mode_choice = input_with_default("  请选择", "1")
    else:
        print(f"  生成模式:")
        print(f"    1. 自动生成（随机）")
        print(f"    2. 固定值（每条数据都一样）")
        print(f"    3. 自定义范围/选项")
        mode_choice = input_with_default("  请选择", "1")

    config = {}

    if mode_choice == "4" and nullable:
        config["mode"] = "null"
        print(f"  ✅ {field_name} → 留空(NULL)")
        return config

    elif mode_choice == "2":
        # 固定值
        config["mode"] = "fixed"
        if ftype == "choice":
            choices = field_def.get("choices", {})
            if isinstance(choices, dict):
                val = input_with_default(f"  请输入值 ({'/'.join(choices.keys())})",
                                         list(choices.keys())[0] if choices else "")
            else:
                val = input_with_default(f"  请输入值 ({'/'.join(str(c) for c in choices)})",
                                         str(choices[0]) if choices else "")
        else:
            val = input_with_default("  请输入固定值", "")
        config["fixed_value"] = val
        print(f"  ✅ {field_name} → 固定值: {val}")

    elif mode_choice == "3":
        # 自定义范围/选项
        config["mode"] = "range"
        if ftype == "int":
            lo = input_with_default("  最小值", str(field_def.get("range", [0, 100])[0]))
            hi = input_with_default("  最大值", str(field_def.get("range", [0, 100])[1]))
            config["range_min"] = int(lo)
            config["range_max"] = int(hi)
            print(f"  ✅ {field_name} → 范围: {lo} ~ {hi}")
        elif ftype == "float":
            lo = input_with_default("  最小值", str(field_def.get("range", [0.0, 1.0])[0]))
            hi = input_with_default("  最大值", str(field_def.get("range", [0.0, 1.0])[1]))
            config["range_min"] = float(lo)
            config["range_max"] = float(hi)
            print(f"  ✅ {field_name} → 范围: {lo} ~ {hi}")
        elif ftype == "choice":
            choices = field_def.get("choices", {})
            if isinstance(choices, dict):
                print(f"  可选值: {', '.join(f'{k}({v})' for k, v in choices.items())}")
                selected = input_with_default("  输入要选的值(多个用逗号分隔)", ",".join(list(choices.keys())[:3]))
                config["choices"] = [c.strip() for c in selected.split(",")]
            else:
                selected = input_with_default(f"  输入要选的值(多个用逗号分隔)", ",".join(str(c) for c in choices[:3]))
                config["choices"] = [c.strip() for c in selected.split(",")]
            print(f"  ✅ {field_name} → 从以下选项中随机: {config['choices']}")
        elif ftype == "string":
            print(f"  字符串自定义模式:")
            print(f"    a. 从几个固定值中随机选")
            print(f"    b. 指定长度范围随机生成")
            sub = input_with_default("  请选择", "a")
            if sub == "a":
                vals = input("  输入可选值(多个用逗号分隔): ").strip()
                config["choices"] = [v.strip() for v in vals.split(",")]
                print(f"  ✅ {field_name} → 从以下值中随机: {config['choices']}")
            else:
                lo = input_with_default("  最短长度", "5")
                hi = input_with_default("  最长长度", "10")
                config["range_min"] = int(lo)
                config["range_max"] = int(hi)
                print(f"  ✅ {field_name} → 长度范围: {lo} ~ {hi}")
        else:
            val = input_with_default("  请输入值", "")
            config["fixed_value"] = val
            config["mode"] = "fixed"

    else:
        # 自动生成
        config["mode"] = "auto"
        print(f"  ✅ {field_name} → 自动生成")

    return config


def interactive_configure_table(table_name, fields_def):
    """
    交互式配置一张表的字段

    Returns:
        dict: {field_name: {mode, fixed_value, ...}}
    """
    print_title(f"配置表: {table_name}")

    # 第一步：选择要生成哪些字段
    print("\n请选择要生成数据的字段（不需要的会留空 NULL）：\n")

    field_list = list(fields_def.keys())
    selected_fields = []

    for i, fname in enumerate(field_list, 1):
        fdef = fields_def[fname]
        label = fdef.get("label", fname)
        required = fdef.get("required", False)
        is_auto = fdef.get("auto", False)
        is_inherit = fdef.get("type") == "inherit"

        if required:
            print(f"  {i:2d}. ☑ {fname:40s} ({label}) — 必填")
            selected_fields.append(fname)
        elif is_inherit:
            print(f"  {i:2d}. ☑ {fname:40s} ({label}) — 继承自主表")
            selected_fields.append(fname)
        elif is_auto:
            print(f"  {i:2d}. ☑ {fname:40s} ({label}) — 自动计算")
            selected_fields.append(fname)
        else:
            default = "y" if i <= 10 else "n"  # 默认前10个字段选中
            val = input(f"  {i:2d}.   {fname:40s} ({label}) — 是否生成? (y/n) [{default}]: ").strip().lower()
            if val != "n" and (val == "y" or default == "y"):
                selected_fields.append(fname)

    if not selected_fields:
        print("\n⚠️  没有选择任何字段，将使用全部字段。")
        selected_fields = field_list

    print(f"\n已选择 {len(selected_fields)} 个字段: {', '.join(selected_fields)}")

    # 第二步：逐个配置字段
    print_title(f"详细配置: {table_name}")

    field_config = {}
    skip_auto = yes_no("\n自动计算的字段(费用、时间等)用默认方式生成？", "y")

    for fname in selected_fields:
        fdef = fields_def[fname]

        # 必填/继承/自动字段可以跳过详细配置
        if fdef.get("required") and fdef.get("type") != "choice":
            if yes_no(f"  {fname} ({fdef.get('label', '')}) 用默认自动生成?", "y"):
                field_config[fname] = {"mode": "auto"}
                continue

        if fdef.get("auto") and skip_auto:
            field_config[fname] = {"mode": "auto"}
            continue

        if fdef.get("type") == "inherit":
            field_config[fname] = {"mode": "auto"}
            continue

        config = configure_field_interactive(fname, fdef)
        field_config[fname] = config

    # 未选中的字段设为 null
    for fname in field_list:
        if fname not in field_config:
            fdef = fields_def[fname]
            if fdef.get("required") or fdef.get("auto") or fdef.get("type") == "inherit":
                field_config[fname] = {"mode": "auto"}
            else:
                field_config[fname] = {"mode": "null"}

    return field_config


def interactive_main():
    """交互式主流程"""
    print_separator()
    print("  🚀 亚马逊 FBA 入库货件 —— 交互式数据生成器")
    print_separator()

    # 1. 基本参数
    print("\n【第一步：基本参数】\n")
    count = int(input_with_default("  生成货件数量", "50"))
    min_items = int(input_with_default("  每个货件最少 SKU 数", "1"))
    max_items = int(input_with_default("  每个货件最多 SKU 数", "5"))

    # 2. 是否使用已保存的模板
    template = None
    if yes_no("\n是否加载已保存的配置模板？", "n"):
        template_name = input_with_default("  模板名称", "default")
        template = load_template(template_name)
        if template:
            print(f"  ✅ 已加载模板: {template_name}")
        else:
            print(f"  ⚠️  模板不存在，将进入手动配置")

    # 3. 配置货件主表字段
    if template and "shipment" in template:
        shipment_config = template["shipment"]
        print(f"\n  使用模板中的货件主表配置")
    else:
        shipment_config = interactive_configure_table(
            "mws_fi_data_inbound_shipment", SHIPMENT_FIELDS
        )

    # 4. 配置明细表字段
    if template and "item" in template:
        item_config = template["item"]
        print(f"\n  使用模板中的明细表配置")
    else:
        item_config = interactive_configure_table(
            "mws_fi_data_inbound_shipment_item", ITEM_FIELDS
        )

    # 5. 保存模板
    if yes_no("\n是否保存当前配置为模板？（下次可复用）", "y"):
        template_name = input_with_default("  模板名称", "default")
        save_template(template_name, shipment_config, item_config)

    # 6. 输出方式
    print("\n【输出方式】\n")
    print("  1. CSV 文件")
    print("  2. SQL 文件")
    print("  3. 直连 MySQL 写入")
    output_choice = input_with_default("  请选择", "1")

    if output_choice == "1":
        output_mode = "csv"
        output_dir = input_with_default("  输出目录", "./output")
    elif output_choice == "2":
        output_mode = "sql"
        output_dir = input_with_default("  SQL 文件路径", "./insert_data.sql")
    else:
        output_mode = "db"
        db_host = input_with_default("  MySQL 主机", "127.0.0.1")
        db_port = int(input_with_default("  MySQL 端口", "3306"))
        db_user = input_with_default("  MySQL 用户", "root")
        db_pass = input("  MySQL 密码: ").strip()
        db_name = input_with_default("  数据库名", "test")

    # 7. 开始生成
    print_title("开始生成数据")

    generator = InboundDataGenerator()
    shipments, items = generator.generate_batch(
        count=count,
        shipment_config=shipment_config,
        item_config=item_config,
        min_items=min_items,
        max_items=max_items,
    )

    # 统计
    print(f"\n📊 数据统计:")
    print(f"  货件总数: {len(shipments)}")
    print(f"  明细总数: {len(items)}")
    if shipments:
        print(f"  平均每货件 SKU: {len(items) / len(shipments):.1f}")

    # 输出
    if output_mode == "csv":
        export_to_csv(shipments, items, output_dir)
    elif output_mode == "sql":
        export_to_sql(shipments, items, output_dir)
    elif output_mode == "db":
        insert_to_mysql(shipments, items, db_host, db_port, db_user, db_pass, db_name)

    print("\n🎉 完成！")


# ============================================================
# 模板管理
# ============================================================

def save_template(name, shipment_config, item_config):
    """保存配置模板"""
    os.makedirs(TEMPLATE_DIR, exist_ok=True)
    filepath = os.path.join(TEMPLATE_DIR, f"{name}.json")
    data = {
        "name": name,
        "created_at": datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
        "shipment": shipment_config,
        "item": item_config,
    }
    with open(filepath, 'w', encoding='utf-8') as f:
        json.dump(data, f, ensure_ascii=False, indent=2)
    print(f"  ✅ 模板已保存: {filepath}")


def load_template(name):
    """加载配置模板"""
    filepath = os.path.join(TEMPLATE_DIR, f"{name}.json")
    if not os.path.exists(filepath):
        return None
    with open(filepath, 'r', encoding='utf-8') as f:
        return json.load(f)


def list_templates():
    """列出所有模板"""
    if not os.path.exists(TEMPLATE_DIR):
        return []
    templates = []
    for f in os.listdir(TEMPLATE_DIR):
        if f.endswith('.json'):
            filepath = os.path.join(TEMPLATE_DIR, f)
            with open(filepath, 'r', encoding='utf-8') as fp:
                data = json.load(fp)
            templates.append({
                "name": data.get("name", f[:-5]),
                "created_at": data.get("created_at", "未知"),
                "filepath": filepath,
            })
    return templates


# ============================================================
# 输出方式
# ============================================================

def export_to_csv(shipments, items, output_dir='.'):
    os.makedirs(output_dir, exist_ok=True)

    # 过滤掉全为 None 的列
    def filter_columns(data):
        if not data:
            return data
        all_keys = list(data[0].keys())
        keep_keys = []
        for k in all_keys:
            has_value = any(row.get(k) is not None for row in data)
            if has_value:
                keep_keys.append(k)
        return [{k: row.get(k) for k in keep_keys} for row in data]

    shipments = filter_columns(shipments)
    items = filter_columns(items)

    shipment_file = os.path.join(output_dir, 'mws_fi_data_inbound_shipment.csv')
    if shipments:
        keys = shipments[0].keys()
        with open(shipment_file, 'w', newline='', encoding='utf-8-sig') as f:
            writer = csv.DictWriter(f, fieldnames=keys)
            writer.writeheader()
            writer.writerows(shipments)
        print(f"✅ 货件主表已导出: {shipment_file} ({len(shipments)} 条)")

    item_file = os.path.join(output_dir, 'mws_fi_data_inbound_shipment_item.csv')
    if items:
        keys = items[0].keys()
        with open(item_file, 'w', newline='', encoding='utf-8-sig') as f:
            writer = csv.DictWriter(f, fieldnames=keys)
            writer.writeheader()
            writer.writerows(items)
        print(f"✅ 货件明细已导出: {item_file} ({len(items)} 条)")


def export_to_sql(shipments, items, sql_file='insert_data.sql'):
    dir_path = os.path.dirname(sql_file)
    if dir_path:
        os.makedirs(dir_path, exist_ok=True)

    with open(sql_file, 'w', encoding='utf-8') as f:
        f.write("-- 亚马逊 FBA 入库货件测试数据\n")
        f.write(f"-- 生成时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
        f.write(f"-- 货件: {len(shipments)} 条, 明细: {len(items)} 条\n\n")

        if shipments:
            f.write("-- 货件主表\n")
            for s in shipments:
                # 只插入非 None 的字段
                cols_vals = [(k, v) for k, v in s.items() if v is not None]
                if cols_vals:
                    cols = ', '.join(k for k, _ in cols_vals)
                    vals = ', '.join(_sql_value(v) for _, v in cols_vals)
                    f.write(f"INSERT INTO `mws_fi_data_inbound_shipment` ({cols}) VALUES ({vals});\n")

        if items:
            f.write("\n-- 货件明细\n")
            for item in items:
                cols_vals = [(k, v) for k, v in item.items() if v is not None]
                if cols_vals:
                    cols = ', '.join(k for k, _ in cols_vals)
                    vals = ', '.join(_sql_value(v) for _, v in cols_vals)
                    f.write(f"INSERT INTO `mws_fi_data_inbound_shipment_item` ({cols}) VALUES ({vals});\n")

    print(f"✅ SQL 文件已导出: {sql_file}")


def _sql_value(value):
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
    try:
        import pymysql
    except ImportError:
        print("❌ 缺少 pymysql，请执行: pip install pymysql")
        sys.exit(1)

    conn = pymysql.connect(
        host=host, port=port, user=user,
        password=password, database=database, charset='utf8mb4',
    )

    try:
        cursor = conn.cursor()

        # 动态构建 INSERT
        if shipments:
            print(f"\n📦 插入货件主表...")
            for i, s in enumerate(shipments):
                cols_vals = [(k, v) for k, v in s.items() if v is not None]
                if cols_vals:
                    cols = ', '.join(k for k, _ in cols_vals)
                    placeholders = ', '.join(['%s'] * len(cols_vals))
                    vals = [v for _, v in cols_vals]
                    sql = f"INSERT INTO `mws_fi_data_inbound_shipment` ({cols}) VALUES ({placeholders})"
                    cursor.execute(sql, vals)
                if (i + 1) % batch_size == 0:
                    conn.commit()
                    print(f"  已插入 {i + 1}/{len(shipments)} 条")
            conn.commit()
            print(f"  货件主表完成: {len(shipments)} 条")

        if items:
            print(f"\n📋 插入货件明细...")
            for i, item in enumerate(items):
                cols_vals = [(k, v) for k, v in item.items() if v is not None]
                if cols_vals:
                    cols = ', '.join(k for k, _ in cols_vals)
                    placeholders = ', '.join(['%s'] * len(cols_vals))
                    vals = [v for _, v in cols_vals]
                    sql = f"INSERT INTO `mws_fi_data_inbound_shipment_item` ({cols}) VALUES ({placeholders})"
                    cursor.execute(sql, vals)
                if (i + 1) % batch_size == 0:
                    conn.commit()
                    print(f"  已插入 {i + 1}/{len(items)} 条")
            conn.commit()
            print(f"  明细表完成: {len(items)} 条")

        print(f"\n✅ 数据写入完成！")

    except Exception as e:
        conn.rollback()
        print(f"❌ 写入失败: {e}")
        raise
    finally:
        cursor.close()
        conn.close()


# ============================================================
# 命令行模式（非交互式，向后兼容）
# ============================================================

def cli_main(args):
    """命令行模式：快速生成，全部字段自动生成"""
    print_separator()
    print("  🚀 亚马逊 FBA 入库货件数据生成器（快速模式）")
    print_separator()
    print(f"  货件数量: {args.count}")
    print(f"  每个货件 SKU 数: {args.min_items} ~ {args.max_items}")
    print(f"  输出方式: {args.output.upper()}")

    # 全自动配置
    shipment_config = {fname: {"mode": "auto"} for fname in SHIPMENT_FIELDS}
    item_config = {fname: {"mode": "auto"} for fname in ITEM_FIELDS}

    generator = InboundDataGenerator(seed=args.seed)
    shipments, items = generator.generate_batch(
        count=args.count,
        shipment_config=shipment_config,
        item_config=item_config,
        min_items=args.min_items,
        max_items=args.max_items,
    )

    # 统计
    status_map = {
        1: "处理中", 2: "已发货", 3: "在途", 4: "已交付", 5: "检查中",
        6: "接收中", 7: "已完成", 8: "已取消", 9: "已删除", 10: "错误",
    }
    print(f"\n📊 货件状态分布:")
    status_count = {}
    for s in shipments:
        status = s.get("SHIPMENT_STATUS", 0)
        key = f"{status}-{status_map.get(status, '未知')}"
        status_count[key] = status_count.get(key, 0) + 1
    for k, v in sorted(status_count.items(), key=lambda x: int(x[0].split('-')[0])):
        bar = '█' * (v * 30 // max(args.count, 1))
        print(f"  {k:12s}: {v:4d}  {bar}")

    print(f"\n📊 数据统计:")
    print(f"  货件总数: {len(shipments)}")
    print(f"  明细总数: {len(items)}")

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


# ============================================================
# 入口
# ============================================================

def main():
    parser = argparse.ArgumentParser(
        description='亚马逊 FBA 入库货件数据生成脚本',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
示例:
  # 交互式模式（推荐，可自定义字段）
  python generate_inbound_data.py

  # 快速模式（全部字段自动生成）
  python generate_inbound_data.py --count 50 --output csv

  # 使用已保存的模板
  python generate_inbound_data.py --template my_config
        """
    )

    # 交互式参数
    parser.add_argument('--interactive', '-i', action='store_true', default=True,
                        help='交互式模式（默认开启）')
    parser.add_argument('--template', type=str, default=None,
                        help='使用已保存的配置模板名称')
    parser.add_argument('--list-templates', action='store_true',
                        help='列出所有已保存的模板')

    # 快速模式参数
    parser.add_argument('--count', type=int, default=50, help='货件数量（默认 50）')
    parser.add_argument('--min-items', type=int, default=1, help='每个货件最少 SKU 数')
    parser.add_argument('--max-items', type=int, default=5, help='每个货件最多 SKU 数')
    parser.add_argument('--seed', type=int, default=None, help='随机种子')
    parser.add_argument('--output', choices=['csv', 'db', 'sql'], default=None,
                        help='输出方式（指定后跳过交互式，直接快速生成）')
    parser.add_argument('--output-dir', type=str, default='./output')
    parser.add_argument('--sql-file', type=str, default='./insert_data.sql')
    parser.add_argument('--host', type=str, default='127.0.0.1')
    parser.add_argument('--port', type=int, default=3306)
    parser.add_argument('--user', type=str, default='root')
    parser.add_argument('--password', type=str, default='')
    parser.add_argument('--database', type=str, default='test')

    args = parser.parse_args()

    # 列出模板
    if args.list_templates:
        templates = list_templates()
        if templates:
            print("已保存的配置模板:")
            for t in templates:
                print(f"  📄 {t['name']}  (创建于 {t['created_at']})")
        else:
            print("暂无已保存的模板。")
        return

    # 指定了 --output 则走快速模式
    if args.output:
        cli_main(args)
        return

    # 否则走交互式
    if args.template:
        template = load_template(args.template)
        if template:
            print(f"✅ 已加载模板: {args.template}")
            # 用模板快速生成
            generator = InboundDataGenerator(seed=args.seed)
            shipments, items = generator.generate_batch(
                count=args.count,
                shipment_config=template.get("shipment", {}),
                item_config=template.get("item", {}),
                min_items=args.min_items,
                max_items=args.max_items,
            )
            print(f"📊 货件: {len(shipments)}, 明细: {len(items)}")
            export_to_csv(shipments, items, args.output_dir)
            print("\n🎉 完成！")
        else:
            print(f"⚠️  模板 '{args.template}' 不存在，进入交互式配置")
            interactive_main()
    else:
        interactive_main()


if __name__ == '__main__':
    main()
