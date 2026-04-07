# SQLite 数据流程（采集 -> 核验 -> 展示）

## 目标
把数据主存储从 CSV 升级到 SQLite，降低误改风险，并跑通闭环。

## 表结构
- `collected_evidence`：采集原始表（所有线索先入这张表）
- `verified_evidence`：核验通过表（只放人工核验后的记录）
- `company_products`：公司与产品映射（补齐“公司->产品”闭环）
- `pending_product_submissions`：产品待审核队列（用户/后台先提交到这里）

## 一键跑通

```bash
cd /Users/jack/IdeaProjects/forbidden_company
bash scripts/run_data_pipeline.sh
```

## 日常命令
1. 初始化数据库

```bash
python3 -m jobs.init_db --db data/forbidden_company.db --schema db/schema.sql
```

2. 导入采集 CSV 到采集表

```bash
python3 -m jobs.import_csv_to_db --db data/forbidden_company.db --csv data/source-intake-round1-jobsites.csv
```

3. 人工核验并提升到核验表

```bash
python3 -m jobs.verify_records \
  --db data/forbidden_company.db \
  --record-ids job-20260406-001,job-20260406-002 \
  --status partial \
  --risk-level medium \
  --boycott \
  --verifier jack \
  --note "已人工复核职位页面原文与截图"
```

4. 维护公司-产品映射

```bash
python3 scripts/upsert_company_product.py \
  --db data/forbidden_company.db \
  --company "永安财产保险股份有限公司" \
  --product "永安车险" \
  --category "保险" \
  --confidence partial \
  --note "官网与公开资料整理"
```

5. 导出前端 JSON

```bash
python3 -m backend.export_companies_json --db data/forbidden_company.db --output data/companies.json --include-pending
```

## 前台同步
- 前台页面优先从后台 `GET /api/public/companies` 拉取最新数据。
- 如果后台暂时不可用，才回退读取 `data/companies.json`。
- 前台会每 30 秒轮询一次最新数据，保证状态同步。

## 建议规则
- `collected_evidence` 不删除历史，保留审计链。
- `verified_evidence` 仅人工核验后写入。
- `company_products` 允许先 `unverified`，后续升级 `partial/verified`。
- `pending_product_submissions` 只放待审核产品，审核通过后再转入 `company_products`。
