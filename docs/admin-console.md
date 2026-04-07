# 核验后台最小页（无命令行采集）

## 启动

```bash
cd /Users/jack/IdeaProjects/forbidden_company
python3 -m backend.admin_server --host 127.0.0.1 --port 8787
```

打开：
- [http://127.0.0.1:8787/admin/](http://127.0.0.1:8787/admin/)

## 页面能力
- 智联一键采集：直接抓取智联招聘的原始列表，使用固定查询「35岁以下 + 10000人以上」，写入 SQLite 并自动按公司去重
- 新增采集线索：写入 `collected_evidence`
- 采集记录核验：把记录提升到 `verified_evidence`，并同步更新采集表状态
- 公司产品映射：先写入 `pending_product_submissions`，审核通过后进入 `company_products`
- 导出展示数据：重建 `data/companies.json`

## 建议日常流程
1. 点击“抓取智联最新100条列表”
2. 在列表中核验为 `partial/verified`
3. 填写公司产品映射
4. 点击“导出 JSON”刷新前台数据
