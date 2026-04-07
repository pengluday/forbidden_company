# 智联采集程序（直接抓取版）

## 1) 日常使用
直接从智联招聘固定搜索结果里抓取 100 条最近招聘列表：

```bash
cd /Users/jack/IdeaProjects/forbidden_company
bash scripts/run_zhilian_collection.sh
```

结果会写入：
- 当日文件：`data/source-intake-zhilian-YYYY-MM-DD.csv`
- SQLite：`data/forbidden_company.db`
- 合并表：`data/source-intake-round1-jobsites.csv`

## 2) 直接调用脚本

```bash
python3 -m collectors.collect_zhilian \
  --output-csv data/source-intake-zhilian-$(date +%F).csv \
  --merge-csv data/source-intake-round1-jobsites.csv \
  --db data/forbidden_company.db \
  --collector manual \
  --limit 100 \
  --seed-url "https://www.zhaopin.com/sou/jl489/kw00PG0DASG57EAJGB/p1?cs=6"
```

## 3) 定时采集（macOS cron）
每天 09:10 执行一次：

```bash
crontab -e
```

加入这一行：

```bash
10 9 * * * cd /Users/jack/IdeaProjects/forbidden_company && bash scripts/run_zhilian_collection.sh >> /Users/jack/IdeaProjects/forbidden_company/data/zhilian-cron.log 2>&1
```

## 4) 字段说明
脚本输出字段与 `source-intake-template.csv` 对齐，默认：
- `source_type=jobsite`
- `source_platform=智联招聘`
- `verification_status=pending`
- `boycott_recommended=false`

## 5) 去重规则
- 同一公司 + 同一来源平台只保留一条采集记录
- 同一个职位 URL 也会去重
- 后台手工录入也走同样的去重逻辑

## 6) 注意事项
- 智联页面会先解析 `__INITIAL_STATE__`，若页面结构变化，脚本会直接报错，方便你在后台观察失败原因。
- 采集结果必须做人工复核并补截图后再提升核验状态。
