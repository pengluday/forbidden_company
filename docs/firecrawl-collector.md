# Firecrawl 采集器

Firecrawl 适合做统一抓取层，先尝试抓职位页正文，再把结果落到 SQLite。

## 使用前提

你需要提供：
- `FIRECRAWL_API_KEY`
- 一批职位 URL

## 单独运行

```bash
cd /Users/jack/IdeaProjects/forbidden_company
export FIRECRAWL_API_KEY="your-key"
python3 -m collectors.collect_firecrawl \
  --input-urls data/firecrawl-urls.txt \
  --output-csv data/source-intake-firecrawl-$(date +%F).csv \
  --merge-csv data/source-intake-round1-jobsites.csv \
  --db data/forbidden_company.db \
  --source-platform 猎聘 \
  --skip-no-evidence
```

## 后台运行

启动后台后，打开 `admin/` 页面，在 “Firecrawl 单 URL 验证” 里粘贴 URL 即可。

## 适用范围

- 猎聘
- 智联招聘

实际能否抓到正文，取决于站点反爬强度和 Firecrawl 当前的渲染能力。
建议先用少量 URL 验证，再决定是否替换站点专用采集器。

## 本次实测

2026-04-06 的样例验证结果：
- 猎聘 job detail 页面可抓到正文
- 智联 job detail 页面可抓到正文

因此目前结论是：
- Firecrawl 可作为统一抓取层先覆盖猎聘、智联
