# 小红书采集器

这个采集器用于把小红书帖子和评论片段落到现有的 `collected_evidence` 表里。

## 适用场景
- 你已经有一批小红书帖子 URL
- 你想把帖子正文和页面里能抓到的评论片段一起归档
- 你希望后续仍然走同一套 SQLite / CSV / 前台导出链路

## 使用方式

```bash
cd /Users/jack/IdeaProjects/forbidden_company
bash scripts/run_xiaohongshu_collection.sh
```

默认读取：
- `data/xiaohongshu-urls.txt`

输出到：
- `data/source-intake-xiaohongshu-YYYY-MM-DD.csv`
- `data/forbidden_company.db`

## 采集策略
- 先尝试 Firecrawl 渲染抓取
- 如果没有 `FIRECRAWL_API_KEY`，就回退到普通网页抓取
- 帖子正文会作为一条证据记录
- 评论会优先走小红书公开评论接口
- 如果接口返回 `无登录信息`，说明当前链接没有可用登录态，评论只能等 cookie 再抓
- 如果接口返回账号异常，会自动回退到浏览器渲染抓取，优先复用本机 Chrome 里的登录态
- `comment_limit=0` 表示抓取全部可见评论；后台单条刷新现在默认用这个模式
- 页面里能识别出来的评论片段只作为兜底，不再作为主要来源

## 注意事项
- 小红书页面结构变化很快，评论抽取是 best effort
- 如果你要做公司级归档，建议在后台里填上公司名，减少误判
- 由于评论和帖子都来自同一平台，建议人工复核后再提升核验状态
- cookie 默认会保存到 `data/xiaohongshu-cookie.txt`
- 你可以直接在后台页面更新它，评论采集会自动读取最新值
