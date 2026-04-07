# 小红书浏览器插件 API 契约

## 1. 约定
- 本机服务默认地址：`http://127.0.0.1:8787`
- 所有接口均为 JSON
- 所有接口允许 CORS，以便浏览器插件访问
- `comment_limit=0` 表示抓取全部可见评论

## 2. 健康检查
### `GET /api/xhs-plugin/status`
返回插件可用状态和默认配置。

示例响应：
```json
{
  "ok": true,
  "service": {
    "name": "forbidden-company-admin",
    "version": "1",
    "online": true
  },
  "xhs": {
    "cookie_configured": true,
    "cookie_length": 1386,
    "cookie_path": "/Users/jack/IdeaProjects/forbidden_company/data/xiaohongshu-cookie.txt"
  },
  "defaults": {
    "comment_limit": 0,
    "include_comments": true
  },
  "endpoints": {
    "collect": "/api/xhs-plugin/collect",
    "refresh": "/api/xhs-plugin/refresh",
    "status": "/api/xhs-plugin/status",
    "cookie": "/api/xhs-cookie"
  }
}
```

## 3. 采集
### `POST /api/xhs-plugin/collect`
采集当前页面帖子和评论。

请求体：
```json
{
  "url": "https://www.xiaohongshu.com/discovery/item/...",
  "title": "可选",
  "comment_limit": 0,
  "include_comments": true,
  "company_name": "",
  "source_platform": "小红书"
}
```

返回字段：
- `ok`
- `output_csv`
- `download_csv_path`
- `download_csv_url`
- `download_json_path`
- `download_json_url`
- `record_count`
- `post_count`
- `comment_count`
- `preview_rows`
- `log`

### `POST /api/xhs-plugin/refresh`
刷新单条链接，先清除旧记录，再重新采集。

请求体：
```json
{
  "url": "https://www.xiaohongshu.com/discovery/item/...",
  "comment_limit": 0,
  "include_comments": true
}
```

返回字段与 `collect` 一致。

## 4. Cookie
### `POST /api/xhs-cookie`
保存或更新小红书 cookie。

请求体：
```json
{
  "cookie": "abRequestId=...; web_session=..."
}
```

### `GET /api/xhs-cookie`
读取 cookie 配置状态。

## 5. 下载
采集成功后，`download_csv_url` 和 `download_json_url` 可直接用于浏览器下载。

## 6. 插件侧约定
- 插件只负责入口、状态展示和导出操作
- 插件不直接执行复杂抓取逻辑
- 插件应在每次采集前检查 `status`
- 插件可将最近任务保存在本地 `chrome.storage.local`
