# Architecture: MVP静态站点

## 1. 技术选型
- 前端：原生 HTML + CSS + JavaScript
- 数据：本地 JSON 文件（`data/companies.json`）
- 部署：任意静态托管（GitHub Pages/Vercel静态模式/Nginx）

## 2. 信息架构
- 顶部：项目定位 + 免责声明
- 控制区：搜索框、来源筛选、核验状态筛选、视图切换
- 内容区：
  - 公司目录（默认）
  - 拒绝消费清单（仅显示 `boycottRecommended=true` 且核验状态>=partial）
- 卡片结构：公司基础信息 + 证据列表

## 3. 前端模块
- `app.js`
  - 数据加载
  - 状态管理（query/source/status/view）
  - 过滤计算与渲染
- `styles.css`
  - 主题变量
  - 响应式布局
- `index.html`
  - 页面骨架与挂载点

## 4. 数据流
1. 页面加载读取 `data/companies.json`
2. 用户输入/筛选触发状态更新
3. 基于状态计算过滤结果
4. 渲染公司卡片与证据明细

## 5. 扩展路径
- V1.1: 增加提交表单与人工审核后台
- V1.2: 增加“证据可信度评分”
- V2.0: API + DB + 审核工作流 + 申诉机制
