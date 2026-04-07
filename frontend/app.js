const API_BASE = window.PUBLIC_API_BASE || "http://127.0.0.1:8787";

const state = {
  companies: [],
  query: "",
  source: "all",
  status: "all",
  view: "all",
  page: 1,
  pageSize: 8,
};

const labels = {
  xiaohongshu: "小红书",
  douyin: "抖音",
  news: "新闻",
  jobsite: "招聘网站",
  pending: "待核验",
  partial: "部分核验",
  verified: "已核验",
  error: "错误",
  high: "高",
  medium: "中",
  low: "低",
};

const statusRank = {
  pending: 1,
  partial: 2,
  verified: 3,
};

function escapeHtml(str) {
  return String(str || "")
    .replaceAll("&", "&amp;")
    .replaceAll("<", "&lt;")
    .replaceAll(">", "&gt;")
    .replaceAll('"', "&quot;")
    .replaceAll("'", "&#39;");
}

function getFilteredCompanies() {
  return state.companies.filter((company) => {
    const visible = company.verificationStatus !== "error";
    const matchQuery = company.name.toLowerCase().includes(state.query.toLowerCase());
    const matchSource =
      state.source === "all" || company.evidence.some((item) => item.sourceType === state.source);
    const matchStatus = state.status === "all" || company.verificationStatus === state.status;
    const matchView =
      state.view === "all" ||
      (company.boycottRecommended && statusRank[company.verificationStatus] >= statusRank.partial);

    return visible && matchQuery && matchSource && matchStatus && matchView;
  });
}

function getPagedCompanies(companies) {
  const totalPages = Math.max(1, Math.ceil(companies.length / state.pageSize));
  if (state.page > totalPages) {
    state.page = totalPages;
  }
  const start = (state.page - 1) * state.pageSize;
  const items = companies.slice(start, start + state.pageSize);
  return { items, totalPages };
}

function renderPagination(container, totalPages) {
  if (totalPages <= 1) {
    container.innerHTML = "";
    return;
  }

  const pages = [];
  const pushPage = (page) => {
    pages.push(`
      <button class="page-btn ${page === state.page ? "active" : ""}" data-page="${page}">${page}</button>
    `);
  };

  const start = Math.max(1, state.page - 2);
  const end = Math.min(totalPages, state.page + 2);

  if (start > 1) {
    pushPage(1);
    if (start > 2) {
      pages.push('<span class="page-ellipsis">…</span>');
    }
  }

  for (let page = start; page <= end; page += 1) {
    pushPage(page);
  }

  if (end < totalPages) {
    if (end < totalPages - 1) {
      pages.push('<span class="page-ellipsis">…</span>');
    }
    pushPage(totalPages);
  }

  container.innerHTML = `
    <button class="page-btn" data-page="${Math.max(1, state.page - 1)}" ${state.page === 1 ? "disabled" : ""}>上一页</button>
    ${pages.join("")}
    <button class="page-btn" data-page="${Math.min(totalPages, state.page + 1)}" ${state.page === totalPages ? "disabled" : ""}>下一页</button>
  `;
}

function render() {
  const listEl = document.getElementById("companyList");
  const countEl = document.getElementById("resultCount");
  const pagerEl = document.getElementById("pagination");
  const companies = getFilteredCompanies();
  const { items, totalPages } = getPagedCompanies(companies);

  countEl.textContent = `共 ${companies.length} 家公司 · 第 ${state.page}/${totalPages} 页`;
  renderPagination(pagerEl, totalPages);

  if (companies.length === 0) {
    listEl.innerHTML = '<div class="empty">暂无匹配结果，请调整筛选条件。</div>';
    return;
  }

  listEl.innerHTML = items
    .map((company) => {
      const tags = [
        `行业：${company.industry}`,
        `风险等级：${labels[company.riskLevel] || company.riskLevel}`,
        `核验：${labels[company.verificationStatus] || company.verificationStatus}`,
        `更新时间：${company.lastUpdated}`,
      ];

      const products = company.products || [];
      const pendingProducts = company.pendingProducts || [];

      return `
      <article class="card">
        <h3>${escapeHtml(company.name)}</h3>
        <p class="meta">${escapeHtml(company.summary)}</p>
        <div>${tags.map((tag) => `<span class="tag">${escapeHtml(tag)}</span>`).join("")}</div>
        ${
          company.verificationStatus === "pending"
            ? '<p class="pending-tip">这条线索正在审核中，欢迎补充证据协助核验。</p>'
            : ""
        }
        <div class="products">
          <strong>相关产品（${products.length}）</strong>
          <ul>
            ${
              products.length
                ? products
                    .map((product) => {
                      const name = escapeHtml(product.name);
                      const category = product.category ? ` · ${escapeHtml(product.category)}` : "";
                      const confidence = product.confidence ? ` · 置信度:${escapeHtml(product.confidence)}` : "";
                      const link = product.url
                        ? `<a href="${escapeHtml(product.url)}" target="_blank" rel="noopener noreferrer">${name}</a>`
                        : name;
                      return `<li>${link}${category}${confidence}</li>`;
                    })
                    .join("")
                : `<li>暂无产品映射。
                    <button class="inline-btn suggest-product-btn" data-company="${escapeHtml(company.name)}">我来补充产品</button>
                   </li>`
            }
          </ul>
          ${
            pendingProducts.length
              ? `<p class="pending-tip">还有 ${pendingProducts.length} 条产品建议正在审核中。</p>`
              : ""
          }
        </div>
        <p class="status">消费建议：${company.boycottRecommended ? "建议避雷" : "持续观察"}</p>
        <div class="evidence">
          <strong>证据来源（${company.evidence.length}）</strong>
          <ul>
            ${company.evidence
              .map(
                (item) => `
                <li>
                  [${escapeHtml(labels[item.sourceType] || item.sourceType)}] ${escapeHtml(item.sourceTitle)}
                  <br />
                  <small>${escapeHtml(item.capturedAt)} · ${escapeHtml(item.summary)}</small>
                  <br />
                  <a href="${escapeHtml(item.sourceUrl)}" target="_blank" rel="noopener noreferrer">查看来源</a>
                </li>
              `
              )
              .join("")}
          </ul>
        </div>
      </article>
    `;
    })
    .join("");
}

async function fetchCompanies() {
  try {
    const response = await fetch(`${API_BASE}/api/public/companies?t=${Date.now()}`);
    if (!response.ok) {
      throw new Error(`加载数据失败: ${response.status}`);
    }
    const payload = await response.json();
    state.companies = payload.items || [];
    const maxPage = Math.max(1, Math.ceil(state.companies.length / state.pageSize));
    state.page = Math.min(state.page, maxPage);
    return;
  } catch (error) {
    const fallback = await fetch(`data/companies.json?t=${Date.now()}`);
    if (!fallback.ok) {
      throw error;
    }
    state.companies = await fallback.json();
    const maxPage = Math.max(1, Math.ceil(state.companies.length / state.pageSize));
    state.page = Math.min(state.page, maxPage);
  }
}

async function submitEvidence() {
  const msgEl = document.getElementById("uEvidenceMsg");
  const payload = {
    company_name: document.getElementById("uCompany").value.trim(),
    source_platform: document.getElementById("uPlatform").value.trim(),
    source_type: document.getElementById("uSourceType").value,
    source_url: document.getElementById("uUrl").value.trim(),
    source_title: document.getElementById("uTitle").value.trim(),
    city: document.getElementById("uCity").value.trim(),
    job_title: document.getElementById("uJobTitle").value.trim(),
    evidence_quote: document.getElementById("uQuote").value.trim(),
    evidence_summary: document.getElementById("uSummary").value.trim(),
  };

  if (!payload.company_name || !payload.source_platform || !payload.source_url) {
    msgEl.textContent = "请填写公司名称、来源平台、来源 URL。";
    return;
  }

  try {
    const res = await fetch(`${API_BASE}/api/public/collect`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(payload),
    });
    const json = await res.json();
    if (!res.ok) throw new Error(json.error || "提交失败");

    msgEl.textContent = `提交成功（${json.record_id}），状态：正在审核中。`;
    await fetchCompanies();
    render();
  } catch (err) {
    msgEl.textContent = `提交失败：${err.message}`;
  }
}

async function submitProductSuggestion() {
  const msgEl = document.getElementById("uProductMsg");
  const payload = {
    company_name: document.getElementById("uProductCompany").value.trim(),
    product_name: document.getElementById("uProductName").value.trim(),
    product_category: document.getElementById("uProductCategory").value.trim(),
    product_url: document.getElementById("uProductUrl").value.trim(),
    confidence: document.getElementById("uProductConfidence").value,
    source_note: document.getElementById("uProductNote").value.trim() || "community submit",
  };

  if (!payload.company_name || !payload.product_name) {
    msgEl.textContent = "请填写公司名称和产品名称。";
    return;
  }

  try {
    const res = await fetch(`${API_BASE}/api/public/product-suggestion`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(payload),
    });
    const json = await res.json();
    if (!res.ok) throw new Error(json.error || "提交失败");

    msgEl.textContent = "产品映射已提交，状态：正在审核中。";
    await fetchCompanies();
    render();
  } catch (err) {
    msgEl.textContent = `提交失败：${err.message}`;
  }
}

function bindEvents() {
  document.getElementById("searchInput").addEventListener("input", (event) => {
    state.query = event.target.value.trim();
    state.page = 1;
    render();
  });

  document.getElementById("sourceFilter").addEventListener("change", (event) => {
    state.source = event.target.value;
    state.page = 1;
    render();
  });

  document.getElementById("statusFilter").addEventListener("change", (event) => {
    state.status = event.target.value;
    state.page = 1;
    render();
  });

  document.querySelectorAll(".tab").forEach((button) => {
    button.addEventListener("click", () => {
      document.querySelectorAll(".tab").forEach((tab) => tab.classList.remove("active"));
      button.classList.add("active");
      state.view = button.dataset.view;
      state.page = 1;
      render();
    });
  });

  document.getElementById("refreshDataBtn").addEventListener("click", async () => {
    await fetchCompanies();
    render();
  });

  document.getElementById("toggleContributionBtn").addEventListener("click", () => {
    document.getElementById("contributionPanel").classList.toggle("hidden");
  });

  document.getElementById("pagination").addEventListener("click", (event) => {
    const button = event.target.closest(".page-btn");
    if (!button || button.disabled) return;
    const page = Number(button.dataset.page);
    if (!Number.isFinite(page) || page < 1) return;
    state.page = page;
    render();
  });

  document.getElementById("submitEvidenceBtn").addEventListener("click", submitEvidence);
  document.getElementById("submitProductBtn").addEventListener("click", submitProductSuggestion);

  document.getElementById("companyList").addEventListener("click", (event) => {
    const button = event.target.closest(".suggest-product-btn");
    if (!button) return;

    const companyName = button.dataset.company || "";
    document.getElementById("uProductCompany").value = companyName;
    document.getElementById("contributionPanel").classList.remove("hidden");
    document.getElementById("uProductName").focus();
  });
}

async function init() {
  await fetchCompanies();
  bindEvents();
  render();
  setInterval(async () => {
    try {
      await fetchCompanies();
      render();
    } catch (_) {
      // Keep the current view if the backend is temporarily unavailable.
    }
  }, 30000);
}

init();
