const API_BASE = window.PUBLIC_API_BASE || "http://127.0.0.1:8787";

const state = {
  companies: [],
  query: "",
  conclusion: "all",
  source: "all",
  status: "all",
  view: "all",
  selectedCompanyId: "",
  productModalCompany: "",
  productModalOpen: false,
  productModalMessage: "",
  showAllEvidence: false,
  modalScrollY: 0,
  modalLocked: false,
  page: 1,
  pageSize: 8,
};

const labels = {
  xiaohongshu: "小红书",
  douyin: "抖音",
  news: "新闻",
  jobsite: "招聘网站",
  clear: "明确存在35+筛选",
  suspected: "疑似存在35+筛选",
  insufficient: "证据不足",
  none: "暂无年龄相关线索",
  pending: "待核验",
  partial: "部分核验",
  verified: "已核验",
  error: "错误",
  high: "高",
  medium: "中",
  low: "低",
  L1: "L1 线索",
  L2: "L2 交叉",
  L3: "L3 强证据",
};

const statusRank = {
  pending: 1,
  partial: 2,
  verified: 3,
};

const conclusionRank = {
  clear: 1,
  suspected: 2,
  insufficient: 3,
  none: 4,
};

function getCompanyConclusion(company) {
  return company.ageRiskConclusion || company.age_risk_conclusion || "insufficient";
}

function getProductLine(company) {
  return (
    company.conclusionBusinessLine ||
    company.productLine ||
    (company.productLines && company.productLines[0] && company.productLines[0].businessLine) ||
    company.conclusionProductName ||
    ""
  );
}

function getConclusionReason(company) {
  return company.conclusionReason || company.conclusion_reason || company.summary || "暂无补充说明。";
}

function getEvidenceLevel(company) {
  return company.evidenceLevel || company.evidence_level || "L1";
}

function getConfidence(company) {
  return company.ageRiskConfidence || company.age_risk_confidence || "low";
}

function escapeHtml(str) {
  return String(str || "")
    .replaceAll("&", "&amp;")
    .replaceAll("<", "&lt;")
    .replaceAll(">", "&gt;")
    .replaceAll('"', "&quot;")
    .replaceAll("'", "&#39;");
}

function getFilteredCompanies() {
  return state.companies
    .filter((company) => {
    const visible = company.verificationStatus !== "error";
    const matchQuery = company.name.toLowerCase().includes(state.query.toLowerCase());
    const companyConclusion = getCompanyConclusion(company);
    const matchConclusion =
      state.conclusion === "all" || companyConclusion === state.conclusion;
    const evidence = company.evidence || [];
    const matchSource = state.source === "all" || evidence.some((item) => item.sourceType === state.source);
    const matchStatus = state.status === "all" || company.verificationStatus === state.status;
    const matchView =
      state.view === "all" ||
      (company.boycottRecommended && statusRank[company.verificationStatus] >= statusRank.partial);

    return visible && matchQuery && matchConclusion && matchSource && matchStatus && matchView;
    })
    .sort((a, b) => {
      const aRank = conclusionRank[getCompanyConclusion(a)] || 99;
      const bRank = conclusionRank[getCompanyConclusion(b)] || 99;
      if (aRank !== bRank) return aRank - bRank;

      const aCount = a.conclusionEvidenceCount || (a.evidence || []).length || 0;
      const bCount = b.conclusionEvidenceCount || (b.evidence || []).length || 0;
      if (aCount !== bCount) return bCount - aCount;

      return String(b.lastUpdated || "").localeCompare(String(a.lastUpdated || ""));
    });
}

function getCompanyStats(companies) {
  const stats = {
    total: companies.length,
    clear: 0,
    suspected: 0,
    insufficient: 0,
    none: 0,
  };

  companies.forEach((company) => {
    const conclusion = getCompanyConclusion(company);
    if (stats[conclusion] !== undefined) {
      stats[conclusion] += 1;
    }
  });

  return stats;
}

function getSelectedCompany() {
  return state.companies.find((company) => company.id === state.selectedCompanyId) || null;
}

function getProductModalCompany() {
  return state.productModalCompany || "";
}

function openProductModal(companyName) {
  state.productModalCompany = companyName || state.productModalCompany || "";
  state.productModalOpen = true;
  state.productModalMessage = "";
  state.showAllEvidence = false;
  const companyInput = document.getElementById("uProductCompany");
  if (companyInput) {
    companyInput.value = state.productModalCompany;
  }
  render();
  window.setTimeout(() => {
    const productInput = document.getElementById("uProductName");
    if (productInput) {
      productInput.focus();
    }
  }, 0);
}

function closeProductModal() {
  state.productModalOpen = false;
  state.productModalMessage = "";
  render();
}

function lockModalScroll() {
  if (state.modalLocked) return;
  state.modalScrollY = window.scrollY || window.pageYOffset || 0;
  document.body.classList.add("modal-open");
  document.body.style.position = "fixed";
  document.body.style.top = `-${state.modalScrollY}px`;
  document.body.style.width = "100%";
  state.modalLocked = true;
}

function unlockModalScroll() {
  if (!state.modalLocked) return;
  document.body.classList.remove("modal-open");
  document.body.style.position = "";
  document.body.style.top = "";
  document.body.style.width = "";
  window.scrollTo(0, state.modalScrollY);
  state.modalLocked = false;
}

function parseCapturedAt(value) {
  const text = String(value || "").trim();
  if (!text) return 0;
  const ts = Date.parse(text);
  return Number.isFinite(ts) ? ts : 0;
}

function sortEvidenceItems(items) {
  return [...items].sort((a, b) => {
    const aTs = parseCapturedAt(a.capturedAt || a.captured_at);
    const bTs = parseCapturedAt(b.capturedAt || b.captured_at);
    if (aTs !== bTs) return bTs - aTs;
    return String(b.sourceTitle || b.source_title || "").localeCompare(String(a.sourceTitle || a.source_title || ""));
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
  const summaryEl = document.getElementById("summaryBar");
  const detailEl = document.getElementById("detailPanel");
  const productEl = document.getElementById("productPanel");
  const companies = getFilteredCompanies();
  const { items, totalPages } = getPagedCompanies(companies);
  const stats = getCompanyStats(state.companies);
  const selectedCompany = getSelectedCompany();
  const productModalCompany = getProductModalCompany();
  if (selectedCompany || state.productModalOpen) {
    lockModalScroll();
  } else {
    unlockModalScroll();
  }

  countEl.textContent = `共 ${companies.length} 家公司 · 第 ${state.page}/${totalPages} 页`;
  renderPagination(pagerEl, totalPages);
  summaryEl.innerHTML = `
    <div class="summary-card">
      <span>总公司数</span>
      <strong>${stats.total}</strong>
    </div>
    <div class="summary-card">
      <span>明确风险</span>
      <strong>${stats.clear}</strong>
    </div>
    <div class="summary-card">
      <span>疑似风险</span>
      <strong>${stats.suspected}</strong>
    </div>
      <div class="summary-card">
      <span>证据不足/暂无</span>
      <strong>${stats.insufficient + stats.none}</strong>
    </div>
  `;

  if (companies.length === 0) {
    listEl.innerHTML = '<div class="empty">暂无匹配结果，请调整筛选条件。</div>';
    detailEl.classList.add("hidden");
    detailEl.innerHTML = "";
    productEl.classList.add("hidden");
    productEl.innerHTML = "";
    return;
  }

  listEl.innerHTML = items
    .map((company) => {
      const conclusion = getCompanyConclusion(company);
      const conclusionLabel = labels[conclusion] || conclusion;
      const productLine = getProductLine(company) || "待补充";
      const evidenceLevel = labels[getEvidenceLevel(company)] || getEvidenceLevel(company);
      const confidence = labels[getConfidence(company)] || getConfidence(company);
      const reason = escapeHtml(getConclusionReason(company));
      const productLines = company.productLines || [];
      const tags = [
        `行业：${company.industry}`,
        `风险等级：${labels[company.riskLevel] || company.riskLevel}`,
        `核验：${labels[company.verificationStatus] || company.verificationStatus}`,
        `结论置信度：${confidence}`,
        `证据等级：${evidenceLevel}`,
        `更新时间：${company.lastUpdated}`,
      ];

      const products = company.products || [];
      const pendingProducts = company.pendingProducts || [];
      const productActionLabel = products.length ? "继续补充产品" : "我来补充产品";

      return `
      <article class="card">
        <div class="card-header">
          <div>
            <h3 class="company-title">${escapeHtml(company.name)}</h3>
            <p class="meta">${escapeHtml(company.summary)}</p>
          </div>
          <span class="conclusion-badge ${escapeHtml(conclusion)}">${escapeHtml(conclusionLabel)}</span>
        </div>
        <div class="meta-line">${tags.map((tag) => `<span class="tag">${escapeHtml(tag)}</span>`).join("")}</div>
        <div class="conclusion-panel">
          <div class="label">核心判断</div>
          <p class="reason">
            ${escapeHtml(conclusionLabel)} · ${escapeHtml(productLine)}
          </p>
          <p class="reason">${reason}</p>
        </div>
        
        ${
          productLines.length
            ? `<div class="products">
                <strong>业务线映射（${productLines.length}）</strong>
                <ul>
                  ${productLines
                    .map((line) => {
                      const name = escapeHtml(line.name);
                      const businessLine = line.businessLine ? ` · ${escapeHtml(line.businessLine)}` : "";
                      const category = line.category ? ` · ${escapeHtml(line.category)}` : "";
                      const confidenceText = line.confidence ? ` · 置信度:${escapeHtml(line.confidence)}` : "";
                      return `<li>${name}${businessLine}${category}${confidenceText}</li>`;
                    })
                    .join("")}
                </ul>
              </div>`
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
                   </li>`
            }
          </ul>
          ${
            pendingProducts.length
              ? `<p class="pending-tip">还有 ${pendingProducts.length} 条产品建议正在审核中。</p>`
              : ""
          }
          <button class="secondary-btn inline-btn suggest-product-btn" data-company="${escapeHtml(company.name)}">${escapeHtml(productActionLabel)}</button>
        </div>
        <p class="status">消费建议：${company.boycottRecommended ? "建议避雷" : "持续观察"}</p>
        <button class="secondary-btn inline-btn view-evidence-btn" data-company-id="${escapeHtml(company.id)}">查看证据来源</button>
      </article>
    `;
    })
    .join("");

  if (selectedCompany) {
    const evidenceItems = sortEvidenceItems(selectedCompany.evidence || []);
    const productLines = selectedCompany.productLines || [];
    const products = selectedCompany.products || [];
    const conclusion = getCompanyConclusion(selectedCompany);
    const conclusionLabel = labels[conclusion] || conclusion;
    const confidence = labels[getConfidence(selectedCompany)] || getConfidence(selectedCompany);
    const evidenceLevel = labels[getEvidenceLevel(selectedCompany)] || getEvidenceLevel(selectedCompany);
    const productLine = getProductLine(selectedCompany) || "待补充";
    const reason = escapeHtml(getConclusionReason(selectedCompany));
    const visibleEvidenceItems = state.showAllEvidence ? evidenceItems : evidenceItems.slice(0, 3);
    const hasMoreEvidence = evidenceItems.length > visibleEvidenceItems.length;

    detailEl.classList.remove("hidden");
    detailEl.innerHTML = `
      <div class="detail-backdrop" data-close="true"></div>
      <div class="detail-dialog" role="dialog" aria-modal="true" aria-labelledby="detailTitle">
        <div class="detail-dialog-shell">
          <div class="detail-header">
            <div>
              <h3 class="detail-title" id="detailTitle">${escapeHtml(selectedCompany.name)} · 证据来源详情</h3>
              <p class="detail-subtitle">
                当前结论：${escapeHtml(conclusionLabel)} · 置信度：${escapeHtml(confidence)} · 证据等级：${escapeHtml(evidenceLevel)} · 业务线：${escapeHtml(productLine)}
              </p>
            </div>
            <div class="detail-actions">
              <button class="secondary-btn" id="closeDetailBtn">关闭详情</button>
            </div>
          </div>
          <div class="detail-scroll">
            <div class="detail-grid">
              <div class="detail-block">
                <h4>结论说明</h4>
                <p class="detail-subtitle">${reason}</p>
                <ul class="detail-list">
                  <li>公司：${escapeHtml(selectedCompany.name)}</li>
                  <li>对应产品：${escapeHtml(selectedCompany.conclusionProductName || productLine || "待补充")}</li>
                  <li>业务线：${escapeHtml(selectedCompany.conclusionBusinessLine || productLine || "待补充")}</li>
                  <li>证据数量：${escapeHtml(String(selectedCompany.conclusionEvidenceCount || evidenceItems.length || 0))}</li>
                </ul>
              </div>
              <div class="detail-block">
                <h4>产品映射</h4>
                <ul class="detail-list">
                  ${
                    productLines.length
                      ? productLines
                          .map((line) => {
                            const name = escapeHtml(line.name);
                            const businessLine = line.businessLine ? ` · ${escapeHtml(line.businessLine)}` : "";
                            const category = line.category ? ` · ${escapeHtml(line.category)}` : "";
                            return `<li>${name}${businessLine}${category}</li>`;
                          })
                          .join("")
                      : `<li>暂无业务线映射</li>`
                  }
                </ul>
                <h4 class="evidence-block">相关产品</h4>
                <ul class="detail-list">
                  ${
                    products.length
                      ? products
                          .map((product) => {
                            const name = escapeHtml(product.name);
                            const category = product.category ? ` · ${escapeHtml(product.category)}` : "";
                            return `<li>${name}${category}</li>`;
                          })
                          .join("")
                      : `<li>暂无产品映射</li>`
                  }
                </ul>
              </div>
            </div>
            <div class="detail-block evidence-block">
              <h4>证据来源</h4>
              ${
                visibleEvidenceItems.length
                  ? visibleEvidenceItems
                      .map(
                        (item) => `
                        <div class="evidence-source">
                          <strong>[${escapeHtml(labels[item.sourceType] || item.sourceType)}] ${escapeHtml(item.sourceTitle)}</strong>
                          <div>${escapeHtml(item.capturedAt)} · ${escapeHtml(item.summary)}</div>
                          <div><a href="${escapeHtml(item.sourceUrl)}" target="_blank" rel="noopener noreferrer">打开原始来源</a></div>
                        </div>
                      `
                      )
                      .join("")
                  : '<p class="detail-subtitle">当前没有可展示的证据来源。</p>'
              }
              ${
                hasMoreEvidence
                  ? `<button class="secondary-btn inline-btn" id="toggleEvidenceBtn">${state.showAllEvidence ? "收起证据" : `展开全部证据（+${evidenceItems.length - 3}）`}</button>`
                  : ""
              }
            </div>
          </div>
        </div>
      </div>
    `;
  } else {
    detailEl.classList.add("hidden");
    detailEl.innerHTML = "";
  }

  if (state.productModalOpen) {
    productEl.classList.remove("hidden");
    productEl.innerHTML = `
      <div class="detail-backdrop" data-product-close="true"></div>
      <div class="detail-dialog product-dialog" role="dialog" aria-modal="true" aria-labelledby="productTitle">
        <div class="detail-dialog-shell">
          <div class="detail-header">
            <div>
              <h3 class="detail-title" id="productTitle">补充产品映射</h3>
              <p class="detail-subtitle">提交后会进入审核，你可以继续补充同一公司的下一条产品。</p>
            </div>
            <div class="detail-actions">
              <button class="secondary-btn" id="closeProductBtn">关闭</button>
            </div>
          </div>
          <div class="detail-scroll">
            <div class="detail-block">
              <div class="form-row">
                <input id="uProductCompany" placeholder="公司名称*" value="${escapeHtml(productModalCompany)}" />
                <input id="uProductName" placeholder="产品名称*" />
                <input id="uProductCategory" placeholder="产品分类" />
                <select id="uProductConfidence">
                  <option value="unverified">unverified</option>
                  <option value="partial">partial</option>
                  <option value="verified">verified</option>
                </select>
              </div>
              <div class="form-row">
                <input id="uProductUrl" placeholder="产品 URL（可空）" />
                <input id="uProductNote" placeholder="补充说明" value="community submit" />
              </div>
              <button class="primary" id="submitProductBtn">提交产品映射（待审核）</button>
              <p id="uProductMsg" class="hint">${escapeHtml(state.productModalMessage)}</p>
            </div>
          </div>
        </div>
      </div>
    `;
  } else {
    productEl.classList.add("hidden");
    productEl.innerHTML = "";
  }
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
  const payload = {
    company_name: document.getElementById("uProductCompany").value.trim(),
    product_name: document.getElementById("uProductName").value.trim(),
    product_category: document.getElementById("uProductCategory").value.trim(),
    product_url: document.getElementById("uProductUrl").value.trim(),
    confidence: document.getElementById("uProductConfidence").value,
    source_note: document.getElementById("uProductNote").value.trim() || "community submit",
  };

  if (!payload.company_name || !payload.product_name) {
    state.productModalMessage = "请填写公司名称和产品名称。";
    render();
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

    state.productModalCompany = payload.company_name;
    state.productModalMessage = "产品映射已提交，状态：正在审核中。你可以继续补充下一条产品。";
    await fetchCompanies();
    render();
    window.setTimeout(() => {
      const companyInput = document.getElementById("uProductCompany");
      const productInput = document.getElementById("uProductName");
      if (companyInput) companyInput.value = payload.company_name;
      if (productInput) productInput.focus();
    }, 0);
  } catch (err) {
    state.productModalMessage = `提交失败：${err.message}`;
    render();
  }
}

async function refreshLatestData() {
  const button = document.getElementById("refreshDataBtn");
  const originalText = button ? button.textContent : "刷新最新数据";
  const countEl = document.getElementById("resultCount");
  if (button) {
    button.disabled = true;
    button.textContent = "正在刷新...";
  }
  if (countEl) {
    countEl.textContent = "正在刷新最新数据...";
  }

  try {
    const res = await fetch(`${API_BASE}/api/export?t=${Date.now()}`);
    if (!res.ok) {
      const payload = await res.json().catch(() => ({}));
      throw new Error(payload.error || `刷新失败: ${res.status}`);
    }
    await fetchCompanies();
    render();
    if (countEl) {
      countEl.textContent = `最新数据已刷新，共 ${state.companies.length} 家公司。`;
    }
  } catch (err) {
    if (countEl) {
      countEl.textContent = `刷新失败：${err.message}`;
    }
  } finally {
    if (button) {
      button.disabled = false;
      button.textContent = originalText;
    }
  }
}

function bindEvents() {
  document.getElementById("searchInput").addEventListener("input", (event) => {
    state.query = event.target.value.trim();
    state.page = 1;
    render();
  });

  document.getElementById("conclusionFilter").addEventListener("change", (event) => {
    state.conclusion = event.target.value;
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
    await refreshLatestData();
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

  document.getElementById("companyList").addEventListener("click", (event) => {
    const button = event.target.closest(".suggest-product-btn");
    if (button) {
      const companyName = button.dataset.company || "";
      openProductModal(companyName);
      return;
    }

    const evidenceButton = event.target.closest(".view-evidence-btn");
    if (!evidenceButton) return;
    state.selectedCompanyId = evidenceButton.dataset.companyId || "";
    state.showAllEvidence = false;
    render();
  });

  document.getElementById("detailPanel").addEventListener("click", (event) => {
    const closeButton = event.target.closest("#closeDetailBtn");
    const backdrop = event.target.closest("[data-close='true']");
    if (closeButton || backdrop) {
      state.selectedCompanyId = "";
      state.showAllEvidence = false;
      render();
      return;
    }

    const toggleButton = event.target.closest("#toggleEvidenceBtn");
    if (!toggleButton) return;
    state.showAllEvidence = !state.showAllEvidence;
    render();
  });

  document.getElementById("productPanel").addEventListener("click", (event) => {
    const closeButton = event.target.closest("#closeProductBtn");
    const backdrop = event.target.closest("[data-product-close='true']");
    if (closeButton || backdrop) {
      closeProductModal();
      return;
    }

    const submitButton = event.target.closest("#submitProductBtn");
    if (submitButton) {
      submitProductSuggestion();
    }
  });

  document.addEventListener("keydown", (event) => {
    if (event.key !== "Escape") return;
    if (state.productModalOpen) {
      closeProductModal();
      return;
    }
    if (state.selectedCompanyId) {
      state.selectedCompanyId = "";
      state.showAllEvidence = false;
      render();
    }
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
