const state = {
  companies: [],
  query: "",
  source: "all",
  status: "all",
  view: "all",
};

const labels = {
  xiaohongshu: "小红书",
  douyin: "抖音",
  news: "新闻",
  jobsite: "招聘网站",
  pending: "待核验",
  partial: "部分核验",
  verified: "已核验",
  high: "高",
  medium: "中",
  low: "低",
};

const statusRank = {
  pending: 1,
  partial: 2,
  verified: 3,
};

function getFilteredCompanies() {
  return state.companies.filter((company) => {
    const matchQuery = company.name.toLowerCase().includes(state.query.toLowerCase());
    const matchSource =
      state.source === "all" || company.evidence.some((item) => item.sourceType === state.source);
    const matchStatus = state.status === "all" || company.verificationStatus === state.status;
    const matchView =
      state.view === "all" ||
      (company.boycottRecommended && statusRank[company.verificationStatus] >= statusRank.partial);

    return matchQuery && matchSource && matchStatus && matchView;
  });
}

function render() {
  const listEl = document.getElementById("companyList");
  const countEl = document.getElementById("resultCount");
  const companies = getFilteredCompanies();

  countEl.textContent = `共 ${companies.length} 家公司`;

  if (companies.length === 0) {
    listEl.innerHTML = '<div class="empty">暂无匹配结果，请调整筛选条件。</div>';
    return;
  }

  listEl.innerHTML = companies
    .map((company) => {
      const tags = [
        `行业：${company.industry}`,
        `风险等级：${labels[company.riskLevel] || company.riskLevel}`,
        `核验：${labels[company.verificationStatus] || company.verificationStatus}`,
        `更新时间：${company.lastUpdated}`,
      ];

      return `
      <article class="card">
        <h3>${company.name}</h3>
        <p class="meta">${company.summary}</p>
        <div>${tags.map((tag) => `<span class="tag">${tag}</span>`).join("")}</div>
        <p class="status">消费建议：${company.boycottRecommended ? "建议避雷" : "持续观察"}</p>
        <div class="evidence">
          <strong>证据来源（${company.evidence.length}）</strong>
          <ul>
            ${company.evidence
              .map(
                (item) => `
                <li>
                  [${labels[item.sourceType] || item.sourceType}] ${item.sourceTitle}
                  <br />
                  <small>${item.capturedAt} · ${item.summary}</small>
                  <br />
                  <a href="${item.sourceUrl}" target="_blank" rel="noopener noreferrer">查看来源</a>
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

function bindEvents() {
  document.getElementById("searchInput").addEventListener("input", (event) => {
    state.query = event.target.value.trim();
    render();
  });

  document.getElementById("sourceFilter").addEventListener("change", (event) => {
    state.source = event.target.value;
    render();
  });

  document.getElementById("statusFilter").addEventListener("change", (event) => {
    state.status = event.target.value;
    render();
  });

  document.querySelectorAll(".tab").forEach((button) => {
    button.addEventListener("click", () => {
      document.querySelectorAll(".tab").forEach((tab) => tab.classList.remove("active"));
      button.classList.add("active");
      state.view = button.dataset.view;
      render();
    });
  });
}

async function init() {
  const response = await fetch("data/companies.json");
  state.companies = await response.json();
  bindEvents();
  render();
}

init();
