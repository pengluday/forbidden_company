#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "$0")/.." && pwd)"
TODAY="$(date +%F)"

python3 -m collectors.collect_zhilian \
  --output-csv "$ROOT_DIR/data/source-intake-zhilian-$TODAY.csv" \
  --merge-csv "$ROOT_DIR/data/source-intake-round1-jobsites.csv" \
  --db "$ROOT_DIR/data/forbidden_company.db" \
  --collector "scheduled-zhilian" \
  --limit 100 \
  --seed-url "https://fe-api.zhaopin.com/c/i/search/positions?MmEwMD=5Hcl4QR9z3G1Ec3KpAuMe7xmQB0Nb91R_znn2UN5te.5JCi9zvtFJib1jJOx6GdQpA.xmwqTdnVzA9GykOQOsekA6xofO6RuXlYbMU9p24IBuVmhKrjcsQm9PU0ZB.NTS.Zm50JRcmhrlTxue6SrDUNzkkLQ40FQtIUjJVZW74OjC1dss6l_aJaZmGFHR1U1XdEFJNcQYTgf7ShSstmvtbp9RzfT1TBpskEXmWkW0sPbUxkc_gF_gSQ81JvL7qNyUElWkeFV962rko1YRP9YpLjluBn148Rzn5T4ykd0vDQvMgeMRmBWttbDbSRSulCWqyrqulMWf6_U2CaQ6ppD1WRg7nFzQ_12RD33if1M6SmJNXvMGSXK2OmKgYRkGa.lofRce7v7LrQJqZjEy1wsPFa&c1K5tw0w6_=49Tir6qGM8dhvPt7R7LNHKMllt2oSy6EFj0wydkWJ.FWYR4dbjFwPEg_Ee2WyEFZhXz0HcDWPRPUVLmazjJr4vwu5cfXksOVzTMzztZUNpRbl7zPkZWn2JPlfwkejIlrDskntg87.o3wT20LhhAbiEetaAGGQ5885izXKd3.7bALLW_IHWUyL1AiZ2p5OxftbeN.JPI0tnTBujxFgP28gpujG6yfib7.jXr6xxRl_mSdpZBVMSLKVPAr1OUjsXozApp....CjrEyYVLcOFlCy3VK8EHCrxy2QT_A5DSKWDLU3HYzvuFxP54WapUIf5.jo6o7EL2mUg6XRQqjRicw_9zg2Z5bQnfTuYXmI356pv7OTHWeGhf76yr59euidb6ZtHaE1_SAQrvSwKUKlafg9.luowYpPaiTqLCD51pZA7UCJl4WJ2tSzQus4woUFczAdfYaAfSrJ3EQYqik127pJva"
