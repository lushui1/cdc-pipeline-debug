# cdc-pipeline-debug 测试指南

## 测试准备

你需要一个支持 Skill 的 Agent 客户端：

| 客户端 | 安装方式 |
|--------|----------|
| **Hermes** | 推荐，原生支持 |
| Qwen Code (Qoder CLI) | 国内可用 |
| Claude Code | 需 Anthropic API Key |
| Cursor | 设置 → Skills 目录 |

## 安装 Skill

```bash
hermes skills install \
  https://raw.githubusercontent.com/lushui1/cdc-pipeline-debug/main/SKILL.md \
  --name cdc-pipeline-debug
```

确认安装成功：

```bash
hermes skills list | grep cdc-pipeline-debug
```

## 测试用例

共 **14 个测试用例**，覆盖 8 大分类 + 负例：

| # | 文件 | 分类 | 测试内容 | 应触发 |
|---|------|------|----------|--------|
| 1 | `tests/001-data-count-mismatch.md` | 一、数据问题 | 凌晨边界数据缺失 | ✅ |
| 2 | `tests/002-data-value-rollback.md` | 一、数据问题 | CDC 日志正常但数据回退 | ✅ |
| 3 | `tests/003-task-startup-error.md` | 二、任务问题 | 任务启动就报错 | ✅ |
| 4 | `tests/004-performance-lag.md` | 三、性能问题 | 延迟越来越高 | ✅ |
| 5 | `tests/005-ddl-schema-change.md` | 四、结构问题 | 加字段未同步到目标库 | ✅ |
| 6 | `tests/006-upgrade-migration.md` | 五、运维问题 | MySQL 升级后 CDC 异常 | ✅ |
| 7 | `tests/007-first-time-setup.md` | 六、配置搭建 | 第一次搭建不会配 | ✅ |
| 8 | `tests/008-monitoring-alerting.md` | 七、预防优化 | 怎么监控和告警 | ✅ |
| 9 | `tests/009-canal-tool-issue.md` | 八、其他工具 | Canal 连不上 MySQL | ✅ |
| 10 | `tests/010-debezium-pg-issue.md` | 八、其他工具 | Debezium 连不上 PG | ✅ |
| 11 | `tests/011-recovery-failure.md` | 二、任务问题 | checkpoint 恢复 binlog 找不到 | ✅ |
| 12 | `tests/012-failover-issue.md` | 五、运维问题 | 主从切换后数据不对 | ✅ |
| 13 | `tests/013-negative-react.md` | 负例 | React Hook 问题 | ❌ |
| 14 | `tests/014-negative-css.md` | 负例 | CSS 兼容性问题 | ❌ |

## 测试方法

### 定性测试

在 Agent 客户端的对话中直接输入每条 prompt，记录：

```
case-001:
  触发: ✅/❌
  回答质量: 正确/部分正确/错误
  备注: _______
```

### 定量测试（可选）

每条正例跑 10 次，统计触发率：

| 指标 | 达标线 |
|------|--------|
| 触发率（正例） | ≥ 80% |
| 不触发率（负例） | 100% |
| 回答正确率 | ≥ 80% |

### 不达标怎么办

| 问题 | 改哪 |
|------|------|
| 触发率 < 80% | 改 `description`，加触发短语，强化 pushy 语气 |
| 回答正确率 < 80% | 改 body 步骤，补场景细节，补示例 |
| 负例误触发 | 检查 description 是否过于宽泛 |

## 打分标准

参考 anolis-skill-creator 发布前自检清单：

- [ ] name 三处一致（ZIP/目录/frontmatter）
- [ ] description 含"做什么 + 什么时候触发"
- [ ] description 至少 3 个触发短语
- [ ] body ≥ 5 行 + ≥ 1 个 `##` 章节
- [ ] ≥ 1 个完整示例
- [ ] 跑过 ≥ 5 条测试 prompt
- [ ] 触发率 ≥ 80%、正确率 ≥ 80%
- [ ] ZIP 内无 `.DS_Store` / `__MACOSX/`
- [ ] 至少一个客户端实测导入成功
