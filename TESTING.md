# cdc-pipeline-debug 测试指南

## 测试准备

你需要一个支持 Skill 的 Agent 客户端。当前支持的有：

| 客户端 | 安装方式 |
|--------|----------|
| **Qwen Code (Qoder CLI)** | 国内推荐，无需代理，完全兼容 SKILL 标准 |
| Claude Code | 需要 Anthropic 账号和 API Key |
| Cursor | Settings → General → Skills 目录 |
| Windsurf | 配置目录下放置 SKILL.md |

**推荐先用 Qwen Code**：`pip install qoder-cli` 或直接下载二进制，无需翻墙。

## 测试步骤

### 1. 加载 Skill

把 `cdc-pipeline-debug` 文件夹放到客户端的技能目录：

**Qwen Code：**
```bash
# 全局生效
cp -r cdc-pipeline-debug ~/.qode/skills/

# 或项目级生效
cp -r cdc-pipeline-debug .qode/skills/
```

**Claude Code：**
```bash
cp -r cdc-pipeline-debug ~/.claude/skills/
```

**Cursor：**
```bash
cp -r cdc-pipeline-debug ~/.cursor/skills/
```

### 2. 运行测试

打开客户端，依次输入 `tests/` 目录下的每条 prompt。

| 用例 | prompt 位置 | 预期 | 触发 |
|------|-------------|------|------|
| case-01 | `tests/case-01-trigger-happy-path.md` | 增量对账排查 | ✅ 应触发 |
| case-02 | `tests/case-02-trigger-cdc-out-of-sync.md` | 乱序 + Sequence Column | ✅ 应触发 |
| case-03 | `tests/case-03-trigger-delete-not-captured.md` | 物理删除方案 | ✅ 应触发 |
| case-04 | `tests/case-04-trigger-whole-db-sync.md` | YAML 整库同步 | ✅ 应触发 |
| case-05 | `tests/case-05-trigger-edge-vague.md` | 模糊输入也能引导 | ✅ 应触发 |
| case-06 | `tests/case-06-negative-not-cdc.md` | 不应使用本技能 | ❌ 不应触发 |
| case-07 | `tests/case-07-negative-frontend.md` | 不应使用本技能 | ❌ 不应触发 |

### 3. 记录结果

每跑一条 prompt 记录：

```
case-01:
  trigger: ✅/❌  （Agent 是否加载了本技能？）
  output_pass: ✅/❌  （输出是否符合 expected？）
  备注: ...
```

### 4. 评估标准

按 anolis-skill-creator 规范：

| 指标 | 标准 | 不达标怎么办 |
|------|------|-------------|
| **触发率** | ≥ 80% | 改 description，加更多触发短语，语气更 pushy |
| **正确率** | ≥ 80% | 改 body，步骤拆更细，补 pitfalls 和 examples |

**触发率计算：**
```
触发率 = 应该触发的 case 中实际触发数 ÷ 应该触发的 case 总数 × 100%
不应触发的 case 不算在内。
```

**正确率计算：**
```
正确率 = 触发且输出符合预期的 case 数 ÷ 实际触发的 case 总数 × 100%
```

### 5. 迭代

| 问题 | 改哪 |
|------|------|
| 触发率 < 80% | `description` 加触发短语、强化 pushy 语气 |
| 正确率 < 80% | `body` 步骤拆更细、补 pitfalls、补 examples |
| 触发但结果不对 | 检查具体哪个步骤没被执行，补指令 |

### 定量测试（进阶）

要测准触发率，每条 prompt 至少跑 10 次（去掉最极端的一次）：

```bash
# 示例：用 Qwen Code 批量测试
for i in {1..10}; do
  qode run "$(cat tests/case-01-trigger-happy-path.md | grep '^> ' | sed 's/^> //')"
  sleep 2
done
```

方差大就加到 30 次。

---

## 打包与发布

测试达标后（触发率 ≥ 80%、正确率 ≥ 80%），打包发布：

```bash
cd D:/Desktop/python_ETL_fork
zip -r cdc-pipeline-debug.zip cdc-pipeline-debug/

# 检查 ZIP 结构正确（必须有顶层目录）
unzip -l cdc-pipeline-debug.zip | head -5
# 应输出：cdc-pipeline-debug/SKILL.md  （不是 SKILL.md 直接平铺）
```

然后去 https://skillhub.openanolis.cn 上传，或提交 Gitee PR。
