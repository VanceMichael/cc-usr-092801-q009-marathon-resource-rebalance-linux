# 协同事务平台

协同事务平台为需要多机构参与的业务提供统一的机构、授权、案件、证据、审批、资源预约、资金分录、消息归并、通知、定时任务和历史回放能力。项目只使用 Python 标准库与 SQLite，适合在单个 Linux 应用容器内运行。

## 马拉松赛事编排

在通用协同能力之上，平台内置了分枪分区赛事编排服务（`civicflow.marathon.MarathonService`，经 `CivicFlow.open(...).marathon` 访问）：

- **资格与分区快照**：报名时保存选手资格快照与原分区，四类保障资源（起跑区容量、医疗救援覆盖、补给水位、接驳班次）随报名逐人锁定。
- **规则版本**：`activate_rules` 发布资格类别到分区的映射，版本单调递增；`adjustable_range` 按生效版本计算选手可调整范围。
- **重排方案**：运营提出（`propose:marathon`），医疗与交通负责人分别确认（`confirm:marathon.medical` / `confirm:marathon.transport`），申请人不得自批，两角色不得同人；落库在同一事务内改写选手分区并重锁全部保障资源，失败整体回滚，不留半状态。
- **现场保护**：已检录或已发枪选手不能被普通重排改写；退赛只释放尚未消耗（未发枪）的资源，已消耗部分保留。
- **回传接入**：计时、检录、医疗回传按来源序号幂等接收；同序号异文进入隔离表并只冻结关联选手，解冻需说明原因。
- **并发防护**：每次现场变更推进环境版本号，方案落库前核对版本与规则版本，过期方案置为 `superseded` 并拒绝落库。
- **指挥与复盘**：`confirmation_queue` 给出发枪前仍可推进的已确认方案；`review_at` 按选定时点重建分区归属、保障占用与每次调整的原因；全部待办与时间依据持久化于 SQLite，进程退出不丢失。

```bash
PYTHONPATH=src python3 -m civicflow.cli --db /tmp/marathon.sqlite3 marathon-demo
PYTHONPATH=src python3 -m civicflow.cli --db /tmp/marathon.sqlite3 marathon-queue
```

## 目录

- `src/civicflow/`：领域服务、SQLite 持久化、权限和命令行入口。
- `tests/`：核心流程、边界条件和异常路径测试。
- `examples/`：本地演示输入。

## 配置

通过 `CIVICFLOW_DB` 指定 SQLite 文件路径；不设置时命令行使用当前目录下的 `civicflow.sqlite3`。所有时间使用带时区的 ISO 8601 字符串。

## 测试

```bash
PYTHONPATH=src python3 -m unittest discover -s tests -v
```

## 编译或构建

```bash
PYTHONPATH=src python3 -m compileall -q src
```

## 使用

初始化数据库并运行离线演示：

```bash
PYTHONPATH=src python3 -m civicflow.cli --db /tmp/civicflow-demo.sqlite3 demo
```

查看当前案件：

```bash
PYTHONPATH=src python3 -m civicflow.cli --db /tmp/civicflow-demo.sqlite3 list-cases
```
