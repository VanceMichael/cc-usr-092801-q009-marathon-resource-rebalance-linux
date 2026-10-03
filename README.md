# 协同事务平台

协同事务平台为需要多机构参与的业务提供统一的机构、授权、案件、证据、审批、资源预约、资金分录、消息归并、通知、定时任务和历史回放能力。项目只使用 Python 标准库与 SQLite，适合在单个 Linux 应用容器内运行。

`civicflow.race` 在通用协同能力之上提供**赛事编排**：面向北京马拉松三枪分区起跑，保存选手资格与原分区快照，按规则版本计算可调整范围，在单个事务内同时锁定起跑区容量、医疗救援覆盖、补给水位与接驳班次，并由运营、医疗、交通三方会签后原子生效。

## 赛事编排能力

- **资格与原分区快照**：报名导入时保存资格信息与原分区，永不被改写；选手每次变化写入带时间、原因、操作人和方案号的历史版本。
- **规则版本**：`put_rule` 发布带生效时间的规则（枪次顺序、允许跨越的枪次/分区数），`adjustable_targets` 按时点取生效规则计算可调整范围；已检录、已发枪或被异文隔离的选手返回空范围。
- **四类资源同锁**：重排方案提出时在同一 `BEGIN IMMEDIATE` 事务内预占起跑区容量与每位选手在目标分区的医疗、补给、接驳资源；任一不足整体回滚，不留预占、不动选手。
- **冻结与退赛**：检录/发枪回传冻结选手，普通重排不能改写；退赛只把 `held/confirmed` 的保障置为 `released`，已有现场回执（`consumed`，如已领取补给）不退仓。
- **幂等回传与异文隔离**：计时、检录、医疗回传按 `(来源, 选手, 序号)` 幂等接收；同序号不同内容只登记冲突并冻结该选手，自动作废仅涉及该选手的待决方案，其他选手不受影响。
- **会签与生效**：方案由运营人员提出，医疗与交通负责人分别确认，申请人不得自批；任一方驳回即释放全部预占。生效前再次核对选手基准版本和最新容量，批量调整与现场回执/检录并发时，旧方案被整体作废，不产生“选手已换区、保障仍指旧区”的半状态。
- **确认队列与复盘**：`confirmation_queue` 返回发枪前可继续推进的待确认/待生效方案；`review_at(as_of=...)` 按任意定时点核对分区、保障分配与每次调整的原因。所有待办与时间依据均在 SQLite 落盘，进程退出不丢失。

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

运行三枪分区编排演示（构造规则、分区、保障资源、选手，完成一次医疗/交通会签的重排）：

```bash
PYTHONPATH=src python3 -m civicflow.cli --db /tmp/race-demo.sqlite3 race-demo
```

查看发枪前可继续推进的确认队列、按定时点复盘：

```bash
PYTHONPATH=src python3 -m civicflow.cli --db /tmp/race-demo.sqlite3 race-queue
PYTHONPATH=src python3 -m civicflow.cli --db /tmp/race-demo.sqlite3 race-review --at 2026-09-28T12:30:00+08:00
```
